/* Copyright (c) 2012, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory.
 * Written by Adam Moody <moody20@llnl.gov>.
 * LLNL-CODE-568372.
 * All rights reserved.
 * This file is part of the LWGRP library.
 * For details, see https://github.com/hpc/lwgrp
 * Please also read this file: LICENSE.TXT. */

#include <stdlib.h>
#include <string.h>

#include "mpi.h"
#include "lwgrp.h"
#include "lwgrp_internal.h"

enum bin_values {
  INDEX_COUNT   = 0,
  INDEX_CLOSEST = 1,
};

/* given a specified number of bins, an index into those bins, and a
 * input group, create and return a new group consisting of all ranks
 * belonging to the same bin, runs in O(num_bins * log N) time */
int lwgrp_chain_split_bin_scan(
  int num_bins,
  int my_bin,
  const lwgrp_chain* in,
  lwgrp_chain* out)
{
  /* With this function, we split the "in" group into up to "num_bins"
   * subgroups.  A process is grouped with all other processes that
   * specify the same value for "my_bin".  The descriptor for the new
   * group is returned in "out".  If my_bin is less than 0, an empty
   * (NULL) group is returned.
   *
   * Implementation:
   * We run two exclusive scans, one scanning from left to right, and
   * another scanning from right to left.  As the output of the
   * left-going scan, a process acquires the number of ranks to its
   * left that are in its bin as well as the rank of the process that
   * is immediately to its left that is also in its bin.  Similarly,
   * the right-going scan provides the process with the number of ranks
   * to the right and the rank of the process immediately to its right
   * that is in the same bin.  With this info, a process can determine
   * its rank and the number of ranks in its group, as well as, the
   * ranks of its left and right partners, which is sufficient to fully
   * define the "out" group. */
  int i;

  if (my_bin >= num_bins) {
    /* TODO: fail */
  }

  /* define some frequently used indicies into our arrays */
  int my_bin_index = 2 * my_bin;
  int rank_index   = 2 * num_bins;

  /* allocate space for our send and receive buffers */
  int elements = 2 * num_bins + 1;
  int* bins = (int*) lwgrp_malloc(
    4 * elements * sizeof(int), sizeof(int), __FILE__, __LINE__
  );
  if (bins == NULL) {
    /* TODO: fail */
  }

  /* set up pointers to our send and receive buffers */
  int* send_left_bins  = bins + (0 * elements);
  int* recv_left_bins  = bins + (1 * elements);
  int* send_right_bins = bins + (2 * elements);
  int* recv_right_bins = bins + (3 * elements);

  /* initialize our send buffers,
   * set all ranks to MPI_PROC_NULL and set all counts to 0 */
  for(i = 0; i < 2*num_bins; i += 2) {
    send_left_bins[i+INDEX_COUNT]    = 0;
    send_right_bins[i+INDEX_COUNT]   = 0;
    send_left_bins[i+INDEX_CLOSEST]  = MPI_PROC_NULL;
    send_right_bins[i+INDEX_CLOSEST] = MPI_PROC_NULL;
  }

  /* for the bin we are in, set the rank to our rank and set the
   * count to 1 */
  if (my_bin >= 0) {
    send_left_bins[my_bin_index+INDEX_COUNT]    = 1;
    send_right_bins[my_bin_index+INDEX_COUNT]   = 1;
    send_left_bins[my_bin_index+INDEX_CLOSEST]  = in->comm_rank;
    send_right_bins[my_bin_index+INDEX_CLOSEST] = in->comm_rank;
  }

  /* execute double, inclusive scan, one going left-to-right,
   * and another right-to-left */
  MPI_Request request[4];
  MPI_Status  status[4];
  MPI_Comm comm  = in->comm;
  int left_rank  = in->comm_left;
  int right_rank = in->comm_right;
  int my_left  = MPI_PROC_NULL;
  int my_right = MPI_PROC_NULL;
  while (left_rank != MPI_PROC_NULL || right_rank != MPI_PROC_NULL) {
    int k = 0;

    /* if we have a left partner, send it our left-going data and
     * recv its right-going data */
    if (left_rank != MPI_PROC_NULL) {
      /* receive right-going data from the left */
      MPI_Irecv(
        recv_left_bins, elements, MPI_INT, left_rank, LWGRP_MSG_TAG_0,
        comm, &request[k]
      );
      k++;

      /* inform rank to our left of the rank on our right, and send
       * it our data */
      send_left_bins[rank_index] = right_rank;
      MPI_Isend(
        send_left_bins, elements, MPI_INT, left_rank, LWGRP_MSG_TAG_0,
        comm, &request[k]
      );
      k++;
    }

    /* if we have a right partner, send it our right-going data and
     * recv its left-going data */
    if (right_rank != MPI_PROC_NULL) {
      /* receive left-going data from the right */
      MPI_Irecv(
        recv_right_bins, elements, MPI_INT, right_rank, LWGRP_MSG_TAG_0,
        comm, &request[k]
      );
      k++;

      /* inform rank to our right of the rank on our left, and send
       * it our data */
      send_right_bins[rank_index] = left_rank;
      MPI_Isend(
        send_right_bins, elements, MPI_INT, right_rank, LWGRP_MSG_TAG_0,
        comm, &request[k]
      );
      k++;
    }

    /* wait for all communication to complete */
    if (k > 0) {
      MPI_Waitall(k, request, status);
    }

    /* if we have a left partner, merge its data with our
     * right-going data */
    if (left_rank != MPI_PROC_NULL) {
      /* first, make note of the rightmost rank in our bin
       * to the left if we haven't already found one */
      if (my_left == MPI_PROC_NULL && my_bin >= 0) {
        my_left = recv_left_bins[my_bin_index+INDEX_CLOSEST];
      }

      /* now merge data from left into our right-going data */
      for(i = 0; i < 2*num_bins; i += 2) {
        /* add the counts for this bin */
        send_right_bins[i+INDEX_COUNT] += recv_left_bins[i+INDEX_COUNT];

        /* if we haven't already defined the rightmost rank for this
         * bin, set it to the value defined in the message from the
         * left */
        if (send_right_bins[i+INDEX_CLOSEST] == MPI_PROC_NULL) {
          send_right_bins[i+INDEX_CLOSEST] = recv_left_bins[i+INDEX_CLOSEST];
        }
      }

      /* get the next rank to send to on our left */
      left_rank = recv_left_bins[rank_index];
    }

    /* if we have a right partner, merge its data with our
     * left-going data */
    if (right_rank != MPI_PROC_NULL) {
      /* first, make note of the leftmost rank in our bin to the
       * right if we haven't already found one */
      if (my_right == MPI_PROC_NULL && my_bin >= 0) {
        my_right = recv_right_bins[my_bin_index+INDEX_CLOSEST];
      }

      /* now merge data from right into our left-going data */
      for(i = 0; i < 2*num_bins; i += 2) {
        /* add the counts for this bin */
        send_left_bins[i+INDEX_COUNT] += recv_right_bins[i+INDEX_COUNT];

        /* if we haven't already defined the leftmost rank for this bin,
         * set it to the value defined in the message from the left */
        if (send_left_bins[i+INDEX_CLOSEST] == MPI_PROC_NULL) {
          send_left_bins[i+INDEX_CLOSEST] = recv_right_bins[i+INDEX_CLOSEST];
        }
      }

      /* get the next rank to send to on our right */
      right_rank = recv_right_bins[rank_index];
    }
  }

  if (my_bin >= 0) {
    /* get count of number of ranks in our bin to our left and
     * right sides */
    int count_left  = send_right_bins[my_bin_index + INDEX_COUNT] - 1;
    int count_right = send_left_bins[my_bin_index + INDEX_COUNT]  - 1;

    /* the number of ranks to our left defines our rank, while we add
     * the number of ranks to our left with the number of ranks to our
     * right plus ourselves to get the total number of ranks in our
     * bin */
    out->comm       = in->comm;
    out->comm_rank  = in->comm_rank;
    out->comm_left  = my_left;
    out->comm_right = my_right;
    out->group_rank = count_left;
    out->group_size = count_left + count_right + 1;
  } else {
    /* create an empty group */
    lwgrp_chain_set_null(out);
  }

  lwgrp_free(&bins);

  return LWGRP_SUCCESS; 
}

/* execute a barrier operation on the chain */
int lwgrp_chain_barrier_dissemination(const lwgrp_chain* group)
{
  /* execute double, inclusive scan, one going left-to-right,
   * and another right-to-left */
  MPI_Request request[4];
  MPI_Status  status[4];
  MPI_Comm comm  = group->comm;
  int left_rank  = group->comm_left;
  int right_rank = group->comm_right;
  int recv_left  = MPI_PROC_NULL;
  int recv_right = MPI_PROC_NULL;
  while (left_rank != MPI_PROC_NULL || right_rank != MPI_PROC_NULL) {
    int k = 0;

    /* if we have a left partner, send it our left-going data
     * and recv its right-going data */
    if (left_rank != MPI_PROC_NULL) {
      /* receive right-going data from the left */
      MPI_Irecv(
        &recv_left, 1, MPI_INT, left_rank, LWGRP_MSG_TAG_0,
        comm, &request[k]
      );
      k++;

      /* inform rank to our left of the rank on our right,
       * and send it our data */
      MPI_Isend(
        &right_rank, 1, MPI_INT, left_rank, LWGRP_MSG_TAG_0,
        comm, &request[k]
      );
      k++;
    }

    /* if we have a right partner, send it our right-going data
     * and recv its left-going data */
    if (right_rank != MPI_PROC_NULL) {
      /* receive left-going data from the right */
      MPI_Irecv(
        &recv_right, 1, MPI_INT, right_rank, LWGRP_MSG_TAG_0,
        comm, &request[k]
      );
      k++;

      /* inform rank to our right of the rank on our left,
       * and send it our data */
      MPI_Isend(
        &left_rank, 1, MPI_INT, right_rank, LWGRP_MSG_TAG_0,
        comm, &request[k]
      );
      k++;
    }

    /* wait for all communication to complete */
    if (k > 0) {
      MPI_Waitall(k, request, status);
    }

    /* get the next left and right ranks */
    left_rank  = recv_left;
    right_rank = recv_right;
  }

  return LWGRP_SUCCESS; 
}

/* issues an allgather operation over the processes in the
 * specified group */
int lwgrp_chain_allgather_brucks_int(
  int sendint,
  int recvbuf[],
  const lwgrp_chain* group)
{
  MPI_Comm comm = group->comm;
  int rank      = group->group_rank;
  int ranks     = group->group_size;

  /* determine size and allocate scratch space */
  int max_ints = 1 + ranks;
  int buf_size = max_ints * sizeof(int);
  int scratch_size = 4 * buf_size;
  char* scratch = NULL;
  if (scratch_size > 0) {
    scratch = (char*) lwgrp_malloc(
      scratch_size, sizeof(int), __FILE__, __LINE__
    );
  }

  /* set up pointers to internal data structures */
  int* send_left_buf  = (int*) (scratch + (0 * buf_size));
  int* send_right_buf = (int*) (scratch + (1 * buf_size));
  int* recv_left_buf  = (int*) (scratch + (2 * buf_size));
  int* recv_right_buf = (int*) (scratch + (3 * buf_size));

  /* copy our own data into the receive buffer */
  recvbuf[rank] = sendint;

  /* execute the allgather operation */
  MPI_Request request[4];
  MPI_Status status[4];
  int left_rank  = group->comm_left;
  int right_rank = group->comm_right;
  int count = 1;
  while (left_rank != MPI_PROC_NULL || right_rank != MPI_PROC_NULL) {
    int k = 0;

    /* if we have a left partner, send it all data we know about
     * from on rank on to the right */
    if (left_rank != MPI_PROC_NULL) {
      /* issue receive for data from left partner */
      MPI_Irecv(
        recv_left_buf, max_ints, MPI_INT, left_rank,
        LWGRP_MSG_TAG_0, comm, &request[k]
      );
      k++;

      /* compute the number of elements we'll be sending left */
      int left_count = count;
      if (rank + left_count > ranks) {
        left_count = ranks - rank;
      }

      /* prepare the buffer */
      memcpy(send_left_buf, &right_rank, sizeof(int));
      memcpy(send_left_buf+1, recvbuf + rank, left_count * sizeof(int));

      /* send the data */
      MPI_Isend(
        (void*)send_left_buf, (1 + left_count), MPI_INT, left_rank,
        LWGRP_MSG_TAG_0, comm, &request[k]
      );
      k++;
    }

    /* if we have a right partner, send it all data we know about
     * from on rank on to the left */
    if (right_rank != MPI_PROC_NULL) {
      /* issue receive for data from right partner */
      MPI_Irecv(
        recv_right_buf, max_ints, MPI_INT, right_rank,
        LWGRP_MSG_TAG_0, comm, &request[k]
      );
      k++;

      /* compute the number of elements we'll be sending right */
      int right_start = rank + 1 - count;
      int right_count = count;
      if (right_start < 0) {
        right_start = 0;
        right_count = rank + 1;
      }

      /* prepare the buffer */
      memcpy(send_right_buf, &left_rank, sizeof(int));
      memcpy(send_right_buf+1, recvbuf + right_start, right_count * sizeof(int));

      /* send the data */
      MPI_Isend(
        send_right_buf, (1 + right_count), MPI_INT, right_rank,
        LWGRP_MSG_TAG_0, comm, &request[k]
      );
      k++;
    }

    /* wait for communication to complete */
    if (k > 0) {
      MPI_Waitall(k, request, status);
    }

    /* copy data from left rank into receive buffer */
    if (left_rank != MPI_PROC_NULL) {
      int left_start = rank + 1 - 2 * count;
      int left_count = count;
      if (left_start < 0) {
        left_start = 0;
        left_count = rank + 1 - count;
      }
      memcpy(recvbuf + left_start, recv_left_buf + 1, left_count * sizeof(int));

      /* get next rank to our left */
      left_rank = recv_left_buf[0];
    }

    /* copy data from right rank into receive buffer */
    if (right_rank != MPI_PROC_NULL) {
      int right_start = rank + count;
      int right_count = count;
      if (right_start + count > ranks) {
        right_count = ranks - right_start;
      }
      memcpy(recvbuf + right_start, recv_right_buf + 1, right_count * sizeof(int));

      /* get next rank to our right */
      right_rank = recv_right_buf[0];
    }

    /* go on to next iteration */
    count <<= 1;
  }

  /* free off scratch space memory */
  lwgrp_free(&scratch);

  return LWGRP_SUCCESS;
}

/* assumes the chain has an exact power of two number of members,
 * input should be in resultbuf and output will be stored there,
 * scratchbuf should be scratch space */
int lwgrp_chain_allreduce_recursive_pow2(
  void* resultbuf,
  void* scratchbuf,
  int count,
  MPI_Datatype type,
  MPI_Op op,
  const lwgrp_chain* group)
{
  /* we use a recurisve doubling algorithm */
  MPI_Request request[4];
  MPI_Status  status[4];

  /* get our rank within comm */
  MPI_Comm comm = group->comm;
  int rank      = group->group_rank;
  int ranks     = group->group_size;

  /* execute recursive doubling operation */
  int mask = 1;
  int left_rank  = group->comm_left;
  int right_rank = group->comm_right;
  int recv_left  = MPI_PROC_NULL;
  int recv_right = MPI_PROC_NULL;
  while (mask < ranks) {
    /* get rank of partner in comm */
    int partner;
    int exchange_rank = rank ^ mask;
    if (exchange_rank < rank) {
      partner = left_rank;
    } else {
      partner = right_rank;
    }

    /* exchange data with partner */
    MPI_Sendrecv(
      resultbuf,  count, type, partner, LWGRP_MSG_TAG_0,
      scratchbuf, count, type, partner, LWGRP_MSG_TAG_0,
      comm, status
    );

    /* reduce data (being careful about non-commutative ops) */
    if (exchange_rank < rank) {
      /* higher order data is in resultbuf,
       * so resultbuf = scratchbuf + resultbuf */
      MPI_Reduce_local(scratchbuf, resultbuf, count, type, op);
    } else {
      /* higher order data is in scratchbuf,
       * so scratchbuf = resultbuf + scratchbuf,
       * then copy result back to resultbuf for sending in next round */
      MPI_Reduce_local(resultbuf, scratchbuf, count, type, op);
      lwgrp_type_dtbuf_memcpy(resultbuf, scratchbuf, count, type);
    }

    /* prepare for next iteration */
    mask <<= 1;

    /* TODO: we could merge the reduce step above with one of
     * these messages below */

    /* if mask is large enough, we can skip the last exchange */
    if (mask < ranks) {
      int k = 0;

      /* if we have a left partner, send it our left-going data and
       * recv its right-going data */
      if (left_rank != MPI_PROC_NULL) {
        /* receive right-going data from the left */
        MPI_Irecv(
          &recv_left, 1, MPI_INT, left_rank,
          LWGRP_MSG_TAG_0, comm, &request[k]
        );
        k++;

        /* inform rank to our left of the rank on our right, and send
         * it our data */
        MPI_Isend(
          &right_rank, 1, MPI_INT, left_rank,
          LWGRP_MSG_TAG_0, comm, &request[k]
        );
        k++;
      }

      /* if we have a right partner, send it our right-going data and
       * recv its left-going data */
      if (right_rank != MPI_PROC_NULL) {
        /* receive left-going data from the right */
        MPI_Irecv(
          &recv_right, 1, MPI_INT, right_rank,
          LWGRP_MSG_TAG_0, comm, &request[k]
        );
        k++;

        /* inform rank to our right of the rank on our left, and send
         * it our data */
        MPI_Isend(
          &left_rank, 1, MPI_INT, right_rank,
          LWGRP_MSG_TAG_0, comm, &request[k]
        );
        k++;
      }

      /* wait for all communication to complete */
      if (k > 0) {
        MPI_Waitall(k, request, status);
      }

      /* get next set of ranks on left and right sides */
      left_rank = recv_left;
      right_rank = recv_right;
    }
  }

  return LWGRP_SUCCESS;
}

int lwgrp_chain_allreduce_recursive(
  const void* sendbuf,
  void* recvbuf,
  int count,
  MPI_Datatype type,
  MPI_Op op,
  const lwgrp_chain* group)
{
  /* we implement a recursive doubling algorithm, but we're careful
   * to do this to support non-commutative ops, basically we find the
   * largest power of two that is <= #ranks, then we assign the initial
   * (#ranks-largest_power_of_two) odd ranks to be the extras and build
   * a new power-of-two chain after reducing the contribution from the
   * extra ranks, e.g.,
   *
   * For a five-task group (one bigger than 2^2=4):
   * 1) initial chain: 0 <--> 1 <--> 2 <--> 3 <--> 4
   * 2) rank 1 sends data to rank 0 and reduce
   * 3) remove rank 1 to form new chain 0 <--> 2 <--> 3 <--> 4
   * 4) recursive double reduction on new chain ((0,2),(3,4))
   * 5) rank 0 sends final result to rank 1 */
  MPI_Request request[4];
  MPI_Status  status[4];

  /* get chain info */
  MPI_Comm comm  = group->comm;
  int comm_rank  = group->comm_rank;
  int left_rank  = group->comm_left;
  int right_rank = group->comm_right;
  int rank       = group->group_rank;
  int ranks      = group->group_size;

  /* copy our data into the receive buffer */
  if (sendbuf != MPI_IN_PLACE) {
    lwgrp_type_dtbuf_memcpy(recvbuf, sendbuf, count, type);
  }

  /* adjust for non-zero lower bounds */
  void* tempbuf = lwgrp_type_dtbuf_alloc(
    count, type, __FILE__, __LINE__
  );

  /* find largest power of two that fits within group_ranks */
  int pow2, log2;
  lwgrp_largest_pow2_log2_lte(ranks, &pow2, &log2);

  /* compue number of extra ranks, and the last rank that borders
   * one of the odd ranks out */
  int extra = ranks - pow2;
  int cutoff = extra * 2;

  /* assume that we are not one of the odd ranks out */
  int odd_rank_out = 0;

  /* reduce data from odd ranks out and remove them from the chain */
  lwgrp_chain new_group;
  const lwgrp_chain* pow2_group = group;
  if (pow2 < ranks) {
    /* assume we'll keep the same neighbors */
    int new_rank  = rank - extra;
    int new_left  = left_rank;
    int new_right = right_rank;

    /* if we are within the cutoff, we need to adjust our rank and
     * neighbors */
    if (rank <= cutoff) {
      /* if we are an odd rank under the cutoff,
       * we are an odd rank out */
      if (rank & 0x1) {
        odd_rank_out = 1;
      }

      /* TODO: we could combine this with the neighbor data below */

      /* odd ranks out send their data to their left neighbor */
      if (rank < cutoff) {
        if (rank & 0x1) {
          /* send reduce result to left */
          MPI_Send(
            recvbuf, count, type, left_rank, LWGRP_MSG_TAG_0, comm
          );
        } else {
          /* recv data from odd rank out on right */
          MPI_Recv(
            tempbuf, count, type, right_rank,
            LWGRP_MSG_TAG_0, comm, status
          );

          /* we do things in a particular way here to ensure correct
           * results for non-commutative ops, since out = in + out and
           * the higher order data is in tempbuf */
          MPI_Reduce_local(recvbuf, tempbuf, count, type, op);
          lwgrp_type_dtbuf_memcpy(recvbuf, tempbuf, count, type);
        }
      }

      /* set our new rank, we throw out out all odd ranks in this range
       * so just divide our rank by two */
      new_rank = (rank >> 1);

      /* TODO: we could eliminate half of these messages */

      /* now exchange neighbors to find new neighbors */
      /* everyone who has a left neighbor will get a new one */
      int k = 0;
      if (left_rank != MPI_PROC_NULL) {
        MPI_Irecv(
          &new_left, 1, MPI_INT, left_rank,
          LWGRP_MSG_TAG_0, comm, &request[k]
        );
        k++;

        MPI_Isend(
          &right_rank, 1, MPI_INT, left_rank,
          LWGRP_MSG_TAG_0, comm, &request[k]
        );
        k++;
      }
      /* everyone but the cutoff rank gets a new right neighbor */
      if (right_rank != MPI_PROC_NULL && rank < cutoff) {
        MPI_Irecv(
          &new_right, 1, MPI_INT, right_rank,
          LWGRP_MSG_TAG_0, comm, &request[k]
        );
        k++;

        MPI_Isend(
          &left_rank, 1, MPI_INT, right_rank,
          LWGRP_MSG_TAG_0, comm, &request[k]
        );
        k++;
      }
      if (k > 0) {
        MPI_Waitall(k, request, status);
      }
    }

    /* now we have enough to build our new chain that excludes the odd
     * ranks out */
    new_group.comm       = comm;
    new_group.comm_rank  = comm_rank;
    new_group.comm_left  = new_left;
    new_group.comm_right = new_right;
    new_group.group_rank = new_rank;
    new_group.group_size = pow2;
    pow2_group = &new_group;
  }

  /* power of two reduce */
  if (! odd_rank_out) {
    lwgrp_chain_allreduce_recursive_pow2(
      recvbuf, tempbuf, count, type, op, pow2_group
    );
  }

  /* send message back to odd ranks out */
  if (rank < cutoff) {
      if (rank & 0x1) {
        /* recv result from left rank */
        MPI_Recv(
          recvbuf, count, type, left_rank,
          LWGRP_MSG_TAG_0, comm, status
        );
      } else {
        /* send result to right rank */
        MPI_Send(
          recvbuf, count, type, right_rank, LWGRP_MSG_TAG_0, comm
        );
      }
  }

  /* free our scratch space */
  lwgrp_type_dtbuf_free(&tempbuf, type, __FILE__, __LINE__);

  return LWGRP_SUCCESS;
}

/* assumes the chain has an exact power of two number of members,
 * input should be in inbuf and output will be stored in outbuf,
 * outbuf is not modified on rank 0 */
int lwgrp_chain_exscan_recursive_pow2(
  void* inbuf,
  void* outbuf,
  int count,
  MPI_Datatype type,
  MPI_Op op,
  const lwgrp_chain* group)
{
  /* we use a recurisve doubling algorithm */
  MPI_Request request[4];
  MPI_Status  status[4];

  /* get our rank within comm */
  MPI_Comm comm = group->comm;
  int rank      = group->group_rank;
  int ranks     = group->group_size;

  /* allocate buffer to hold scan data */
  void* sendbuf = lwgrp_type_dtbuf_alloc(count, type, __FILE__, __LINE__);
  void* recvbuf = lwgrp_type_dtbuf_alloc(count, type, __FILE__, __LINE__);

  /* copy input data into temporary send buffer */
  lwgrp_type_dtbuf_memcpy(sendbuf, inbuf, count, type);

  /* execute recursive doubling operation */
  int initialized = 0;
  int mask = 1;
  int left_rank  = group->comm_left;
  int right_rank = group->comm_right;
  int recv_left  = MPI_PROC_NULL;
  int recv_right = MPI_PROC_NULL;
  while (mask < ranks) {
    /* get rank of partner in comm */
    int partner;
    int exchange_rank = rank ^ mask;
    if (exchange_rank < rank) {
      partner = left_rank;
    } else {
      partner = right_rank;
    }

    /* exchange data with partner */
    MPI_Sendrecv(
      sendbuf, count, type, partner, LWGRP_MSG_TAG_0,
      recvbuf, count, type, partner, LWGRP_MSG_TAG_0,
      comm, status
    );

    /* reduce data (being careful about non-commutative ops) */
    if (exchange_rank < rank) {
      /* higher order data is in sendbuf,
       * so sendbuf = recvbuf + sendbuf */
      MPI_Reduce_local(recvbuf, sendbuf, count, type, op);

      /* we only accumulate values in user buffer for ranks
       * before the current rank, higher order data in outbuf,
       * in resultbuf,
       * so outbuf = recvbuf + outbuf */
      if (initialized) {
          MPI_Reduce_local(recvbuf, outbuf, count, type, op);
      } else {
          lwgrp_type_dtbuf_memcpy(outbuf, recvbuf, count, type);
          initialized = 1;
      }
    } else {
      /* higher order data is in recvbuf,
       * so recvbuf = sendbuf + recvbuf,
       * then copy result back to sendbuf for sending in next round */
      MPI_Reduce_local(sendbuf, recvbuf, count, type, op);
      lwgrp_type_dtbuf_memcpy(sendbuf, recvbuf, count, type);
    }

    /* prepare for next iteration */
    mask <<= 1;

    /* TODO: we could merge the reduce step above with one of
     * these messages below */

    /* if mask is large enough, we can skip the last exchange */
    if (mask < ranks) {
      int k = 0;

      /* if we have a left partner, send it our left-going data and
       * recv its right-going data */
      if (left_rank != MPI_PROC_NULL) {
        /* receive right-going data from the left */
        MPI_Irecv(
          &recv_left, 1, MPI_INT, left_rank,
          LWGRP_MSG_TAG_0, comm, &request[k]
        );
        k++;

        /* inform rank to our left of the rank on our right, and send
         * it our data */
        MPI_Isend(
          &right_rank, 1, MPI_INT, left_rank,
          LWGRP_MSG_TAG_0, comm, &request[k]
        );
        k++;
      }

      /* if we have a right partner, send it our right-going data and
       * recv its left-going data */
      if (right_rank != MPI_PROC_NULL) {
        /* receive left-going data from the right */
        MPI_Irecv(
          &recv_right, 1, MPI_INT, right_rank,
          LWGRP_MSG_TAG_0, comm, &request[k]
        );
        k++;

        /* inform rank to our right of the rank on our left, and send
         * it our data */
        MPI_Isend(
          &left_rank, 1, MPI_INT, right_rank,
          LWGRP_MSG_TAG_0, comm, &request[k]
        );
        k++;
      }

      /* wait for all communication to complete */
      if (k > 0) {
        MPI_Waitall(k, request, status);
      }

      /* get next set of ranks on left and right sides */
      left_rank = recv_left;
      right_rank = recv_right;
    }
  }

  /* free memory */
  lwgrp_type_dtbuf_free(&recvbuf, type, __FILE__, __LINE__);
  lwgrp_type_dtbuf_free(&sendbuf, type, __FILE__, __LINE__);

  return LWGRP_SUCCESS;
}

int lwgrp_chain_exscan_recursive(
  const void* sendbuf,
  void* recvbuf,
  int count,
  MPI_Datatype type,
  MPI_Op op,
  const lwgrp_chain* group)
{
  /* we implement a recursive doubling algorithm, but we're careful
   * to do this to support non-commutative ops, basically we find the
   * largest power of two that is <= #ranks, then we assign the initial
   * (#ranks-largest_power_of_two) odd ranks to be the extras and build
   * a new power-of-two chain after reducing the contribution from the
   * extra ranks, e.g.,
   *
   * For a five-task group (one bigger than 2^2=4):
   * 1) initial chain: 0 <--> 1 <--> 2 <--> 3 <--> 4
   * 2) rank 1 sends data to rank 0 and reduce
   * 3) remove rank 1 to form new chain 0 <--> 2 <--> 3 <--> 4
   * 4) recursive double reduction on new chain ((0,2),(3,4))
   * 5) rank 0 sends final result to rank 1 */
  MPI_Request request[4];
  MPI_Status  status[4];

  /* get chain info */
  MPI_Comm comm  = group->comm;
  int comm_rank  = group->comm_rank;
  int left_rank  = group->comm_left;
  int right_rank = group->comm_right;
  int rank       = group->group_rank;
  int ranks      = group->group_size;

  /* adjust for non-zero lower bounds */
  void* inbuf  = lwgrp_type_dtbuf_alloc(count, type, __FILE__, __LINE__);
  void* outbuf = lwgrp_type_dtbuf_alloc(count, type, __FILE__, __LINE__);

  /* identify location of caller's input data */
  const void* userbuf = sendbuf;
  if (sendbuf == MPI_IN_PLACE) {
    /* input data is in receive buffer */
    userbuf = recvbuf;
  }

  /* copy our data into the temporary buffer */
  lwgrp_type_dtbuf_memcpy(inbuf, userbuf, count, type);

  /* find largest power of two that fits within group_ranks */
  int pow2, log2;
  lwgrp_largest_pow2_log2_lte(ranks, &pow2, &log2);

  /* compue number of extra ranks, and the last rank that borders
   * one of the odd ranks out */
  int extra = ranks - pow2;
  int cutoff = extra * 2;

  /* assume that we are not one of the odd ranks out */
  int odd_rank_out = 0;

  /* reduce data from odd ranks out and remove them from the chain */
  lwgrp_chain new_group;
  const lwgrp_chain* pow2_group = group;
  if (pow2 < ranks) {
    /* assume we'll keep the same neighbors */
    int new_rank  = rank - extra;
    int new_left  = left_rank;
    int new_right = right_rank;

    /* if we are within the cutoff, we need to adjust our rank and
     * neighbors */
    if (rank <= cutoff) {
      /* if we are an odd rank under the cutoff,
       * we are an odd rank out */
      if (rank & 0x1) {
        odd_rank_out = 1;
      }

      /* TODO: we could combine this with the neighbor data below */

      /* odd ranks out send their data to their left neighbor */
      if (rank < cutoff) {
        if (rank & 0x1) {
          /* send reduce result to left */
          MPI_Send(
            inbuf, count, type, left_rank, LWGRP_MSG_TAG_0, comm
          );
        } else {
          /* recv data from odd rank out on right */
          MPI_Recv(
            outbuf, count, type, right_rank,
            LWGRP_MSG_TAG_0, comm, status
          );

          /* we do things in a particular way here to ensure correct
           * results for non-commutative ops, since out = in + out and
           * the higher order data is in outbuf */
          MPI_Reduce_local(inbuf, outbuf, count, type, op);
          lwgrp_type_dtbuf_memcpy(inbuf, outbuf, count, type);
        }
      }

      /* set our new rank, we throw out out all odd ranks in this range
       * so just divide our rank by two */
      new_rank = (rank >> 1);

      /* TODO: we could eliminate half of these messages */

      /* now exchange neighbors to find new neighbors */
      /* everyone who has a left neighbor will get a new one */
      int k = 0;
      if (left_rank != MPI_PROC_NULL) {
        MPI_Irecv(
          &new_left, 1, MPI_INT, left_rank,
          LWGRP_MSG_TAG_0, comm, &request[k]
        );
        k++;

        MPI_Isend(
          &right_rank, 1, MPI_INT, left_rank,
          LWGRP_MSG_TAG_0, comm, &request[k]
        );
        k++;
      }
      /* everyone but the cutoff rank gets a new right neighbor */
      if (right_rank != MPI_PROC_NULL && rank < cutoff) {
        MPI_Irecv(
          &new_right, 1, MPI_INT, right_rank,
          LWGRP_MSG_TAG_0, comm, &request[k]
        );
        k++;

        MPI_Isend(
          &left_rank, 1, MPI_INT, right_rank,
          LWGRP_MSG_TAG_0, comm, &request[k]
        );
        k++;
      }
      if (k > 0) {
        MPI_Waitall(k, request, status);
      }
    }

    /* now we have enough to build our new chain that excludes the odd
     * ranks out */
    new_group.comm       = comm;
    new_group.comm_rank  = comm_rank;
    new_group.comm_left  = new_left;
    new_group.comm_right = new_right;
    new_group.group_rank = new_rank;
    new_group.group_size = pow2;
    pow2_group = &new_group;
  }

  /* scan with power of two, store result in user's receive buffer */
  if (! odd_rank_out) {
    /* execute the scan */
    lwgrp_chain_exscan_recursive_pow2(
      inbuf, recvbuf, count, type, op, pow2_group
    );
  }

  /* send message back to odd ranks out */
  if (rank < cutoff) {
      if (rank & 0x1) {
        /* recv result from left rank */
        MPI_Recv(
          recvbuf, count, type, left_rank,
          LWGRP_MSG_TAG_0, comm, status
        );
      } else {
        /* send result to odd rank */
        if (rank == 0) {
          /* there is no value before us, just send user data */
          MPI_Send(
            (void*)userbuf, count, type, right_rank,
            LWGRP_MSG_TAG_0, comm
          );
        } else {
          /* got a value before us, tack on user's data then send,
           * copy contents of userbuf to inbuf,
           * inbuf = recvbuf + inbuf,
           * send inbuf to odd rank */
          lwgrp_type_dtbuf_memcpy(inbuf, userbuf, count, type);
          MPI_Reduce_local(recvbuf, inbuf, count, type, op);
          MPI_Send(
            inbuf, count, type, right_rank,
            LWGRP_MSG_TAG_0, comm
          );
        }
      }
  }

  /* free our scratch space */
  lwgrp_type_dtbuf_free(&outbuf, type, __FILE__, __LINE__);
  lwgrp_type_dtbuf_free(&inbuf,  type, __FILE__, __LINE__);

  return LWGRP_SUCCESS;
}

/* execute an left-to-right exclusive scan simultaneously with a
 * right-to-left exclusive scan */
int lwgrp_chain_double_exscan_recursive(
  const void* sendleft,
  void* recvright,
  const void* sendright,
  void* recvleft,
  int count,
  MPI_Datatype type,
  MPI_Op op,
  const lwgrp_chain* group)
{
  /* TODO: use recursive doubling so all procs do same ops */

  /* get chain info */
  MPI_Comm comm  = group->comm;
  int left_rank  = group->comm_left;
  int right_rank = group->comm_right;

  /* adjust for lower bounds */
  void* tempsendleft  = lwgrp_type_dtbuf_alloc(count, type, __FILE__, __LINE__);
  void* tempsendright = lwgrp_type_dtbuf_alloc(count, type, __FILE__, __LINE__);
  void* temprecvleft  = lwgrp_type_dtbuf_alloc(count, type, __FILE__, __LINE__);
  void* temprecvright = lwgrp_type_dtbuf_alloc(count, type, __FILE__, __LINE__);

  /* intialize send buffers */
  if (sendleft != MPI_IN_PLACE) {
    lwgrp_type_dtbuf_memcpy(tempsendleft, sendleft, count, type);
  } else {
    lwgrp_type_dtbuf_memcpy(tempsendleft, recvleft, count, type);
  }
  if (sendright != MPI_IN_PLACE) {
    lwgrp_type_dtbuf_memcpy(tempsendright, sendright, count, type);
  } else {
    lwgrp_type_dtbuf_memcpy(tempsendright, recvright, count, type);
  }

  /* execute double, exclusive scan,
   * one going left-to-right, and another right-to-left */
  MPI_Request request[8];
  MPI_Status  status[8];
  int new_left  = MPI_PROC_NULL;
  int new_right = MPI_PROC_NULL;
  int recvleft_initialized  = 0;
  int recvright_initialized = 0;
  while (left_rank != MPI_PROC_NULL || right_rank != MPI_PROC_NULL) {
    /* first execute the scan portion */
    int k = 0;

    /* if we have a left partner, send it our left-going data and
     * recv its right-going data */
    if (left_rank != MPI_PROC_NULL) {
      /* receive right-going data from the left */
      MPI_Irecv(temprecvleft, count, type, left_rank, LWGRP_MSG_TAG_0, comm, &request[k]);
      k++;

      /* inform rank to our left of the rank on our right, and send
       * it our data */
      MPI_Isend(tempsendleft, count, type, left_rank, LWGRP_MSG_TAG_0, comm, &request[k]);
      k++;
    }

    /* if we have a right partner, send it our right-going data and
     * recv its left-going data */
    if (right_rank != MPI_PROC_NULL) {
      /* receive left-going data from the right */
      MPI_Irecv(temprecvright, count, type, right_rank, LWGRP_MSG_TAG_0, comm, &request[k]);
      k++;

      /* inform rank to our right of the rank on our left, and send
       * it our data */
      MPI_Isend(tempsendright, count, type, right_rank, LWGRP_MSG_TAG_0, comm, &request[k]);
      k++;
    }

    /* wait for all communication to complete */
    if (k > 0) {
      MPI_Waitall(k, request, status);
    }

    /* if we have a left partner, merge its data with our result and
     * our right-going data */
    if (left_rank != MPI_PROC_NULL) {
      /* reduce data into right-going buffer */
      MPI_Reduce_local(temprecvleft, tempsendright, count, type, op);

      /* with exscan our recvbuf is not valid in the first iteration */
      if (recvleft_initialized) {
        MPI_Reduce_local(temprecvleft, recvleft, count, type, op);
      } else {
        lwgrp_type_dtbuf_memcpy(recvleft, temprecvleft, count, type);
        recvleft_initialized = 1;
      }
    }

    /* if we have a right partner, merge its data with our left-going data */
    if (right_rank != MPI_PROC_NULL) {
      /* reduce data into left-going buffer */
      MPI_Reduce_local(temprecvright, tempsendleft, count, type, op);

      /* with exscan our recvbuf is not valid in the first iteration */
      if (recvright_initialized) {
        MPI_Reduce_local(temprecvright, recvright, count, type, op);
      } else {
        lwgrp_type_dtbuf_memcpy(recvright, temprecvright, count, type);
        recvright_initialized = 1;
      }
    }

    /* now exchange addresses for the next iteration */
    k = 0;

    /* if we have a left partner, send it our left-going data and
     * recv its right-going data */
    if (left_rank != MPI_PROC_NULL) {
      /* receive right-going data from the left */
      MPI_Irecv(&new_left, 1, MPI_INT, left_rank, LWGRP_MSG_TAG_0, comm, &request[k]);
      k++;

      /* inform rank to our left of the rank on our right, and send
       * it our data */
      MPI_Isend(&right_rank, 1, MPI_INT, left_rank, LWGRP_MSG_TAG_0, comm, &request[k]);
      k++;
    }

    /* if we have a right partner, send it our right-going data and
     * recv its left-going data */
    if (right_rank != MPI_PROC_NULL) {
      /* receive left-going data from the right */
      MPI_Irecv(&new_right, 1, MPI_INT, right_rank, LWGRP_MSG_TAG_0, comm, &request[k]);
      k++;

      /* inform rank to our right of the rank on our left, and send
       * it our data */
      MPI_Isend(&left_rank, 1, MPI_INT, right_rank, LWGRP_MSG_TAG_0, comm, &request[k]);
      k++;
    }

    /* wait for all communication to complete */
    if (k > 0) {
      MPI_Waitall(k, request, status);
    }

    /* get next left and right processes */
    left_rank = new_left;
    right_rank = new_right;
  }

  /* free memory */
  lwgrp_type_dtbuf_free(&temprecvright, type, __FILE__, __LINE__);
  lwgrp_type_dtbuf_free(&temprecvleft,  type, __FILE__, __LINE__);
  lwgrp_type_dtbuf_free(&tempsendright, type, __FILE__, __LINE__);
  lwgrp_type_dtbuf_free(&tempsendleft,  type, __FILE__, __LINE__);

  return LWGRP_SUCCESS; 
}
