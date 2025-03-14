/* Copyright (c) 2012, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory.
 * Written by Adam Moody <moody20@llnl.gov>.
 * LLNL-CODE-568372.
 * All rights reserved.
 * This file is part of the LWGRP library.
 * For details, see https://github.com/hpc/lwgrp
 * Please also read this file: LICENSE.TXT. */

#include <stdlib.h>

#include "mpi.h"
#include "lwgrp.h"
#include "lwgrp_internal.h"

/* assumes the chain has an exact power of two number of members,
 * input should be in resultbuf and output will be stored there,
 * scratchbuf should be scratch space */
static int lwgrp_logchain_allreduce_recursive_pow2(
  void* resultbuf,
  void* scratchbuf,
  int count,
  MPI_Datatype type,
  MPI_Op op,
  const lwgrp_chain* group,
  const lwgrp_logchain* list)
{
  /* we use a recurisve doubling algorithm */
  MPI_Status  status[4];

  /* get our rank within comm */
  MPI_Comm comm = group->comm;
  int rank      = group->group_rank;
  int ranks     = group->group_size;

  /* execute recursive doubling operation */
  int mask = 1;
  int index = 0;
  while (mask < ranks) {
    /* get rank of partner in comm */
    int partner;
    int exchange_rank = rank ^ mask;
    if (exchange_rank < rank) {
      partner = list->left_list[index];
    } else {
      partner = list->right_list[index];
    }

    /* exchange data with partner */
    MPI_Sendrecv(
      resultbuf,  count, type, partner, LWGRP_MSG_TAG_0,
      scratchbuf, count, type, partner, LWGRP_MSG_TAG_0,
      comm, status
    );

    /* reduce data (being careful about non-commutative ops) */
    if (exchange_rank < rank) {
      /* higher order data is in resultbuf, so resultbuf = scratchbuf + resultbuf */
      MPI_Reduce_local(scratchbuf, resultbuf, count, type, op);
    } else {
      /* higher order data is in scratchbuf, so scratchbuf = resultbuf + scratchbuf,
       * then copy result back to resultbuf for sending in next round */
      MPI_Reduce_local(resultbuf, scratchbuf, count, type, op);
      lwgrp_type_dtbuf_memcpy(resultbuf, scratchbuf, count, type);
    }

    /* prepare for next iteration */
    mask <<= 1;
    index++;
  }

  return LWGRP_SUCCESS;
}

int lwgrp_logchain_allreduce_recursive(
  const void* sendbuf,
  void* recvbuf,
  int count,
  MPI_Datatype type,
  MPI_Op op,
  const lwgrp_chain* group,
  const lwgrp_logchain* list)
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

  /* allocate buffer to receive partial results */
  void* tempbuf = lwgrp_type_dtbuf_alloc(count, type, __FILE__, __LINE__);

  /* find largest power of two that fits within group_ranks */
  int pow2, log2;
  lwgrp_largest_pow2_log2_lte(ranks, &pow2, &log2);

  /* invoke power-of-two algorithm directly if we can */
  if (ranks == pow2) {
    /* note that this takes recvbuf / scratch as params rather than sendbuf / recvbuf */
    int rc = lwgrp_logchain_allreduce_recursive_pow2(recvbuf, tempbuf, count, type, op, group, list);

    /* free our scratch space */
    lwgrp_type_dtbuf_free(&tempbuf, type, __FILE__, __LINE__);

    return rc;
  }

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
          int left_rank = list->left_list[0];
          MPI_Send(recvbuf, count, type, left_rank, LWGRP_MSG_TAG_0, comm);
        } else {
          /* recv data from odd rank out on right */
          int right_rank = list->right_list[0];
          MPI_Recv(tempbuf, count, type, right_rank, LWGRP_MSG_TAG_0, comm, status);

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

      /* everyone who has a left neighbor will get a new one */
      if (rank > 0) {
        new_left = list->left_list[1];
      }

      /* everyone but the cutoff rank gets a new right neighbor */
      if (rank < cutoff) {
        new_right = list->right_list[1];
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

  /* power of two reduce using chain instead of logchain */
  if (! odd_rank_out) {
    lwgrp_chain_allreduce_recursive_pow2(recvbuf, tempbuf, count, type, op, pow2_group);
  }

  /* send message back to odd ranks out */
  if (rank < cutoff) {
      if (rank & 0x1) {
        /* recv result from left rank */
        int left_rank = list->left_list[0];
        MPI_Recv(recvbuf, count, type, left_rank, LWGRP_MSG_TAG_0, comm, status);
      } else {
        /* send result to right rank */
        int right_rank = list->right_list[0];
        MPI_Send(recvbuf, count, type, right_rank, LWGRP_MSG_TAG_0, comm);
      }
  }

  /* free our scratch space */
  lwgrp_type_dtbuf_free(&tempbuf, type, __FILE__, __LINE__);

  return LWGRP_SUCCESS;
}

int lwgrp_logchain_reduce_recursive(
  const void* sendbuf,
  void* recvbuf,
  int count,
  MPI_Datatype type,
  MPI_Op op,
  int root,
  const lwgrp_chain* group,
  const lwgrp_logchain* list)
{
  /* TODO: actually implement a reduce rather than borrowing allreduce */

  /* allocate buffer to receive partial results */
  void* buf = lwgrp_type_dtbuf_alloc(count, type, __FILE__, __LINE__);

  /* if we're the root, use the recvbuf,
   * otherwise use the temporary buffer */
  void* tempbuf = NULL;
  int rank = group->group_rank;
  if (rank == root) {
    tempbuf = recvbuf;
  } else {
    tempbuf = buf;
  }

  /* execute the allreduce */
  int rc = lwgrp_logchain_allreduce_recursive(
    sendbuf, tempbuf, count, type, op, group, list
  );

  /* free our scratch space */
  lwgrp_type_dtbuf_free(&buf, type, __FILE__, __LINE__);

  return rc; 
}
