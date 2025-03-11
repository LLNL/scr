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

/* Debra Hensgen, Raphael Finkel, Udi Manber,
 * "Two algorithms for barrier synchronization",
 * International Journal of Parallel Programming,
 * 1988-02-01, Vol 17, Issue 1 */
int lwgrp_logring_barrier_dissemination(
  const lwgrp_ring* group,
  const lwgrp_logring* list)
{
  int rc = LWGRP_SUCCESS;

  /* get ring info */
  MPI_Comm comm  = group->comm;
  int ranks      = group->group_size;

  /* execute barrier dissemination algorithm */
  int index = 0;
  int dist  = 1;
  while (dist < ranks) {
    /* get source and destination ranks */
    int src = list->left_list[index];
    int dst = list->right_list[index];

    /* send empty messages as a signal */
    MPI_Status status[2];
    MPI_Sendrecv(
      NULL, 0, MPI_BYTE, dst, LWGRP_MSG_TAG_0,
      NULL, 0, MPI_BYTE, src, LWGRP_MSG_TAG_0,
      comm, status
    );

    /* prepare for next iteration */
    index++;
    dist <<= 1;
  } 

  return rc;
}

/* implements a binomail tree */
int lwgrp_logring_bcast_binomial(
  void* buffer,
  int count,
  MPI_Datatype datatype,
  int root,
  const lwgrp_ring* group,
  const lwgrp_logring* list)
{
  int rc = LWGRP_SUCCESS;

  /* get ring info */
  MPI_Comm comm  = group->comm;
  int rank       = group->group_rank;
  int ranks      = group->group_size;

  /* adjust our rank by setting the root to be rank 0 */
  int treerank = rank - root;
  if (treerank < 0) {
    treerank += ranks;
  }

  /* get largest power-of-two strictly less than ranks */
  int pow2, log2;
  lwgrp_largest_pow2_log2_lessthan(ranks, &pow2, &log2);

  /* run through binomial tree */
  int parent = 0;
  int received = (rank == root) ? 1 : 0;
  while (pow2 > 0) {
    /* check whether we need to receive or send data */
    if (! received) {
      /* if we haven't received a message yet, see if the parent for
       * this step will send to us */
      int target = parent + pow2;
      if (treerank == target) {
        /* we're the target, get the real rank and receive data */
        MPI_Status status;
        int src = list->left_list[log2];
        MPI_Recv(
          buffer, count, datatype, src, LWGRP_MSG_TAG_0,
          comm, &status
        );
        received = 1;
      } else if (treerank > target) {
        /* if we are in the top half of the subtree set our new
         * potential parent */
        parent = target;
      }
    } else {
      /* we have received the data, so if we have a child,
       * send data */
      if (treerank + pow2 < ranks) {
        int dst = list->right_list[log2];
        MPI_Send(
          buffer, count, datatype, dst, LWGRP_MSG_TAG_0, comm
        );
      }
    }

    /* cut the step size in half and keep going */
    log2--;
    pow2 >>= 1;
  }

  return rc;
}

/* Jehoshua Bruck, Ching-Tien Ho, Shlomo Kipnis, Eli Upfal,
 * and Derrick Weathersby, "Efficient algorithms for
 * all-to-all communications in multiport message-passing systems",
 * IEEE Transactions on Parallel and Distributed Systems,
 * Vol 8, No 11, 1997 */
int lwgrp_logring_gather_brucks(
  const void* sendbuf,
  void* recvbuf,
  int num,
  MPI_Datatype datatype,
  int root,
  const lwgrp_ring* group,
  const lwgrp_logring* list)
{
  int rc = LWGRP_SUCCESS;

  /* get ring info */
  int rank  = group->group_rank;
  int ranks = group->group_size;

  /* TODO: need to allocate for true extent here since the
   * datatype may not be tileable on the non-root procs */

  /* allocate a temporary receive buffer */
  size_t total_elems = num * ranks;
  void* tmpbuf = lwgrp_type_dtbuf_alloc(
    total_elems, datatype, __FILE__, __LINE__
  );

  /* if we're the root, use recvbuf, otherwise use temporary */
  char* buf;
  if (rank == root) {
    buf = tmpbuf;
  } else {
    buf = recvbuf;
  }

  /* delegate work to allgather */
  rc = lwgrp_logring_allgather_brucks(
    sendbuf, buf, num, datatype,
    group, list
  );

  /* free temporary memory */
  lwgrp_type_dtbuf_free(&tmpbuf, datatype, __FILE__, __LINE__);

  return rc;
}

/* issue an allgather using Bruck's algorithm */
int lwgrp_logring_allgather_brucks(
  const void* sendbuf,
  void* recvbuf,
  int num,
  MPI_Datatype datatype,
  const lwgrp_ring* group,
  const lwgrp_logring* list)
{
  int rc = LWGRP_SUCCESS;

  /* get ring info */
  MPI_Comm comm  = group->comm;
  int rank       = group->group_rank;
  int ranks      = group->group_size;

  /* allocate temporary buffer */
  size_t total_elems = num * ranks;
  void* tmpbuf = lwgrp_type_dtbuf_alloc(
    total_elems, datatype, __FILE__, __LINE__
  );

  /* copy our own data into the temporary buffer */
  const void* inputbuf = sendbuf;
#if MPI_VERSION >= 2
  if (sendbuf == MPI_IN_PLACE) {
    inputbuf = (const void*) lwgrp_type_dtbuf_from_dtbuf(
      recvbuf, num * rank, datatype
    );
  }
#endif
  lwgrp_type_dtbuf_memcpy(tmpbuf, inputbuf, num, datatype);

  /* execute the allgather operation */
  MPI_Request request[2];
  MPI_Status status[2];
  int index = 0;
  int step  = 1;
  int ranks_received = 1;
  while (step < ranks) {
    /* get ranks for left and right partners */
    int src = list->right_list[index];
    int dst = list->left_list[index];

    /* determine number of elements we'll be sending and receiving in
     * this round */
    int ranks_incoming = step;
    if (ranks_received + ranks_incoming > ranks) {
      ranks_incoming = ranks - ranks_received;
    }
    int num_exchange = num * ranks_incoming;

    /* receive data from source */
    void* recv_pos = lwgrp_type_dtbuf_from_dtbuf(
      tmpbuf, ranks_received * num, datatype
    );
    MPI_Irecv(
      recv_pos, num_exchange, datatype, src, LWGRP_MSG_TAG_0,
      comm, &request[0]
    ); 

    /* send the data to destination */
    MPI_Isend(
      tmpbuf, num_exchange, datatype, dst, LWGRP_MSG_TAG_0,
      comm, &request[1]
    );

    /* wait for communication to complete */
    MPI_Waitall(2, request, status);

    /* add the count to the total number we've received */
    ranks_received += ranks_incoming;

    /* go on to next iteration */
    index++;
    step <<= 1;
  }

  /* shift our data back to the proper position in receive buffer */
  int num_pre  = num * rank;
  int num_post = num * (ranks - rank);
  void* buf_pre  = lwgrp_type_dtbuf_from_dtbuf(
    recvbuf, num_pre, datatype
  );
  void* buf_post = lwgrp_type_dtbuf_from_dtbuf(
    tmpbuf, num_post, datatype
  );
  lwgrp_type_dtbuf_memcpy(buf_pre, tmpbuf,  num_post, datatype);
  lwgrp_type_dtbuf_memcpy(recvbuf, buf_post, num_pre, datatype);

  /* free the temporary buffer */
  lwgrp_type_dtbuf_free(&tmpbuf, datatype, __FILE__, __LINE__);

  return rc;
}

int lwgrp_logring_allgatherv_brucks(
  const void* sendbuf,
  void* recvbuf,
  const int counts[],
  const int displs[],
  MPI_Datatype datatype,
  const lwgrp_ring* group,
  const lwgrp_logring* list)
{
  int i;
  int rc = LWGRP_SUCCESS;


  /* get ring info */
  MPI_Comm comm  = group->comm;
  int rank       = group->group_rank;
  int ranks      = group->group_size;

  /* total up number of items */
  int sum = 0;
  int prefix_sum = 0;
  for (i = 0; i < ranks; i++) {
    sum += counts[i];
    if (i < rank) {
      prefix_sum += counts[i];
    }
  }

  /* free some temporary space to work with */
  void* tmpbuf = lwgrp_type_dtbuf_alloc(
    sum, datatype, __FILE__, __LINE__
  );

  /* copy our own data into the temporary buffer */
  int num = counts[rank];
  const void* inputbuf = sendbuf;
#if MPI_VERSION >= 2
  if (sendbuf == MPI_IN_PLACE) {
    inputbuf = (const void*) lwgrp_type_dtbuf_from_dtbuf(
      recvbuf, prefix_sum, datatype
    );
  }
#endif
  lwgrp_type_dtbuf_memcpy(tmpbuf, inputbuf, num, datatype);

  /* execute the allgather operation */
  MPI_Request request[2];
  MPI_Status status[2];
  int index = 0;
  int step  = 1;
  int ranks_received = 1;
  int num_received   = num;
  while (step < ranks) {
    /* get ranks for left and right partners */
    int src = list->right_list[index];
    int dst = list->left_list[index];

    /* determine number of elements we'll be sending and receiving in
     * this round */
    int ranks_incoming = step;
    if (ranks_received + ranks_incoming > ranks) {
      ranks_incoming = ranks - ranks_received;
    }
    int num_incoming = 0;
    for (i = rank + 1; i < rank + 1 + step; i++) {
      int index = i;
      if (i >= ranks) {
        index -= ranks;
      }
      num_incoming += counts[index];
    }

    /* receive data from source */
    void* recv_pos = lwgrp_type_dtbuf_from_dtbuf(
      tmpbuf, num_received, datatype
    );
    MPI_Irecv(
      recv_pos, num_incoming, datatype, src, LWGRP_MSG_TAG_0,
      comm, &request[0]
    ); 

    /* send the data to destination */
    MPI_Isend(
      tmpbuf, num_received, datatype, dst, LWGRP_MSG_TAG_0,
      comm, &request[1]
    );

    /* wait for communication to complete */
    MPI_Waitall(2, request, status);

    /* add the count to the total number we've received */
    ranks_received += ranks_incoming;
    num_received   += num_incoming;

    /* go on to next iteration */
    index++;
    step <<= 1;
  }

  /* shift our data back to the proper position in receive buffer */
  int num_pre  = prefix_sum;
  int num_post = sum - num_pre;
  void* buf_pre  = lwgrp_type_dtbuf_from_dtbuf(
    recvbuf, num_pre, datatype
  );
  void* buf_post = lwgrp_type_dtbuf_from_dtbuf(
    tmpbuf, num_post, datatype
  );
  lwgrp_type_dtbuf_memcpy(buf_pre, tmpbuf,  num_post, datatype);
  lwgrp_type_dtbuf_memcpy(recvbuf, buf_post, num_pre, datatype);

  /* free the temporary buffer */
  lwgrp_type_dtbuf_free(&tmpbuf, datatype, __FILE__, __LINE__);

  return rc;
}

/* execute an alltoall using Bruck's algorithm */
int lwgrp_logring_alltoall_brucks(
  const void* sendbuf,
  void* recvbuf,
  int num,
  MPI_Datatype datatype,
  const lwgrp_ring* group,
  const lwgrp_logring* list)
{
  int i;
  int rc = LWGRP_SUCCESS;

  /* get ring info */
  MPI_Comm comm  = group->comm;
  int rank       = group->group_rank;
  int ranks      = group->group_size;

  /* TODO: feels like some of these memory copies could be avoided */

  void* send_data = lwgrp_type_dtbuf_alloc(ranks, datatype, __FILE__, __LINE__);
  void* recv_data = lwgrp_type_dtbuf_alloc(ranks, datatype, __FILE__, __LINE__);
  void* tmp_data  = lwgrp_type_dtbuf_alloc(ranks, datatype, __FILE__, __LINE__);

  /* copy our send data to our receive buffer, and rotate it so our own
   * rank is at the top */
  const void* inputbuf = sendbuf;
#if MPI_VERSION >= 2
  if (sendbuf == MPI_IN_PLACE) {
    inputbuf = (const void*) recvbuf;
  }
#endif
  int num_pre  = num * rank;
  int num_post = num * (ranks - rank);
  void* buf_pre  = lwgrp_type_dtbuf_from_dtbuf(inputbuf, num_pre, datatype);
  void* buf_post = lwgrp_type_dtbuf_from_dtbuf(tmp_data, num_post, datatype);
  lwgrp_type_dtbuf_memcpy(tmp_data, buf_pre, num_post, datatype);
  lwgrp_type_dtbuf_memcpy(buf_post, inputbuf, num_pre, datatype);

  /* now run through Bruck's index algorithm to exchange data */
  MPI_Request request[2];
  MPI_Status  status[2];
  int index = 0;
  int step = 1;
  while (step < ranks) {
    /* determine our source and destination ranks for this step */
    int dst = list->right_list[index];
    int src = list->left_list[index];

    /* pack our data to send and count number of bytes */
    int send_count = 0;
    for (i = 0; i < ranks; i++) {
      int mask = (i & step);
      if (mask) {
        void* send_ptr = lwgrp_type_dtbuf_from_dtbuf(send_data, send_count, datatype);
        void* tmp_ptr  = lwgrp_type_dtbuf_from_dtbuf(tmp_data,  i * num,    datatype);
        lwgrp_type_dtbuf_memcpy(send_ptr, tmp_ptr, num, datatype);
        send_count += num;
      }
    }

    /* exchange messages */
    MPI_Irecv(
      recv_data, send_count, datatype, src, LWGRP_MSG_TAG_0,
      comm, &request[0]
    );
    MPI_Isend(
      send_data, send_count, datatype, dst, LWGRP_MSG_TAG_0,
      comm, &request[1]
    );
    MPI_Waitall(2, request, status);

    /* unpack received data into our buffer */
    int recv_count = 0;
    for (i = 0; i < ranks; i++) {
      int mask = (i & step);
      if (mask) {
        void* recv_ptr = lwgrp_type_dtbuf_from_dtbuf(recv_data, recv_count, datatype);
        void* tmp_ptr  = lwgrp_type_dtbuf_from_dtbuf(tmp_data,  i * num,    datatype);
        lwgrp_type_dtbuf_memcpy(tmp_ptr, recv_ptr, num, datatype);
        recv_count += num;
      }
    }

    /* go on to the next phase of the exchange */
    index++;
    step <<= 1;
  }

  /* copy our data to our receive buffer, and rotate it back so our own
   * rank is in its proper position */
  int num_pre2  = num * (rank + 1);
  int num_post2 = num * (ranks - rank - 1);
  void* buf_pre2  = lwgrp_type_dtbuf_from_dtbuf(tmp_data,  num_pre2, datatype);
  void* buf_post2 = lwgrp_type_dtbuf_from_dtbuf(send_data, num_post2, datatype);
  lwgrp_type_dtbuf_memcpy(send_data, buf_pre2, num_post2, datatype);
  lwgrp_type_dtbuf_memcpy(buf_post2, tmp_data, num_pre2,  datatype);

  /* elements are in reverse order, so flip them around */
  for (i = 0; i < ranks; i++) {
    void* buf_dst = lwgrp_type_dtbuf_from_dtbuf(recvbuf, i * num, datatype);
    void* buf_src = lwgrp_type_dtbuf_from_dtbuf(send_data, (ranks - i - 1) * num, datatype);
    lwgrp_type_dtbuf_memcpy(buf_dst, buf_src, num, datatype);
  }

  /* free off our internal data structures */
  lwgrp_type_dtbuf_free(&tmp_data,  datatype, __FILE__, __LINE__);
  lwgrp_type_dtbuf_free(&recv_data, datatype, __FILE__, __LINE__);
  lwgrp_type_dtbuf_free(&send_data, datatype, __FILE__, __LINE__);

  return rc;
}

int lwgrp_logring_alltoallv_linear(
  const void* sendbuf, const int sendcounts[], const int senddispls[],
        void* recvbuf, const int recvcounts[], const int recvdispls[],
  MPI_Datatype datatype, const lwgrp_ring* group, const lwgrp_logring* list)
{
  /* this is really implemented with the ring, so just call that */
  int rc = lwgrp_ring_alltoallv_linear(
    sendbuf, sendcounts, senddispls,
    recvbuf, recvcounts, recvdispls,
    datatype, group
  );
  return rc;
}

int lwgrp_logring_reduce_recursive(
  const void* sendbuf,
  void* recvbuf,
  int count,
  MPI_Datatype type,
  MPI_Op op,
  int root,
  const lwgrp_chain* group,
  const lwgrp_logring* list)
{
  int rc = LWGRP_SUCCESS;

  /* create a chain from the ring */
  lwgrp_chain chain;
  lwgrp_chain_build_from_ring(group, &chain);

  /* create a logchain from the logring */
  lwgrp_logchain logchain;
  lwgrp_logchain_build_from_logring(group, list, &logchain);

  /* hand off to the logchain reduce */
  rc = lwgrp_logchain_reduce_recursive(
    sendbuf, recvbuf, count, type, op, root,
    &chain, &logchain
  );

  /* free the logchain and chain */
  lwgrp_logchain_free(&logchain);
  lwgrp_chain_free(&chain);

  return rc;
}

int lwgrp_logring_allreduce_recursive(
  const void* sendbuf,
  void* recvbuf,
  int count,
  MPI_Datatype type,
  MPI_Op op,
  const lwgrp_chain* group,
  const lwgrp_logring* list)
{
  int rc = LWGRP_SUCCESS;

  /* create a chain from the ring */
  lwgrp_chain chain;
  lwgrp_chain_build_from_ring(group, &chain);

  /* create a logchain from the logring */
  lwgrp_logchain logchain;
  lwgrp_logchain_build_from_logring(group, list, &logchain);

  /* hand off to the logchain allreduce */
  rc = lwgrp_logchain_allreduce_recursive(
    sendbuf, recvbuf, count, type, op,
    &chain, &logchain
  );

  /* free the logchain and chain */
  lwgrp_logchain_free(&logchain);
  lwgrp_chain_free(&chain);

  return rc;
}

/* left-to-right inclusive scan */
int lwgrp_logring_scan_recursive(
  const void* inbuf,
  void* outbuf,
  int count,
  MPI_Datatype type,
  MPI_Op op,
  const lwgrp_ring* group,
  const lwgrp_logring* list)
{
  int rc = LWGRP_SUCCESS;

  /* delegate work to exscan */
  rc = lwgrp_logring_exscan_recursive(
    inbuf, outbuf, count, type, op, group, list
  );

  /* now add in our own result */
  if (group->group_rank > 0) {
    /* reduce our data into result */
    MPI_Reduce_local((void*)inbuf, outbuf, count, type, op);
  } else {
    /* for rank 0, just copy data over */
    lwgrp_type_dtbuf_memcpy(outbuf, inbuf, count, type);
  }

  return rc;
}

/* left-to-right exclusive scan */
int lwgrp_logring_exscan_recursive(
  const void* inbuf,
  void* outbuf,
  int count,
  MPI_Datatype type,
  MPI_Op op,
  const lwgrp_ring* group,
  const lwgrp_logring* list)
{
  int rc = LWGRP_SUCCESS;

  /* create a chain from the ring */
  lwgrp_chain chain;
  lwgrp_chain_build_from_ring(group, &chain);

  /* delegate work to double scan operation */
  rc = lwgrp_chain_exscan_recursive(
    inbuf, outbuf, count, type, op,
    &chain
  );

  /* free the chain */
  lwgrp_chain_free(&chain);

  return rc;
}

int lwgrp_logring_double_exscan_recursive(
  const void* sendleft,
  void* recvright,
  const void* sendright,
  void* recvleft,
  int count,
  MPI_Datatype type,
  MPI_Op op,
  const lwgrp_ring* group,
  const lwgrp_logring* list)
{
  /* convert ring to chain and call chain's double exscan function */
  int rc = LWGRP_SUCCESS;

  /* create a chain from the ring */
  lwgrp_chain chain;
  lwgrp_chain_build_from_ring(group, &chain);

  /* delegate work to chain double exscan */
  rc = lwgrp_chain_double_exscan_recursive(
    sendleft, recvright, sendright, recvleft, count, type, op,
    &chain
  );

  /* free the chain */
  lwgrp_chain_free(&chain);

  return rc;
}
