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

/* Based on "Exascale Algorithms for Generalized MPI_Comm_split",
 * EuroMPI 2011, Adam Moody, Dong H. Ahn, and Bronis R. de Supinkski
 *
 * Light-weight distributed group representations as chains and rings
 * that only track addresses of left and right neighbors in the group
 * along with the group size and rank of the local process using O(1)
 * memory.  Also provides logchains and logrings that track members
 * 2^d hops away to the left and right sides O(log N) memory. */

int LWGRP_MSG_TAG_0 = 0;
#define LWGRP_FAILURE (! LWGRP_SUCCESS)

/* -----------------------------------------------------
 * Functions that operate on a chain
 * -------------------------------------------------- */

int lwgrp_chain_set_null(lwgrp_chain* group)
{
  if (group != NULL) {
    /* the check for a NULL group is the size of the group */
    group->comm       = MPI_COMM_NULL;
    group->comm_rank  = MPI_PROC_NULL;
    group->comm_left  = MPI_PROC_NULL;
    group->comm_right = MPI_PROC_NULL;
    group->group_rank = -1;
    group->group_size =  0;
  }

  return LWGRP_SUCCESS;
}

int lwgrp_chain_copy(const lwgrp_chain* in, lwgrp_chain* out)
{
  if (out == NULL) {
    return LWGRP_FAILURE;
  }

  if (in != NULL) {
    /* simple struct, so just do a memcpy of it */
    memcpy(out, in, sizeof(lwgrp_chain));
  } else {
    /* in == NULL, so set out to empty group */
    lwgrp_chain_set_null(out);
  }

  return LWGRP_SUCCESS;
}

/* build a group from a communicator */
int lwgrp_chain_build_from_mpicomm(MPI_Comm comm, lwgrp_chain* group)
{
  /* check that we have a valid group pointer */
  if (group == NULL) {
    return LWGRP_FAILURE;
  }

  if (comm != MPI_COMM_NULL) {
    /* get our rank and the size of our communicator */
    int rank, ranks;
    MPI_Comm_rank(comm, &rank);
    MPI_Comm_size(comm, &ranks);

    /* identify rank of our left neighbor, if we have one */
    int left = rank - 1;
    if (left < 0) {
      left = MPI_PROC_NULL;
    }

    /* identify rank of our right neighbor, if we have one */
    int right = rank + 1;
    if (right >= ranks) {
      right = MPI_PROC_NULL;
    }

    /* set the group parameters */
    group->comm       = comm;
    group->comm_rank  = rank;
    group->comm_left  = left;
    group->comm_right = right;
    group->group_rank = rank;
    group->group_size = ranks;
  } else {
    /* passed the NULL communicator, so set the group to empty */
    lwgrp_chain_set_null(group);
  }

  return LWGRP_SUCCESS;
}

/* enables a library to construct a group with just
 * left/right/size/rank values, useful for libs that create their own
 * groups via scans */
int lwgrp_chain_build_from_vals(
  MPI_Comm comm,
  int left,
  int right,
  int size,
  int rank,
  lwgrp_chain* group)
{
  /* check that we got a valid pointer */
  if (group == NULL) {
    return LWGRP_FAILURE;
  }

  /* if size is not positive, set the group to the empty group */
  if (size <= 0) {
    lwgrp_chain_set_null(group);
    return LWGRP_SUCCESS;
  }

  /* get our rank within comm */
  int comm_rank;
  MPI_Comm_rank(comm, &comm_rank);

  /* fill in group structure with input params */
  group->comm       = comm;
  group->comm_rank  = comm_rank;
  group->comm_left  = left;
  group->comm_right = right;
  group->group_rank = rank;
  group->group_size = size;

  return LWGRP_SUCCESS;
}

/* build a group from a communicator (we do this a lot) */
int lwgrp_chain_build_from_ring(
  const lwgrp_ring* ring,
  lwgrp_chain* group)
{
  /* they are made from the same struct, so just do a memcpy */
  lwgrp_chain_copy((const lwgrp_chain*)ring, group);

  /* chop ring at first and last rank */
  if (group != NULL) {
    if (group->group_rank == 0) {
      group->comm_left = MPI_PROC_NULL;
    }
    if (group->group_rank == group->group_size-1) {
      group->comm_right = MPI_PROC_NULL;
    }
  }

  return LWGRP_SUCCESS;
}

/* free off resources associated with list object */
int lwgrp_chain_free(lwgrp_chain* group)
{
  /* nothing to free really, just set values back to NULL */
  int rc = lwgrp_chain_set_null(group);
  return rc;
}

/* -----------------------------------------------------
 * Functions that operate on a ring
 * -------------------------------------------------- */

int lwgrp_ring_set_null(lwgrp_ring* group)
{
  /* same structure, so just use the chain routine */
  int rc = lwgrp_chain_set_null((lwgrp_chain*) group);
  return rc;
}

int lwgrp_ring_copy(const lwgrp_ring* in, lwgrp_ring* out)
{
  /* same structure, so just use the chain routine */
  int rc = lwgrp_chain_copy((const lwgrp_chain*) in, (lwgrp_chain*) out);
  return rc;
}

/* build a group from a communicator */
int lwgrp_ring_build_from_mpicomm(MPI_Comm comm, lwgrp_ring* group)
{
  if (comm != MPI_COMM_NULL) {
    /* get our rank and the size of our communicator */
    int rank, ranks;
    MPI_Comm_rank(comm, &rank);
    MPI_Comm_size(comm, &ranks);

    /* wrap around to highest rank */
    int left = rank - 1;
    if (left < 0) {
      left = ranks - 1;
    }

    /* wrap around to rank 0 */
    int right = rank + 1;
    if (right >= ranks) {
      right = 0;
    }

    /* set output parameters */
    group->comm       = comm;
    group->comm_rank  = rank;
    group->comm_left  = left;
    group->comm_right = right;
    group->group_rank = rank;
    group->group_size = ranks;
  } else {
    lwgrp_ring_set_null(group);
  }

  return LWGRP_SUCCESS;
}

/* build a group from a list of ranks */
int lwgrp_ring_build_from_list(MPI_Comm comm, int group_size, const int group_list[], lwgrp_ring* group)
{
  if (comm != MPI_COMM_NULL && group_size > 0) {
    /* get our rank and the size of our communicator */
    int rank, ranks;
    MPI_Comm_rank(comm, &rank);
    MPI_Comm_size(comm, &ranks);

    /* search until we find our rank within the group_list */
    int i;
    int group_rank = -1;
    for (i = 0; i < group_size; i++) {
      if (group_list[i] == rank) {
        group_rank = i;
        break;
      }
    }

    if (group_rank < 0) {
      /* ERROR: rank not found in list */
    }

    /* identify our left neighbor */
    int left;
    if (group_rank > 0) {
      /* pick rank which is one less */
      left = group_list[group_rank-1];
    } else {
      /* wrap around to right end */
      left = group_list[group_size-1];
    }

    /* identify our right neighbor */
    int right;
    if (group_rank < group_size-1) {
      /* pick rank which is one more */
      right = group_list[group_rank+1];
    } else {
      /* wrap around to left end */
      right = group_list[0];
    }

    /* set output parameters */
    group->comm       = comm;
    group->comm_rank  = rank;
    group->comm_left  = left;
    group->comm_right = right;
    group->group_rank = group_rank;
    group->group_size = group_size;
  } else {
    lwgrp_ring_set_null(group);
  }

  return LWGRP_SUCCESS;
}

int lwgrp_ring_build_from_chain(
  const lwgrp_chain* chain,
  lwgrp_ring* ring)
{
  /* first rank needs to know address of last rank, and vice versa,
   * so we execute a double broadcast to get these values */
  int send_left[2];
  int send_right[2];
  int recv_left[2];
  int recv_right[2];

  /* initialize the values we'll send to the left and right sides,
   * because of how this operation works, the final send_right value
   * will be the value specified by rank 0 and the final send_left
   * value will be the value specified by rank N-1. */
  send_right[1] = chain->comm_rank;
  send_left[1]  = chain->comm_rank;

  /* execute the broadcast operation */
  MPI_Request request[4];
  MPI_Status  status[4];
  MPI_Comm comm  = chain->comm;
  int left_rank  = chain->comm_left;
  int right_rank = chain->comm_right;
  while (left_rank != MPI_PROC_NULL || right_rank != MPI_PROC_NULL) {
    /* if we have a left partner, send it our partial result and
     * our rightmost rank, recv its partial result and its leftmost
     * rank */
    int k = 0;
    if (left_rank != MPI_PROC_NULL) {
      /* receive right-going data from the left */
      MPI_Irecv(
        recv_left, 2, MPI_INT, left_rank, LWGRP_MSG_TAG_0,
        comm, &request[k]
      );
      k++;

      /* inform rank to our left of the rank on our right, and send
       * it our partial result */
      send_left[0] = right_rank;
      MPI_Isend(
        send_left, 2, MPI_INT, left_rank, LWGRP_MSG_TAG_0,
        comm, &request[k]
      );
      k++;
    }

    /* if we have a right partner, send it our partial result and
     * our leftmost rank, receive its partial result and its rightmost
     * rank */
    if (right_rank != MPI_PROC_NULL) {
      /* receive left-going data from the right */
      MPI_Irecv(
        recv_right, 2, MPI_INT, right_rank, LWGRP_MSG_TAG_0,
        comm, &request[k]
      );
      k++;

      /* inform rank to our right of the rank on our left, and send
       * it our partial result */
      send_right[0] = left_rank;
      MPI_Isend(
        send_right, 2, MPI_INT, right_rank, LWGRP_MSG_TAG_0,
        comm, &request[k]
      );
      k++;
    }

    /* wait for all communication to complete */
    if (k > 0) {
      MPI_Waitall(k, request, status);
    }

    /* if we have a left partner, reduce its data */
    if (left_rank != MPI_PROC_NULL) {
      /* take the value we receive from the left, and forward it to
       * the right */
      send_right[1] = recv_left[1];

      /* get the next rank to send to on our left */
      left_rank = recv_left[0];
    }

    /* if we have a right partner, reduce its data */
    if (right_rank != MPI_PROC_NULL) {
      /* take the value we receive from the right, and forward it to
       * the left */
      send_left[1] = recv_right[1];

      /* get the next rank to send to on our right */
      right_rank = recv_right[0];
    }
  }

  /* set our result and return */
  ring->comm       = chain->comm;
  ring->comm_rank  = chain->comm_rank;
  ring->comm_left  = chain->comm_left;
  ring->comm_right = chain->comm_right;
  ring->group_rank = chain->group_rank;
  ring->group_size = chain->group_size;

  /* now form the ring by setting rank 0's left partner to be the
   * last rank in the group and setting the last ranks's right
   * partner to be rank 0 */
  if (ring->group_rank == 0) {
    ring->comm_left = send_left[1];
  }
  if (ring->group_rank == (ring->group_size - 1)) {
    ring->comm_right = send_right[1];
  }

  return LWGRP_SUCCESS;
}

/* free off resources associated with list object */
int lwgrp_ring_free(lwgrp_ring* group)
{
  int rc = lwgrp_ring_set_null(group);
  return rc;
}

/* -----------------------------------------------------
 * Functions that operate on a logchain
 * -------------------------------------------------- */

static int lwgrp_logchain_init(int ranks, lwgrp_logchain* list)
{
  /* initialize the fields to 0 and NULL */
  list->left_size  = 0;
  list->right_size = 0;
  list->left_list  = NULL;
  list->right_list = NULL;

  /* compute ceiling(log(ranks)) to determine the maximum
   * number of neighbors we can have */
  int list_size = 0;
  int count = 1;
  while (count < ranks) {
    list_size++;
    count <<= 1;
  }

  /* we grab one more than we need to store MPI_PROC_NULL at
   * end of each list */
  list_size++;

  /* allocate memory to hold list of left and right rank lists */
  int* left_list  = NULL;
  int* right_list = NULL;
  if (list_size > 0) {
    left_list  = (int*) lwgrp_malloc(list_size * sizeof(int), sizeof(int), __FILE__, __LINE__);
    right_list = (int*) lwgrp_malloc(list_size * sizeof(int), sizeof(int), __FILE__, __LINE__);
    if (left_list == NULL || right_list == NULL) {
      /* TODO: fail */
    }
  }

  /* now that we've allocated memory, assign it to the list struct */
  list->left_list  = left_list;
  list->right_list = right_list;

  return LWGRP_SUCCESS;
}

/* given a group, build a list of neighbors that are 2^d away on
 * our left and right sides */
int lwgrp_logchain_build_from_chain(
  const lwgrp_chain* group,
  lwgrp_logchain* list)
{
  /* get the communicator, our rank in the group, and the size of
   * the group */
  MPI_Comm comm = group->comm;
  int ranks     = group->group_size;

  /* allocate ceil(log(ranks)) memory for left and right lists */
  lwgrp_logchain_init(ranks, list);

  /* build list of left and right ranks in our group */
  MPI_Request request[4];
  MPI_Status  status[4];
  int left_rank  = group->comm_left;
  int right_rank = group->comm_right;
  int recv_left_rank  = MPI_PROC_NULL;
  int recv_right_rank = MPI_PROC_NULL;
  while (left_rank != MPI_PROC_NULL || right_rank != MPI_PROC_NULL) {
    int k = 0;

    /* if we have a left partner, send our rightmost rank to it and
     * receive itsleftmost rank */
    if (left_rank != MPI_PROC_NULL) {
      /* record our current left rank in our list */
      list->left_list[list->left_size] = left_rank;
      list->left_size++;

      /* receive next left rank from our current left rank */
      MPI_Irecv(
        &recv_left_rank, 1, MPI_INT, left_rank, LWGRP_MSG_TAG_0,
        comm, &request[k]
      );
      k++;

      /* send our rightmost rank to our left rank */
      MPI_Isend(
        &right_rank, 1, MPI_INT, left_rank, LWGRP_MSG_TAG_0,
        comm, &request[k]
      );
      k++;
    }

    /* if we have a right partner, send our leftmost rank to it and
     * receive its rightmost rank */
    if (right_rank != MPI_PROC_NULL) {
      /* record our current right rank in our list */
      list->right_list[list->right_size] = right_rank;
      list->right_size++;

      /* receive next right rank from our current right rank */
      MPI_Irecv(
        &recv_right_rank, 1, MPI_INT, right_rank, LWGRP_MSG_TAG_0,
        comm, &request[k]
      );
      k++;

      /* send our leftmost rank to our right rank */
      MPI_Isend(
        &left_rank, 1, MPI_INT, right_rank, LWGRP_MSG_TAG_0,
        comm, &request[k]
      );
      k++;
    }

    /* wait for communication to complete */
    if (k > 0) {
      MPI_Waitall(k, request, status);
    }

    /* update our current left and right ranks to the next set */
    if (left_rank != MPI_PROC_NULL) {
      left_rank = recv_left_rank;
    }
    if (right_rank != MPI_PROC_NULL) {
      right_rank = recv_right_rank;
    }
  }

  /* end each list with MPI_PROC_NULL */
  list->left_list[list->left_size]   = MPI_PROC_NULL;
  list->right_list[list->right_size] = MPI_PROC_NULL;
  list->left_size++;
  list->right_size++;

  return LWGRP_SUCCESS;
}

/* given a group, build a list of neighbors that are 2^d away on
 * our left and right sides */
int lwgrp_logchain_build_from_logring(
  const lwgrp_ring* ring,
  const lwgrp_logring* logring,
  lwgrp_logchain* list)
{
  /* get our rank and the number of ranks in our group */
  int rank  = ring->group_rank;
  int ranks = ring->group_size;

  /* allocate ceil(log(ranks)) memory for left and right lists */
  lwgrp_logchain_init(ranks, list);

  int index = 0;
  int count = 1;
  while (count < ranks) {
    /* set our next left neighbor,
     * and increment our left list size if we have one */
    int left = rank - count;
    if (left >= 0) {
      list->left_list[index] = logring->left_list[index];
      list->left_size++;
    }
    
    /* set our next right neighbor,
     * and increment our right list size if we have one */
    int right = rank + count;
    if (right < ranks) {
      list->right_list[index] = logring->right_list[index];
      list->right_size++;
    }

    index++;
    count <<= 1;
  }

  /* end each list with MPI_PROC_NULL */
  list->left_list[list->left_size]   = MPI_PROC_NULL;
  list->right_list[list->right_size] = MPI_PROC_NULL;
  list->left_size++;
  list->right_size++;

  return LWGRP_SUCCESS;
}

/* given a group, build a list of neighbors that are 2^d away on
 * our left and right sides */
int lwgrp_logchain_build_from_mpicomm(MPI_Comm comm, lwgrp_logchain* list)
{
  /* get the communicator, our rank in the group, and the size of
   * the group */
  int rank, ranks;
  MPI_Comm_rank(comm, &rank);
  MPI_Comm_size(comm, &ranks);

  /* allocate ceil(log(ranks)) memory for left and right lists */
  lwgrp_logchain_init(ranks, list);

  int index = 0;
  int count = 1;
  while (count < ranks) {
    /* set our next left neighbor,
     * and increment our left list size if we have one */
    int left = rank - count;
    if (left >= 0) {
      list->left_list[index] = left;
      list->left_size++;
    }
    
    /* set our next right neighbor,
     * and increment our right list size if we have one */
    int right = rank + count;
    if (right < ranks) {
      list->right_list[index] = right;
      list->right_size++;
    }

    index++;
    count <<= 1;
  }

  /* end each list with MPI_PROC_NULL */
  list->left_list[list->left_size]   = MPI_PROC_NULL;
  list->right_list[list->right_size] = MPI_PROC_NULL;
  list->left_size++;
  list->right_size++;

  return LWGRP_SUCCESS;
}

/* free off resources associated with list object */
int lwgrp_logchain_free(lwgrp_logchain* list)
{
  if (list != NULL) {
    list->left_size  = 0;
    list->right_size = 0;

    lwgrp_free(&list->left_list);
    lwgrp_free(&list->right_list);
  }

  return LWGRP_SUCCESS;
}

/* -----------------------------------------------------
 * Functions that operate on a logring
 * -------------------------------------------------- */

static int lwgrp_logring_init(int ranks, lwgrp_logring* list)
{
  int rc = lwgrp_logchain_init(ranks, (lwgrp_logchain*)list);
  return rc;
}

/* given a group, build a list of neighbors that are 2^d away on
 * our left and right sides */
int lwgrp_logring_build_from_ring(
  const lwgrp_ring* group,
  lwgrp_logring* list)
{
  /* get the communicator, our rank in the group, and the size of
   * the group */
  MPI_Comm comm = group->comm;
  int ranks     = group->group_size;

  /* allocate ceil(log(ranks)) memory for left and right lists */
  lwgrp_logring_init(ranks, list);

  /* build list of left and right ranks in our group */
  MPI_Request request[4];
  MPI_Status  status[4];
  int left_rank  = group->comm_left;
  int right_rank = group->comm_right;
  int recv_left_rank  = MPI_PROC_NULL;
  int recv_right_rank = MPI_PROC_NULL;
  int dist = 1;
  while (dist < ranks) {
    /* record our current left rank in our list */
    list->left_list[list->left_size] = left_rank;
    list->left_size++;

    /* record our current right rank in our list */
    list->right_list[list->right_size] = right_rank;
    list->right_size++;

    /* receive next left rank from current left rank,
     * and receive next right rank from current right rank */
    MPI_Irecv(
      &recv_left_rank,  1, MPI_INT, left_rank,  LWGRP_MSG_TAG_0,
      comm, &request[0]
    );
    MPI_Irecv(
      &recv_right_rank, 1, MPI_INT, right_rank, LWGRP_MSG_TAG_0,
      comm, &request[1]
    );

    /* send our current right rank to our left rank,
     * and send our current left rank to our right rank */
    MPI_Isend(
      &right_rank, 1, MPI_INT, left_rank,  LWGRP_MSG_TAG_0,
      comm, &request[2]
    );
    MPI_Isend(
      &left_rank,  1, MPI_INT, right_rank, LWGRP_MSG_TAG_0,
      comm, &request[3]
    );

    /* wait for communication to complete */
    MPI_Waitall(4, request, status);

    /* update our current left and right ranks to the next set */
    left_rank  = recv_left_rank;
    right_rank = recv_right_rank;
    dist <<= 1;
  }

  /* end each list with MPI_PROC_NULL */
  list->left_list[list->left_size]   = MPI_PROC_NULL;
  list->right_list[list->right_size] = MPI_PROC_NULL;
  list->left_size++;
  list->right_size++;

  return LWGRP_SUCCESS;
}

/* given a group, build a list of neighbors that are 2^d away on
 * our left and right sides */
int lwgrp_logring_build_from_mpicomm(MPI_Comm comm, lwgrp_logring* list)
{
  /* get the communicator, our rank in the group, and the size of
   * the group */
  int rank, ranks;
  MPI_Comm_rank(comm, &rank);
  MPI_Comm_size(comm, &ranks);

  /* allocate ceil(log(ranks)) memory for left and right lists */
  lwgrp_logring_init(ranks, list);

  int count = 1;
  while (count < ranks) {
    /* set our next left neighbor,
     * and increment our left list size */
    int left = (rank - count + ranks) % ranks;
    list->left_list[list->left_size] = left;
    list->left_size++;
    
    /* set our next right neighbor,
     * and increment our right list size */
    int right = (rank + count) % ranks;
    list->right_list[list->right_size] = right;
    list->right_size++;

    count <<= 1;
  }

  /* end each list with MPI_PROC_NULL */
  list->left_list[list->left_size]   = MPI_PROC_NULL;
  list->right_list[list->right_size] = MPI_PROC_NULL;
  list->left_size++;
  list->right_size++;

  return LWGRP_SUCCESS;
}

/* build a group from a list of ranks */
int lwgrp_logring_build_from_list(MPI_Comm comm, int group_size, const int group_list[], lwgrp_logring* list)
{
  /* allocate ceil(log(ranks)) memory for left and right lists */
  lwgrp_logring_init(group_size, list);

  /* get our rank and the size of our communicator */
  int rank, ranks;
  MPI_Comm_rank(comm, &rank);
  MPI_Comm_size(comm, &ranks);

  /* search until we find our rank within the group_list */
  int i;
  int group_rank = -1;
  for (i = 0; i < group_size; i++) {
    if (group_list[i] == rank) {
      group_rank = i;
      break;
    }
  }

  if (group_rank < 0) {
    /* ERROR: rank not found in list */
  }

  int count = 1;
  while (count < group_size) {
    /* set our next left neighbor,
     * and increment our left list size */
    int left_index = group_rank - count;
    if (left_index < 0) {
      left_index += group_size;
    }
    int left_rank = group_list[left_index];
    list->left_list[list->left_size] = left_rank;
    list->left_size++;
    
    /* set our next right neighbor,
     * and increment our right list size */
    int right_index = group_rank + count;
    if (right_index >= group_size) {
      right_index -= group_size;
    }
    int right_rank = group_list[right_index];
    list->right_list[list->right_size] = right_rank;
    list->right_size++;

    count <<= 1;
  }

  /* end each list with MPI_PROC_NULL */
  list->left_list[list->left_size]   = MPI_PROC_NULL;
  list->right_list[list->right_size] = MPI_PROC_NULL;
  list->left_size++;
  list->right_size++;

  return LWGRP_SUCCESS;
}

/* free off resources associated with list object */
int lwgrp_logring_free(lwgrp_logring* list)
{
  int rc = lwgrp_logchain_free((lwgrp_logchain*)list);
  return rc;
}
