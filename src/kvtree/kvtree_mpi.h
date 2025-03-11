#ifndef KVTREE_MPI_H
#define KVTREE_MPI_H

#include "mpi.h"
#include "kvtree.h"

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
extern "C" {
#endif

/** \file kvtree_mpi.h
 *  \ingroup kvtree
 *  \brief Hash MPI transfer functions
 */

/** packs and send the given hash to the specified rank */
int kvtree_send(const kvtree* hash, int rank, MPI_Comm comm);

/** receives a hash from the specified rank and unpacks it into specified hash */
int kvtree_recv(kvtree* hash, int rank, MPI_Comm comm);

/** send and receive a hash in the same step */
int kvtree_sendrecv(
    const kvtree* hash_send, int rank_send,
          kvtree* hash_recv, int rank_recv,
    MPI_Comm comm
);

/** broadcasts a hash from a root and unpacks it into specified hash on all other tasks */
int kvtree_bcast(kvtree* hash, int root, MPI_Comm comm);

/** insert message destined for rank into send kvtree,
 * merges msg with any existing data destined for the same rank */
int kvtree_exchange_sendq(
  kvtree* send_hash, /**< [OUT] kvtree to attach message to */
  int rank,          /**< [IN] destination rank */
  const kvtree* msg  /**< [IN] data to be send to rank */
);

/** execute a (sparse) global exchange, similar to an alltoallv operation
 *
 * hash_send specifies destinations as:
 *
 *     <rank_X>
 *       <hash_to_send_to_rank_X>
 *     <rank_Y>
 *       <hash_to_send_to_rank_Y>
 *
 * hash_recv returns hashes sent from remote ranks as:
 *
 *     <rank_A>
 *       <hash_received_from_rank_A>
 *     <rank_B>
 *       <hash_received_from_rank_B>
 */
int kvtree_exchange(const kvtree* hash_send, kvtree* hash_recv, MPI_Comm comm);

typedef enum {
  KVTREE_EXCHANGE_RIGHT = 0,
  KVTREE_EXCHANGE_LEFT,
} kvtree_exchange_enum;

/** like kvtree_exchange, but with a direction specified for Bruck's
 * algorithm */
int kvtree_exchange_direction(
  const kvtree* hash_send,
        kvtree* hash_recv,
  MPI_Comm comm,
  kvtree_exchange_enum direction
);

/** gather data from each process in comm, and write to a series of files
 * specified by the path and filename prefix */
int kvtree_write_gather(
  const char* prefix, /**< [IN] - path/filename prefix to prepend to filenames */
  kvtree* data,       /**< [IN] - input data */
  MPI_Comm comm       /**< [IN] - communicator of participating processes */
);

/** read from a series of files specified by the path and filename prefix,
 * and scatter data to each process in comm */
int kvtree_read_scatter(
  const char* prefix, /**< [IN]  - path/filename prefix to prepend to filenames */
  kvtree* data,       /**< [OUT] - output data */
  MPI_Comm comm       /**< [IN]  - communicator of participating processes */
);

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
} /* extern "C" */
#endif

#endif
