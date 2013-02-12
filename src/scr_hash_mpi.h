/*
 * Copyright (c) 2009, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory.
 * Written by Adam Moody <moody20@llnl.gov>.
 * LLNL-CODE-411039.
 * All rights reserved.
 * This file is part of The Scalable Checkpoint / Restart (SCR) library.
 * For details, see https://sourceforge.net/projects/scalablecr/
 * Please also read this file: LICENSE.TXT.
*/

#ifndef SCR_HASH_MPI_H
#define SCR_HASH_MPI_H

#include "mpi.h"
#include "scr_hash.h"

/*
=========================================
Hash MPI transfer functions
=========================================
*/

/* packs and send the given hash to the specified rank */
int scr_hash_send(const scr_hash* hash, int rank, MPI_Comm comm);

/* receives a hash from the specified rank and unpacks it into specified hash */
int scr_hash_recv(scr_hash* hash, int rank, MPI_Comm comm);

/* send and receive a hash in the same step */
int scr_hash_sendrecv(
    const scr_hash* hash_send, int rank_send,
          scr_hash* hash_recv, int rank_recv,
    MPI_Comm comm
);

/* broadcasts a hash from a root and unpacks it into specified hash on all other tasks */
int scr_hash_bcast(scr_hash* hash, int root, MPI_Comm comm);

/* execute a (sparse) global exchange, similar to an alltoallv operation
 * 
 * hash_send specifies destinations as:
 * <rank_X>
 *   <hash_to_send_to_rank_X>
 * <rank_Y>
 *   <hash_to_send_to_rank_Y>
 *
 * hash_recv returns hashes sent from remote ranks as:
 * <rank_A>
 *   <hash_received_from_rank_A>
 * <rank_B>
 *   <hash_received_from_rank_B> */
int scr_hash_exchange(const scr_hash* hash_send, scr_hash* hash_recv, MPI_Comm comm);

typedef enum {
  SCR_HASH_EXCHANGE_RIGHT = 0,
  SCR_HASH_EXCHANGE_LEFT,
} scr_hash_exchange_enum;

/* like scr_hash_exchange, but with a direction specified for Bruck's
 * algorithm */
int scr_hash_exchange_direction(
  const scr_hash* hash_send,
        scr_hash* hash_recv,
  MPI_Comm comm,
  scr_hash_exchange_enum direction
);

#endif
