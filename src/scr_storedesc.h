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

#ifndef SCR_STOREDESC_H
#define SCR_STOREDESC_H

#include "scr_globals.h"

/*
=========================================
Define store descriptor structure
=========================================
*/

typedef struct {
  int      enabled;   /* flag indicating whether this descriptor is active */
  int      index;     /* each descriptor is indexed starting from 0 */
  int      max_count; /* maximum number of datasets to be stored in device */
  char*    base;      /* base directory holding storage */
  MPI_Comm comm;      /* communicator of processes that can access storage */
  int      rank;      /* local rank of process in communicator */
  int      ranks;     /* number of ranks in communicator */
} scr_storedesc;

/*
=========================================
Store descriptor functions
=========================================
*/

/* initialize the specified redundancy descriptor */
int scr_storedesc_init(scr_storedesc* store);

/* free any memory associated with the specified redundancy descriptor */
int scr_storedesc_free(scr_storedesc* store);

/* duplicate a store descriptor */
int scr_storedesc_copy(scr_storedesc* out, const scr_storedesc* in);

/* build a store descriptor corresponding to the specified hash */
int scr_storedesc_create_from_hash(scr_storedesc* s, int index, const scr_hash* hash);

/*
=========================================
Routines that operate on scr_storedescs array
=========================================
*/

int scr_storedescs_index_from_base(const char* base);

/* fill in scr_storedescs array */
int scr_storedescs_create();

/* free scr_storedescs array */
int scr_storedescs_free();

#endif
