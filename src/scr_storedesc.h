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
  char*    name;      /* name of store */
  int      max_count; /* maximum number of datasets to be stored in device */
  int      can_mkdir; /* flag indicating whether mkdir/rmdir work */
  MPI_Comm comm;      /* communicator of processes that can access storage */
  int      rank;      /* local rank of process in communicator */
  int      ranks;     /* number of ranks in communicator */
} scr_storedesc;

/*
=========================================
Store descriptor functions
=========================================
*/

/* initialize the specified store descriptor */
int scr_storedesc_init(scr_storedesc* store);

/* free any memory associated with the specified store descriptor */
int scr_storedesc_free(scr_storedesc* store);

/* build a store descriptor corresponding to the specified hash */
int scr_storedesc_create_from_hash(
  scr_storedesc* s, int index, const scr_hash* hash
);

/* create specified directory on store */
int scr_storedesc_dir_create(const scr_storedesc* s, const char* dir);

/* delete specified directory on store */
int scr_storedesc_dir_delete(const scr_storedesc* s, const char* dir);

/*
=========================================
Routines that operate on scr_storedescs array
=========================================
*/

/* lookup index in scr_storedescs given a target name,
 * returns -1 if not found */
int scr_storedescs_index_from_name(const char* name);

/* fill in scr_storedescs array from scr_storedescs_hash */
int scr_storedescs_create();

/* free scr_storedescs array */
int scr_storedescs_free();

#endif
