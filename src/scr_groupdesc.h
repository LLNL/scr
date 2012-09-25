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

#ifndef SCR_GROUPDESC_H
#define SCR_GROUPDESC_H

#include "scr_globals.h"

/*
=========================================
Define group descriptor structure
=========================================
*/

typedef struct {
  int      enabled;      /* flag indicating whether this descriptor is active */
  int      index;        /* each descriptor is indexed starting from 0 */
  char*    name;         /* name of group */
  MPI_Comm comm;         /* communicator of processes in same group */
  int      rank;         /* local rank of process in communicator */
  int      ranks;        /* number of ranks in communicator */
} scr_groupdesc;

/*
=========================================
Group descriptor functions
=========================================
*/

/* initialize the specified group descriptor */
int scr_groupdesc_init(scr_groupdesc* group);

/* free any memory associated with the specified group descriptor */
int scr_groupdesc_free(scr_groupdesc* group);

/*
=========================================
Routines that operate on scr_groupdescs array
=========================================
*/

/* given a group name, return its index within scr_groupdescs array,
 * returns -1 if not found */
int scr_groupdescs_index_from_name(const char* name);

/* given a group name, return pointer to groupdesc struct within
 * scr_groupdescs array, returns NULL if not found */
scr_groupdesc* scr_groupdescs_from_name(const char* name);

/* fill in scr_groupdescs array from scr_groupdescs_hash */
int scr_groupdescs_create();

/* free scr_groupdescs array */
int scr_groupdescs_free();

#endif
