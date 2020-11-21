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

/* All rights reserved. This program and the accompanying materials
 * are made available under the terms of the BSD-3 license which accompanies this
 * distribution in LICENSE.TXT
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the BSD-3  License in
 * LICENSE.TXT for more details.
 *
 * GOVERNMENT LICENSE RIGHTS-OPEN SOURCE SOFTWARE
 * The Government's rights to use, modify, reproduce, release, perform,
 * display, or disclose this software are subject to the terms of the BSD-3
 * License as provided in Contract No. B609815.
 * Any reproduction of computer software, computer software documentation, or
 * portions thereof marked with this legend must also reproduce the markings.
 *
 * Author: Christopher Holguin <christopher.a.holguin@intel.com>
 *
 * (C) Copyright 2015-2016 Intel Corporation.
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
  char*    xfer;      /* AXL xfer type string (bbapi, sync, pthread, etc..) */
  char*    view;      /* indicates whether store is node-local or global */
  MPI_Comm comm;      /* communicator of processes that can access storage */
  int      rank;      /* local rank of process in communicator */
  int      ranks;     /* number of ranks in communicator */
} scr_storedesc;

/*
=========================================
Store descriptor functions
=========================================
*/

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

/* lookup index in scr_storedescs given a child path
 * within the space of that descriptor,
 * returns -1 if not found */
int scr_storedescs_index_from_child_path(const char* path);

/* fill in scr_storedescs array from scr_storedescs_hash */
int scr_storedescs_create(MPI_Comm comm);

/* free scr_storedescs array */
int scr_storedescs_free(void);

#endif
