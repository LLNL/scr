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
int scr_groupdescs_create(MPI_Comm comm);

/* free scr_groupdescs array */
int scr_groupdescs_free(void);

#endif
