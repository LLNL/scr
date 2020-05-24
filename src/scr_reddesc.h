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

#ifndef SCR_REDDESC_H
#define SCR_REDDESC_H

#include "scr_globals.h"

#include "kvtree.h"

/*
=========================================
Define redundancy descriptor structure
=========================================
*/

typedef struct {
  int      enabled;        /* flag indicating whether this descriptor is active */
  int      index;          /* each descriptor is indexed starting from 0 */
  int      interval;       /* how often to apply this descriptor, pick largest such
                            * that interval evenly divides checkpoint id */
  int      output;         /* flag indicating whether this descriptor should be used for output */
  int      bypass;         /* flag indicating whether data should bypass cache */
  int      store_index;    /* index into scr_storedesc for storage descriptor */
  int      group_index;    /* index into scr_groupdesc for failure group */
  char*    base;           /* base cache directory to use */
  char*    directory;      /* full directory base/dataset.id */
  int      copy_type;      /* redundancy scheme to apply */
  int      er_scheme;      /* encoding scheme id */
} scr_reddesc;

/*
=========================================
Redundancy descriptor functions
=========================================
*/

/* initialize the specified redundancy descriptor */
int scr_reddesc_init(
  scr_reddesc* c
);

/* free any memory associated with the specified redundancy
 * descriptor */
int scr_reddesc_free(
  scr_reddesc* c
);

/* given a checkpoint id and a list of redundancy descriptors,
 * select and return a pointer to a descriptor for the 
 * specified checkpoint id */
scr_reddesc* scr_reddesc_for_checkpoint(
  int id,
  int ndescs,
  scr_reddesc* descs
);

/* convert the specified redundancy descritpor into a corresponding
 * hash */
int scr_reddesc_store_to_hash(
  const scr_reddesc* c,
  kvtree* hash
);

/* build a redundancy descriptor corresponding to the specified hash,
 * this function is collective */
int scr_reddesc_create_from_hash(
  scr_reddesc* c,
  int index,
  const kvtree* hash
);

/* return pointer to store descriptor associated with redundancy
 * descriptor, returns NULL if reddesc or storedesc is not enabled */
scr_storedesc* scr_reddesc_get_store(
  const scr_reddesc* desc
);

/* apply redundancy scheme to files */
int scr_reddesc_apply(
  scr_filemap* map,
  const scr_reddesc* c,
  int id
);

/* rebuilds files for specified dataset id using specified redundancy descriptor,
 * adds them to filemap, and returns SCR_SUCCESS if all processes succeeded */
int scr_reddesc_recover(
  scr_cache_index* cindex,
  int id,
  const char* path
);

/* removes redundancy files added during scr_reddesc_apply */
int scr_reddesc_unapply(
  const scr_cache_index* cindex,
  int id,
  const char* path
);

/*
=========================================
Routines that operate on scr_reddescs array
=========================================
*/

/* create scr_reddescs array from scr_reddescs_hash */
int scr_reddescs_create(void);

/* free scr_reddescs array */
int scr_reddescs_free(void);

#endif
