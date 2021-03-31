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

#ifndef SCR_CACHE_INDEX_H
#define SCR_CACHE_INDEX_H

#include "scr.h"
#include "scr_meta.h"
#include "scr_dataset.h"

#include "spath.h"
#include "kvtree.h"

/* a special type of hash */
typedef kvtree scr_cache_index;

/*
=========================================
Cache index set/get/unset data functions
=========================================
*/

/* set the CURRENT name, used to rememeber if we already proccessed
 * a SCR_CURRENT name a user may have provided to set the current value,
 * we ignore that request in later runs and use this marker to remember */
int scr_cache_index_set_current(const kvtree* h, const char* current);

/* returns the CURRENT name */
int scr_cache_index_get_current(const kvtree* h, char** current);

/* sets the dataset hash for the given dataset id */
int scr_cache_index_set_dataset(scr_cache_index* cindex, int dset, kvtree* hash);

/* copies the dataset hash for the given dataset id into hash */
int scr_cache_index_get_dataset(const scr_cache_index* cindex, int dset, kvtree* hash);

/* unset the dataset hash for the given dataset id */
int scr_cache_index_unset_dataset(scr_cache_index* cindex, int dset);

/* record directory where dataset is stored */
int scr_cache_index_set_dir(scr_cache_index* cindex, int dset, const char* path);

/* returns pointer to directory for dataset */
int scr_cache_index_get_dir(const scr_cache_index* cindex, int dset, char** path);

/* unset the directory for the given dataset id */
int scr_cache_index_unset_dir(scr_cache_index* cindex, int dset);

/* mark dataset as cache bypass (read/write direct to prefix dir) */
int scr_cache_index_set_bypass(scr_cache_index* cindex, int dset, int bypass);

/* get value of bypass flag for dataset */
int scr_cache_index_get_bypass(const scr_cache_index* cindex, int dset, int* bypass);

/*
=========================================
Cache index clear and copy functions
=========================================
*/

/* remove all associations for a given dataset */
int scr_cache_index_remove_dataset(scr_cache_index* cindex, int dset);

/* clear the cache index completely */
int scr_cache_index_clear(scr_cache_index* cindex);

/* merges cindex2 into cindex1 */
int scr_cache_index_merge(scr_cache_index* cindex1, scr_cache_index* cindex2);

/*
=========================================
Cache index list functions
=========================================
*/

/* given a cache index, return a list of datasets */
/* TODO: must free datasets list when done with it */
int scr_cache_index_list_datasets(const scr_cache_index* cindex, int* ndsets, int** dsets);

/*
=========================================
Cache index iterator functions
=========================================
*/

/* given a cache index, return a hash elem pointer to the first dataset */
kvtree_elem* scr_cache_index_first_dataset(const scr_cache_index* cindenx);

/*
=========================================
Cache index query count functions
=========================================
*/

/* returns the latest dataset id (largest int) in given index */
int scr_cache_index_latest_dataset(const scr_cache_index* cindex);

/* return the number of datasets in the index */
int scr_cache_index_num_datasets(const scr_cache_index* cindex);

/*
=========================================
Cache index read/write/free functions
=========================================
*/

/* reads specified file and fills in cache index structure */
int scr_cache_index_read(const spath* file, scr_cache_index* cindex);

/* writes given cache index to specified file */
int scr_cache_index_write(const spath* file, const scr_cache_index* cindex);

/* create a new cache index structure */
scr_cache_index* scr_cache_index_new(void);

/* free memory resources assocaited with cache index */
int scr_cache_index_delete(scr_cache_index** ptr_cindex);

#endif
