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

/* Implements an interface to read and write index files. */

#ifndef SCR_INDEX_API_H
#define SCR_INDEX_API_H

#include <stdio.h>
#include "spath.h"
#include "kvtree.h"
#include "scr_dataset.h"

/* read the index file from given directory and merge its contents into the given hash */
int scr_index_read(const spath* dir, kvtree* index);

/* overwrite the contents of the index file in given directory with given hash */
int scr_index_write(const spath* dir, kvtree* index);

/* read index file and return max dataset and checkpoint ids,
 * returns SCR_SUCCESS if file read successfully */
int scr_index_get_max_ids(const spath* dir, int* dset_id, int* ckpt_id, int* ckpt_dset_id);

/* remove given dataset name from hash */
int scr_index_remove(kvtree* index, const char* name);

/* set dataset name as current to restart from */
int scr_index_set_current(kvtree* index, const char* name);

/* get dataset name as current to restart from */
int scr_index_get_current(kvtree* index, char** name);

/* unset dataset name as current to restart from */
int scr_index_unset_current(kvtree* index);

/* write completeness code (0 or 1) for given dataset id and name in given hash */
int scr_index_set_dataset(kvtree* index, int id, const char* name, const scr_dataset* dataset, int complete);

/* write completeness code (0 or 1) for given dataset id and name in given hash */
int scr_index_set_complete(kvtree* index, int id, const char* name, int complete);

/* record fetch event for given dataset id and name in given hash */
int scr_index_mark_fetched(kvtree* index, int id, const char* name);

/* record failed fetch event for given dataset id and name in given hash */
int scr_index_mark_failed(kvtree* index, int id, const char* name);

/* clear failed fetch events for given dataset id and name in given hash */
int scr_index_clear_failed(kvtree* index, int id, const char* name);

/* record flush time for given dataset id and name in given hash */
int scr_index_mark_flushed(kvtree* index, int id, const char* name);

/* copy dataset into given dataset object,
 * returns SCR_FAILURE if key is not set */
int scr_index_get_dataset(kvtree* index, int id, const char* name, scr_dataset* dataset);

/* get completeness code for given dataset id and name in given hash,
 * sets complete=0 and returns SCR_FAILURE if key is not set */
int scr_index_get_complete(kvtree* index, int id, const char* name, int* complete);

/* lookup the dataset id corresponding to the given dataset name name in given hash
 * (assumes a name maps to a single dataset id) */
int scr_index_get_id_by_name(const kvtree* index, const char* name, int* id);

/* lookup the most recent complete dataset id and name whose id is less than earlier_than
 * setting earlier_than = -1 disables this filter */
int scr_index_get_most_recent_complete(const kvtree* index, int earlier_than, int* id, char* name);

/* lookup the dataset having the lowest id, return its id and name,
 * sets id to -1 to indicate no dataset is left */
int scr_index_get_oldest(const kvtree* index, int* id, char* name);

/* remove checkpoints from index that are later than given dataset id */
int scr_index_remove_later(kvtree* index, int id);

#endif
