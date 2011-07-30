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

#ifndef SCR_HALT_H
#define SCR_HALT_H

#include <stdio.h>
#include "scr_hash.h"
#include "scr_dataset.h"

/* read the index file from given directory and merge its contents into the given hash */
int scr_index_read(const char* dir, scr_hash* index);

/* overwrite the contents of the index file in given directory with given hash */
int scr_index_write(const char* dir, scr_hash* index);

/* add given dataset id and directory name to given hash */
int scr_index_add_dir(scr_hash* index, int id, const char* name);

/* write completeness code (0 or 1) for given dataset id and directory in given hash */
int scr_index_set_dataset(scr_hash* index, const scr_dataset* dataset, int complete);

/* write completeness code (0 or 1) for given dataset id and directory in given hash */
int scr_index_set_complete(scr_hash* index, int id, const char* name, int complete);

/* record fetch event for given dataset id and directory in given hash */
int scr_index_mark_fetched(scr_hash* index, int id, const char* name);

/* record failed fetch event for given dataset id and directory in given hash */
int scr_index_mark_failed(scr_hash* index, int id, const char* name);

/* record flush time for given dataset id and directory in given hash */
int scr_index_mark_flushed(scr_hash* index, int id, const char* name);

/* get completeness code for given dataset id and directory in given hash,
 * sets complete=0 and returns SCR_FAILURE if key is not set */
int scr_index_get_complete(scr_hash* index, int id, const char* name, int* complete);

/* lookup the dataset id corresponding to the given dataset directory name in given hash
 * (assumes a directory maps to a single dataset id) */
int scr_index_get_id_by_dir(const scr_hash* index, const char* name, int* id);

/* lookup the most recent complete dataset id and directory whose id is less than earlier_than
 * setting earlier_than = -1 disables this filter */
int scr_index_get_most_recent_complete(const scr_hash* index, int earlier_than, int* id, char* name);

#endif
