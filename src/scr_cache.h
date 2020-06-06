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

#ifndef SCR_CACHE_H
#define SCR_CACHE_H

#include "scr_reddesc.h"
#include "scr_cache_index.h"
#include "scr_filemap.h"

/* returns name of the dataset directory for a given redundancy descriptor
 * and dataset id */
char* scr_cache_dir_get(const scr_reddesc* reddesc, int id);

/* returns name of hidden .scr subdirectory within the dataset directory
 * for a given redundancy descriptor and dataset id, caller must free
 * returned string */
char* scr_cache_dir_hidden_get(const scr_reddesc* reddesc, int id);

/* read file map for dataset from cache directory */
int scr_cache_get_map(const scr_cache_index* cindex, int id, scr_filemap* map);

/* write file map for dataset to cache directory */
int scr_cache_set_map(const scr_cache_index* cindex, int id, const scr_filemap* map);

/* delete file map file for dataset from cache directory */
int scr_cache_unset_map(const scr_cache_index* cindex, int id);

/* return string pointing to filemap file, caller must free string when done */
const char* scr_cache_get_map_file(const scr_cache_index* cindex, int id);

/* create a dataset directory given a redundancy descriptor and dataset id,
 * waits for all tasks on the same node before returning */
int scr_cache_dir_create(const scr_reddesc* reddesc, int id);

/* remove all files associated with specified dataset */
int scr_cache_delete(scr_cache_index* cindex, int id);

/* delete dataset with matching name from cache, if one exists */
int scr_cache_delete_by_name(scr_cache_index* cindex, const char* name);

/* each process passes in an ordered list of dataset ids along with a current
 * index, this function identifies the next smallest id across all processes
 * and returns this id in current, it also updates index on processes as
 * appropriate */
int scr_next_dataset(int ndsets, const int* dsets, int* index, int* current);

/* remove all files from cache */
int scr_cache_purge(scr_cache_index* cindex);

/* inspects that all listed files are readable and complete,
 * unlinks any that are not */
//int scr_cache_clean(scr_cache_index* cindex);

/* returns true iff each file in the cache can be read */
int scr_cache_check_files(const scr_cache_index* cindex, int id);

/* checks whether specifed file exists, is readable, and is complete */
int scr_bool_have_file(const scr_filemap* map, const char* file);

/* compute and store crc32 value for specified file in given dataset and rank,
 * check against current value if one is set */
int scr_compute_crc(scr_filemap* map, const char* file);

/* return store descriptor associated with dataset, returns NULL if not found */
scr_storedesc* scr_cache_get_storedesc(const scr_cache_index* cindex, int id);

#endif
