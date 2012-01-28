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
#include "scr_filemap.h"

/* searches through the cache descriptors and returns the size of the cache whose BASE
 * matches the specified base */
int scr_cachedesc_size(const char* target);

/* returns name of the cache directory for a given redundancy descriptor and dataset id */
int scr_cache_dir_get(const struct scr_reddesc* c, int id, char* dir);

/* create a cache directory given a redundancy descriptor and dataset id,
 * waits for all tasks on the same node before returning */
int scr_cache_dir_create(const struct scr_reddesc* c, int id);

/* remove all files associated with specified dataset */
int scr_cache_delete(scr_filemap* map, int id);

/* each process passes in an ordered list of dataset ids along with a current index,
 * this function identifies the next smallest id across all processes and returns
 * this id in current, it also updates index on processes as appropriate */
int scr_next_dataset(int ndsets, const int* dsets, int* index, int* current);

/* given a filemap, a dataset, and a rank, unlink those files and remove them from the map */
int scr_unlink_rank(scr_filemap* map, int id, int rank);

/* remove all files recorded in filemap and the filemap itself */
int scr_cache_purge(scr_filemap* map);

/* opens the filemap, inspects that all listed files are readable and complete,
 * unlinks any that are not */
int scr_cache_clean(scr_filemap* map);

/* returns true iff each file in the filemap can be read */
int scr_cache_check_files(const scr_filemap* map, int id);

#endif
