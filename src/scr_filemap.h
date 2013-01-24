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

#ifndef SCR_FILEMAP_H
#define SCR_FILEMAP_H

#include "scr.h"
#include "scr_hash.h"
#include "scr_meta.h"
#include "scr_dataset.h"

/* a special type of hash */
typedef scr_hash scr_filemap;

/*
=========================================
Filemap set/get/unset data functions
=========================================
*/

/* adds a new filename to the filemap and associates it with a specified dataset id and a rank */
int scr_filemap_add_file(scr_filemap* map, int dset, int rank, const char* file);

/* removes a filename for a given dataset id and rank from the filemap */
int scr_filemap_remove_file(scr_filemap* map, int dset, int rank, const char* file);

/* sets the redundancy descriptor hash for the given rank and dataset id */
int scr_filemap_set_desc(scr_filemap* map, int dset, int rank, scr_hash* hash);

/* copies the redundancy descriptor hash for the given rank and dataset id into hash */
int scr_filemap_get_desc(const scr_filemap* map, int dset, int rank, scr_hash* hash);

/* unset the redundancy descriptor hash for the given rank and dataset id */
int scr_filemap_unset_desc(scr_filemap* map, int dset, int rank);

/* sets the flush/scavenge descriptor hash for the given rank and dataset id */
int scr_filemap_set_flushdesc(scr_filemap* map, int dset, int rank, scr_hash* hash);

/* copies the flush/scavenge descriptor hash for the given rank and dataset id into hash */
int scr_filemap_get_flushdesc(const scr_filemap* map, int dset, int rank, scr_hash* hash);

/* unset the flush/scavenge descriptor hash for the given rank and dataset id */
int scr_filemap_unset_flushdesc(scr_filemap* map, int dset, int rank);

/* sets the dataset hash for the given rank and dataset id */
int scr_filemap_set_dataset(scr_filemap* map, int dset, int rank, scr_hash* hash);

/* copies the dataset hash for the given rank and dataset id into hash */
int scr_filemap_get_dataset(const scr_filemap* map, int dset, int rank, scr_hash* hash);

/* unset the dataset hash for the given rank and dataset id */
int scr_filemap_unset_dataset(scr_filemap* map, int dset, int rank);

/* set number of files to expect for a given rank in a given dataset id */
int scr_filemap_set_expected_files(scr_filemap* map, int dset, int rank, int expect);

/* get number of files to expect for a given rank in a given dataset id */
int scr_filemap_get_expected_files(const scr_filemap* map, int dset, int rank);

/* unset number of files to expect for a given rank in a given dataset id */
int scr_filemap_unset_expected_files(scr_filemap* map, int dset, int rank);

/* sets metadata for file */
int scr_filemap_set_meta(scr_filemap* map, int dset, int rank, const char* file, const scr_meta* meta);

/* gets metadata for file */
int scr_filemap_get_meta(const scr_filemap* map, int dset, int rank, const char* file, scr_meta* meta);

/* unsets metadata for file */
int scr_filemap_unset_meta(scr_filemap* map, int dset, int rank, const char* file);

/*
=========================================
Filemap clear and copy functions
=========================================
*/

/* remove all associations for a given rank in a given dataset */
int scr_filemap_remove_rank_by_dataset(scr_filemap* map, int dset, int rank);

/* remove all associations for a given rank */
int scr_filemap_remove_rank(scr_filemap* map, int rank);

/* remove all associations for a given dataset */
int scr_filemap_remove_dataset(scr_filemap* map, int dset);

/* clear the filemap completely */
int scr_filemap_clear(scr_filemap* map);

/* adds all files from map2 to map1 and updates num_expected_files to total file count */
int scr_filemap_merge(scr_filemap* map1, scr_filemap* map2);

/* extract specified rank from given filemap and return as a new filemap */
scr_filemap* scr_filemap_extract_rank(scr_filemap* map, int rank);

/*
=========================================
Filemap list functions
=========================================
*/

/* given a filemap, return a list of ranks */
/* TODO: must free ranks list when done with it */
int scr_filemap_list_ranks(const scr_filemap* map, int* nranks, int** ranks);

/* given a filemap, return a list of datasets */
/* TODO: must free datasets list when done with it */
int scr_filemap_list_datasets(const scr_filemap* map, int* ndsets, int** dsets);

/* given a filemap and a dataset, return a list of ranks */
/* TODO: must free ranks list when done with it */
int scr_filemap_list_ranks_by_dataset(const scr_filemap* map, int id, int* n, int** v);

/* given a filemap, a dataset id, and a rank, return the number of files and a list of the filenames */
/* TODO: must free files list when done with it */
int scr_filemap_list_files(const scr_filemap* map, int dset, int rank, int* numfiles, char*** files);

/*
=========================================
Filemap iterator functions
=========================================
*/

/* given a filemap, return a hash elem pointer to the first rank */
scr_hash_elem* scr_filemap_first_rank(const scr_filemap* map);

/* given a filemap, return a hash elem pointer to the first rank for a given dataset */
scr_hash_elem* scr_filemap_first_rank_by_dataset(const scr_filemap* map, int dset);

/* given a filemap, return a hash elem pointer to the first dataset */
scr_hash_elem* scr_filemap_first_dataset(const scr_filemap* map);

/* given a filemap, return a hash elem pointer to the first dataset for a given rank */
scr_hash_elem* scr_filemap_first_dataset_by_rank(const scr_filemap* map, int rank);

/* given a filemap, a dataset id, and a rank, return a hash elem pointer to the first file */
scr_hash_elem* scr_filemap_first_file(const scr_filemap* map, int dset, int rank);

/*
=========================================
Filemap query count functions
=========================================
*/

/* returns true if have a hash for specified rank */
int scr_filemap_have_rank(const scr_filemap* map, int rank);

/* returns true if have a hash for specified rank of a given dataset */
int scr_filemap_have_rank_by_dataset(const scr_filemap* map, int dset, int rank);

/* returns the latest dataset id (largest int) in given map */
int scr_filemap_latest_dataset(const scr_filemap* map);

/* returns the oldest dataset id (smallest int) in given map */
int scr_filemap_oldest_dataset(const scr_filemap* map, int higher_than);

/* return the number of ranks in the hash */
int scr_filemap_num_ranks(const scr_filemap* map);

/* return the number of ranks in the hash for a given dataset id */
int scr_filemap_num_ranks_by_dataset(const scr_filemap* map, int dset);

/* return the number of datasets in the hash */
int scr_filemap_num_datasets(const scr_filemap* map);

/* return the number of files in the hash for a given dataset id and rank */
int scr_filemap_num_files(const scr_filemap* map, int dset, int rank);

/*
=========================================
Filemap read/write/free functions
=========================================
*/

/* reads specified file and fills in filemap structure */
int scr_filemap_read(const scr_path* file, scr_filemap* map);

/* writes given filemap to specified file */
int scr_filemap_write(const scr_path* file, const scr_filemap* map);

/* create a new filemap structure */
scr_filemap* scr_filemap_new();

/* free memory resources assocaited with filemap */
int scr_filemap_delete(scr_filemap** ptr_map);

#endif
