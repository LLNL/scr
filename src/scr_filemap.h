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

/* records the name of the filemap and a pointer to a kvl list */
struct scr_filemap
{
  struct scr_hash* hash;
};

/*
=========================================
Filemap add/remove data functions
=========================================
*/

/* adds a new filename to the filemap and associates it with a specified checkpoint id and a rank */
int scr_filemap_add_file(struct scr_filemap* map, int checkpointid, int rank, const char* file);

/* removes a filename for a given checkpoint id and rank from the filemap */
int scr_filemap_remove_file(struct scr_filemap* map, int checkpointid, int rank, const char* file);

/* set number of files to expect for a given rank in a given checkpoint id */
int scr_filemap_set_expected_files(struct scr_filemap* map, int ckpt, int rank, int expect);

/* unset number of files to expect for a given rank in a given checkpoint id */
int scr_filemap_unset_expected_files(struct scr_filemap* map, int ckpt, int rank);

/* sets a tag/value pair on a given file */
int scr_filemap_set_tag(struct scr_filemap* map, int ckpt, int rank, const char* file, const char* tag, const char* value);

/* unsets a tag on a given file */
int scr_filemap_unset_tag(struct scr_filemap* map, int ckpt, int rank, const char* file, const char* tag);

/* copies file data (including tags) from one filemap to another */
int scr_filemap_copy_file(struct scr_filemap* map, struct scr_filemap* src_map, int ckpt, int rank, const char* file);

/* remove all associations for a given rank in a given checkpoint */
int scr_filemap_remove_rank_by_checkpoint(struct scr_filemap* map, int checkpoint_id, int rank);

/* remove all associations for a given rank */
int scr_filemap_remove_rank(struct scr_filemap* map, int rank);

/* remove all associations for a given checkpoint */
int scr_filemap_remove_checkpoint(struct scr_filemap* map, int ckpt);

/*
=========================================
Filemap list functions
=========================================
*/

/* given a filemap, return a list of ranks */
/* TODO: must free ranks list when done with it */
int scr_filemap_list_ranks(struct scr_filemap* map, int* nranks, int** ranks);

/* given a filemap, return a list of checkpoints */
/* TODO: must free checkpoints list when done with it */
int scr_filemap_list_checkpoints(struct scr_filemap* map, int* ncheckpoints, int** checkpoints);

/* given a filemap and a checkpoint, return a list of ranks */
/* TODO: must free ranks list when done with it */
int scr_filemap_list_ranks_by_checkpoint(struct scr_filemap* map, int ckpt, int* n, int** v);

/* given a filemap and a rank, return a list of checkpoints */
/* TODO: must free checkpoints list when done with it */
int scr_filemap_list_checkpoints_by_rank(struct scr_filemap* map, int rank, int* n, int** v);

/* given a filemap, a checkpoint id, and a rank, return the number of files and a list of the filenames */
/* TODO: must free files list when done with it */
int scr_filemap_list_files(struct scr_filemap* map, int checkpointid, int rank, int* numfiles, char*** files);

/*
=========================================
Filemap iterator functions
=========================================
*/

/* given a filemap, return a hash elem pointer to the first rank */
struct scr_hash_elem* scr_filemap_first_rank(struct scr_filemap* map);

/* given a filemap, return a hash elem pointer to the first rank for a given checkpoint */
struct scr_hash_elem* scr_filemap_first_rank_by_checkpoint(struct scr_filemap* map, int checkpoint);

/* given a filemap, return a hash elem pointer to the first checkpoint */
struct scr_hash_elem* scr_filemap_first_checkpoint(struct scr_filemap* map);

/* given a filemap, return a hash elem pointer to the first checkpoint for a given rank */
struct scr_hash_elem* scr_filemap_first_checkpoint_by_rank(struct scr_filemap* map, int rank);

/* given a filemap, a checkpoint id, and a rank, return a hash elem pointer to the first file */
struct scr_hash_elem* scr_filemap_first_file(struct scr_filemap* map, int checkpoint, int rank);

/*
=========================================
Filemap query count functions
=========================================
*/

/* returns true if have a hash for specified rank */
int scr_filemap_have_rank(struct scr_filemap* map, int rank);

/* returns true if have a hash for specified rank of a given checkpoint */
int scr_filemap_have_rank_by_checkpoint(struct scr_filemap* map, int checkpointid, int rank);

/* returns the latest checkpoint id (largest int) in given map */
int scr_filemap_latest_checkpoint(struct scr_filemap* map);

/* returns the oldest checkpoint id (smallest int) in given map */
int scr_filemap_oldest_checkpoint(struct scr_filemap* map);

/* return the number of ranks in the hash */
int scr_filemap_num_ranks(struct scr_filemap* map);

/* return the number of ranks in the hash for a given checkpoint id */
int scr_filemap_num_ranks_by_checkpoint(struct scr_filemap* map, int checkpoint);

/* return the number of checkpoints in the hash */
int scr_filemap_num_checkpoints(struct scr_filemap* map);

/* return the number of files in the hash for a given checkpoint id and rank */
int scr_filemap_num_files(struct scr_filemap* map, int checkpoint, int rank);

/* return the number of expected files in the hash for a given checkpoint id and rank */
int scr_filemap_num_expected_files(struct scr_filemap* map, int ckpt, int rank);

/*
=========================================
Filemap read/write/free functions
=========================================
*/

/* adds all files from map2 to map1 and updates num_expected_files to total file count */
int scr_filemap_merge(struct scr_filemap* map1, struct scr_filemap* map2);

/* extract specified rank from given filemap and return as a new filemap */
struct scr_filemap* scr_filemap_extract_rank(struct scr_filemap* map, int rank);

/* reads specified file and fills in filemap structure */
int scr_filemap_read(const char* file, struct scr_filemap* map);

/* writes given filemap to specified file */
int scr_filemap_write(const char* file, struct scr_filemap* map);

/* create a new filemap structure */
struct scr_filemap* scr_filemap_new();

/* free memory resources assocaited with filemap */
int scr_filemap_delete(struct scr_filemap* map);

#endif
