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

/* a special type of hash */
typedef struct scr_hash scr_filemap;

/*
=========================================
Filemap add/remove data functions
=========================================
*/

/* adds a new filename to the filemap and associates it with a specified checkpoint id and a rank */
int scr_filemap_add_file(scr_filemap* map, int checkpointid, int rank, const char* file);

/* removes a filename for a given checkpoint id and rank from the filemap */
int scr_filemap_remove_file(scr_filemap* map, int checkpointid, int rank, const char* file);

/* sets the checkpoint descriptor hash for the given rank and checkpoint id */
int scr_filemap_set_desc(scr_filemap* map, int ckpt, int rank, struct scr_hash* hash);

/* copies the checkpoint descriptor hash for the given rank and checkpoint id into hash */
int scr_filemap_get_desc(scr_filemap* map, int ckpt, int rank, struct scr_hash* hash);

/* unset the checkpoint descriptor hash for the given rank and checkpoint id */
int scr_filemap_unset_desc(scr_filemap* map, int ckpt, int rank);

/* set number of files to expect for a given rank in a given checkpoint id */
int scr_filemap_set_expected_files(scr_filemap* map, int ckpt, int rank, int expect);

/* unset number of files to expect for a given rank in a given checkpoint id */
int scr_filemap_unset_expected_files(scr_filemap* map, int ckpt, int rank);

/* sets a tag/value pair */
int scr_filemap_set_tag(scr_filemap* map, int ckpt, int rank, const char* tag, const char* value);

/* gets the value for a given tag, returns NULL if not found */
char* scr_filemap_get_tag(scr_filemap* map, int ckpt, int rank, const char* tag);

/* unsets a tag */
int scr_filemap_unset_tag(scr_filemap* map, int ckpt, int rank, const char* tag);

/* copies file data (including tags) from one filemap to another */
int scr_filemap_copy_file(scr_filemap* map, scr_filemap* src_map, int ckpt, int rank, const char* file);

/* remove all associations for a given rank in a given checkpoint */
int scr_filemap_remove_rank_by_checkpoint(scr_filemap* map, int checkpoint_id, int rank);

/* remove all associations for a given rank */
int scr_filemap_remove_rank(scr_filemap* map, int rank);

/* remove all associations for a given checkpoint */
int scr_filemap_remove_checkpoint(scr_filemap* map, int ckpt);

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
int scr_filemap_list_ranks(scr_filemap* map, int* nranks, int** ranks);

/* given a filemap, return a list of checkpoints */
/* TODO: must free checkpoints list when done with it */
int scr_filemap_list_checkpoints(scr_filemap* map, int* ncheckpoints, int** checkpoints);

/* given a filemap and a checkpoint, return a list of ranks */
/* TODO: must free ranks list when done with it */
int scr_filemap_list_ranks_by_checkpoint(scr_filemap* map, int ckpt, int* n, int** v);

/* given a filemap and a rank, return a list of checkpoints */
/* TODO: must free checkpoints list when done with it */
int scr_filemap_list_checkpoints_by_rank(scr_filemap* map, int rank, int* n, int** v);

/* given a filemap, a checkpoint id, and a rank, return the number of files and a list of the filenames */
/* TODO: must free files list when done with it */
int scr_filemap_list_files(scr_filemap* map, int checkpointid, int rank, int* numfiles, char*** files);

/*
=========================================
Filemap iterator functions
=========================================
*/

/* given a filemap, return a hash elem pointer to the first rank */
struct scr_hash_elem* scr_filemap_first_rank(scr_filemap* map);

/* given a filemap, return a hash elem pointer to the first rank for a given checkpoint */
struct scr_hash_elem* scr_filemap_first_rank_by_checkpoint(scr_filemap* map, int checkpoint);

/* given a filemap, return a hash elem pointer to the first checkpoint */
struct scr_hash_elem* scr_filemap_first_checkpoint(scr_filemap* map);

/* given a filemap, return a hash elem pointer to the first checkpoint for a given rank */
struct scr_hash_elem* scr_filemap_first_checkpoint_by_rank(scr_filemap* map, int rank);

/* given a filemap, a checkpoint id, and a rank, return a hash elem pointer to the first file */
struct scr_hash_elem* scr_filemap_first_file(scr_filemap* map, int checkpoint, int rank);

/*
=========================================
Filemap query count functions
=========================================
*/

/* returns true if have a hash for specified rank */
int scr_filemap_have_rank(scr_filemap* map, int rank);

/* returns true if have a hash for specified rank of a given checkpoint */
int scr_filemap_have_rank_by_checkpoint(scr_filemap* map, int checkpointid, int rank);

/* returns the latest checkpoint id (largest int) in given map */
int scr_filemap_latest_checkpoint(scr_filemap* map);

/* returns the oldest checkpoint id (smallest int) in given map */
int scr_filemap_oldest_checkpoint(scr_filemap* map, int higher_than);

/* return the number of ranks in the hash */
int scr_filemap_num_ranks(scr_filemap* map);

/* return the number of ranks in the hash for a given checkpoint id */
int scr_filemap_num_ranks_by_checkpoint(scr_filemap* map, int checkpoint);

/* return the number of checkpoints in the hash */
int scr_filemap_num_checkpoints(scr_filemap* map);

/* return the number of files in the hash for a given checkpoint id and rank */
int scr_filemap_num_files(scr_filemap* map, int checkpoint, int rank);

/* return the number of expected files in the hash for a given checkpoint id and rank */
int scr_filemap_num_expected_files(scr_filemap* map, int ckpt, int rank);

/*
=========================================
Filemap read/write/free functions
=========================================
*/

/* reads specified file and fills in filemap structure */
int scr_filemap_read(const char* file, scr_filemap* map);

/* writes given filemap to specified file */
int scr_filemap_write(const char* file, scr_filemap* map);

/* create a new filemap structure */
scr_filemap* scr_filemap_new();

/* free memory resources assocaited with filemap */
int scr_filemap_delete(scr_filemap* map);

#endif
