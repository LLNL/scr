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

/* Defines a data structure that keeps track of the number
 * and the names of the files a process writes out in a given
 * checkpoint. */

/*
GOALS:
  - support different number of processes per node on
    a restart
  - support multiple files per rank per checkpoint
  - support multiple checkpoints at different cache levels

READ:
  master process on each node reads filemap
  and distributes pieces to others

WRITE:
  all processes send their file info to master
  and master writes it out

  master filemap file
    list of ranks this node has files for
      for each rank, list of checkpoint ids
        for each checkpoint id, list of locations (RAM, SSD, PFS, etc)
            for each location, list of files for this rank for this checkpoint
*/

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>

/* need at least version 8.5 of queue.h from Berkeley */
/*#include <sys/queue.h>*/
#include "queue.h"

#include "scr.h"
#include "scr_io.h"
#include "scr_err.h"
#include "scr_hash.h"
#include "scr_filemap.h"

#define SCR_FILEMAP_KEY_RANK   ("RANK")
#define SCR_FILEMAP_KEY_CKPT   ("CKPT")
#define SCR_FILEMAP_KEY_FILE   ("FILE")
#define SCR_FILEMAP_KEY_DESC   ("CKPTDESC")
#define SCR_FILEMAP_KEY_EXPECT ("EXPECT")

/* returns the RANK hash */
struct scr_hash* scr_filemap_rh(struct scr_hash* h)
{
  struct scr_hash* rh = scr_hash_get(h, SCR_FILEMAP_KEY_RANK);
  return rh;
}

/* returns the CKPT hash */
struct scr_hash* scr_filemap_ch(struct scr_hash* h)
{
  struct scr_hash* ch = scr_hash_get(h, SCR_FILEMAP_KEY_CKPT);
  return ch;
}

/* returns the hash associated with a particular rank */
struct scr_hash* scr_filemap_r(struct scr_hash* h, int rank)
{
  struct scr_hash* r = scr_hash_get_kv_int(h, SCR_FILEMAP_KEY_RANK, rank);
  return r;
}

/* returns the hash associated with a particular checkpoint */
struct scr_hash* scr_filemap_c(struct scr_hash* h, int ckpt)
{
  struct scr_hash* c = scr_hash_get_kv_int(h, SCR_FILEMAP_KEY_CKPT, ckpt);
  return c;
}

/* returns the hash associated with a particular rank and checkpoint pair */
struct scr_hash* scr_filemap_rc(struct scr_hash* h, int ckpt, int rank)
{
  struct scr_hash* r  = scr_filemap_r(h, rank);
  struct scr_hash* rc = scr_filemap_c(r, ckpt);
  return rc;
}

/* returns the FILE hash associated with a particular rank and checkpoint pair */
struct scr_hash* scr_filemap_fh(struct scr_hash* hash, int ckpt, int rank)
{
  struct scr_hash* rc = scr_filemap_rc(hash, ckpt, rank);
  struct scr_hash* fh = scr_hash_get(rc, SCR_FILEMAP_KEY_FILE);
  return fh;
}

/* returns the hash associated with a particular rank, checkpoint, and file tuple */
struct scr_hash* scr_filemap_rcf(struct scr_hash* hash, int ckpt, int rank, const char* file)
{
  struct scr_hash* fh  = scr_filemap_fh(hash, ckpt, rank);
  struct scr_hash* rcf = scr_hash_get(fh, file);
  return rcf;
}

/* adds a new filename to the filemap and associates it with a specified checkpoint id and a rank */
int scr_filemap_add_file(scr_filemap* map, int ckpt, int rank, const char* file)
{
  /* set RANK/CKPT index and get hash references */
  struct scr_hash* r  = scr_hash_set_kv_int(map, SCR_FILEMAP_KEY_RANK, rank);
  struct scr_hash* rc = scr_hash_set_kv_int(r,   SCR_FILEMAP_KEY_CKPT, ckpt);

  /* add file to RANK/CKPT/FILE hash */
  scr_hash_set_kv(rc, SCR_FILEMAP_KEY_FILE, file);

  /* set CKPT/RANK index */
  struct scr_hash* c  = scr_hash_set_kv_int(map, SCR_FILEMAP_KEY_CKPT, ckpt);
  struct scr_hash* cr = scr_hash_set_kv_int(c,   SCR_FILEMAP_KEY_RANK, rank);

  return SCR_SUCCESS;
}

/* unset RANK/CKPT and CKPT/RANK indicies if the map for this rank and checkpoint is empty */
static int scr_filemap_unset_if_empty(scr_filemap* map, int ckpt, int rank)
{
  /* get hash references for this rank and checkpoint pair */
  struct scr_hash* r  = scr_filemap_r(map, rank);
  struct scr_hash* c  = scr_filemap_c(map, ckpt);

  /* see if we have anything left in the map for this rank and checkpoint */
  struct scr_hash* rc = scr_filemap_c(r,   ckpt);
  if (scr_hash_size(rc) == 0) {
    /* unset the checkpoint under the rank / checkpoint index */
    scr_hash_unset_kv_int(r, SCR_FILEMAP_KEY_CKPT, ckpt);

    /* and unset the rank under the checkpoint / rank index */
    scr_hash_unset_kv_int(c, SCR_FILEMAP_KEY_RANK, rank);
  }

  /* if there is nothing left under this rank, unset the rank */
  if (scr_hash_size(r) == 0) {
    scr_hash_unset_kv_int(map, SCR_FILEMAP_KEY_RANK, rank);
  }

  /* if there is nothing left under this checkpoint, unset the checkpoint */
  if (scr_hash_size(c) == 0) {
    scr_hash_unset_kv_int(map, SCR_FILEMAP_KEY_CKPT, ckpt);
  }

  return SCR_SUCCESS;
}

/* removes a filename for a given checkpoint id and rank from the filemap */
int scr_filemap_remove_file(scr_filemap* map, int ckpt, int rank, const char* file)
{
  /* remove file from RANK/CKPT/FILE hash */
  struct scr_hash* rc = scr_filemap_rc(map, ckpt, rank);
  scr_hash_unset_kv(rc, SCR_FILEMAP_KEY_FILE, file);

  /* unset RANK/CKPT and CKPT/RANK indicies if the hash is empty */
  scr_filemap_unset_if_empty(map, ckpt, rank);

  return SCR_SUCCESS;
}

/* sets the checkpoint descriptor hash for the given rank and checkpoint id */
int scr_filemap_set_desc(scr_filemap* map, int ckpt, int rank, struct scr_hash* hash)
{
  /* set the RANK/CKPT index and get hash references */
  struct scr_hash* r  = scr_hash_set_kv_int(map, SCR_FILEMAP_KEY_RANK, rank);
  struct scr_hash* rc = scr_hash_set_kv_int(r,   SCR_FILEMAP_KEY_CKPT, ckpt);

  /* set the EXPECT value under the RANK/CKPT hahs */
  scr_hash_unset(rc, SCR_FILEMAP_KEY_DESC);
  struct scr_hash* desc = scr_hash_new();
  scr_hash_merge(desc, hash);
  scr_hash_set(rc, SCR_FILEMAP_KEY_DESC, desc);

  /* set CKPT/RANK index */
  struct scr_hash* c  = scr_hash_set_kv_int(map, SCR_FILEMAP_KEY_CKPT, ckpt);
  struct scr_hash* cr = scr_hash_set_kv_int(c,   SCR_FILEMAP_KEY_RANK, rank);

  return SCR_SUCCESS;
}

/* copies the checkpoint descriptor hash for the given rank and checkpoint id into hash */
int scr_filemap_get_desc(scr_filemap* map, int ckpt, int rank, struct scr_hash* hash)
{
  /* get RANK/CKPT hash */
  struct scr_hash* rc = scr_filemap_rc(map, ckpt, rank);
  struct scr_hash* desc = scr_hash_get(rc, SCR_FILEMAP_KEY_DESC);
  if (desc != NULL) {
    scr_hash_merge(hash, desc);
    return SCR_SUCCESS;
  }

  return SCR_FAILURE; 
}

/* unset the checkpoint descriptor hash for the given rank and checkpoint id */
int scr_filemap_unset_desc(scr_filemap* map, int ckpt, int rank)
{
  /* unset CKPTDESC value */
  struct scr_hash* rc = scr_filemap_rc(map, ckpt, rank);
  scr_hash_unset(rc, SCR_FILEMAP_KEY_DESC);

  /* unset RANK/CKPT and CKPT/RANK indicies if the hash is empty */
  scr_filemap_unset_if_empty(map, ckpt, rank);

  return SCR_SUCCESS;
}

/* set number of files to expect for a given rank in a given checkpoint id */
int scr_filemap_set_expected_files(scr_filemap* map, int ckpt, int rank, int expect)
{
  /* set the RANK/CKPT index and get hash references */
  struct scr_hash* r  = scr_hash_set_kv_int(map, SCR_FILEMAP_KEY_RANK, rank);
  struct scr_hash* rc = scr_hash_set_kv_int(r,   SCR_FILEMAP_KEY_CKPT, ckpt);

  /* set the EXPECT value under the RANK/CKPT hahs */
  scr_hash_unset(rc, SCR_FILEMAP_KEY_EXPECT);
  scr_hash_set_kv_int(rc, SCR_FILEMAP_KEY_EXPECT, expect);

  /* set CKPT/RANK index */
  struct scr_hash* c  = scr_hash_set_kv_int(map, SCR_FILEMAP_KEY_CKPT, ckpt);
  struct scr_hash* cr = scr_hash_set_kv_int(c,   SCR_FILEMAP_KEY_RANK, rank);

  return SCR_SUCCESS;
}

/* unset number of files to expect for a given rank in a given checkpoint id */
int scr_filemap_unset_expected_files(scr_filemap* map, int ckpt, int rank)
{
  /* unset EXPECT value */
  struct scr_hash* rc = scr_filemap_rc(map, ckpt, rank);
  scr_hash_unset(rc, SCR_FILEMAP_KEY_EXPECT);

  /* unset RANK/CKPT and CKPT/RANK indicies if the hash is empty */
  scr_filemap_unset_if_empty(map, ckpt, rank);

  return SCR_SUCCESS;
}

/* sets a tag/value pair */
int scr_filemap_set_tag(scr_filemap* map, int ckpt, int rank, const char* tag, const char* value)
{
  /* define tag in Rank/CheckpointID/File hash */
  struct scr_hash* rc = scr_filemap_rc(map, ckpt, rank);
  scr_hash_unset(rc, tag);
  scr_hash_set_kv(rc, tag, value);

  return SCR_SUCCESS;
}

/* gets the value for a given tag, returns NULL if not found */
char* scr_filemap_get_tag(scr_filemap* map, int ckpt, int rank, const char* tag)
{
  /* define tag in Rank/CheckpointID/File hash */
  struct scr_hash* rc = scr_filemap_rc(map, ckpt, rank);
  char* value = scr_hash_elem_get_first_val(rc, tag);
  return value;
}

/* unsets a tag */
int scr_filemap_unset_tag(scr_filemap* map, int ckpt, int rank, const char* tag)
{
  /* define tag in Rank/CheckpointID/File hash */
  struct scr_hash* rc = scr_filemap_rc(map, ckpt, rank);
  scr_hash_unset(rc, tag);

  /* unset RANK/CKPT and CKPT/RANK indicies if the hash is empty */
  scr_filemap_unset_if_empty(map, ckpt, rank);

  return SCR_SUCCESS;
}

/* copies file data (including tags) from one filemap to another */
int scr_filemap_copy_file(scr_filemap* map, scr_filemap* src_map, int ckpt, int rank, const char* file)
{
  /* first add the file to the map */
  scr_filemap_add_file(map, ckpt, rank, file);

  return SCR_SUCCESS;
}

/* remove all associations for a given rank in a given checkpoint */
int scr_filemap_remove_rank_by_checkpoint(scr_filemap* map, int ckpt, int rank)
{
  /* remove checkpoint from the RANK/CKPT index, and remove RANK if that was the last item */
  struct scr_hash* r = scr_filemap_r(map, rank);
  scr_hash_unset_kv_int(r, SCR_FILEMAP_KEY_CKPT, ckpt);
  if (scr_hash_size(r) == 0) {
    scr_hash_unset_kv_int(map, SCR_FILEMAP_KEY_RANK, rank);
  }

  /* remove rank from the CKPT/RANK index, and remove CKPT if that was the last item */
  struct scr_hash* c = scr_filemap_c(map, ckpt);
  scr_hash_unset_kv_int(c, SCR_FILEMAP_KEY_RANK, rank);
  if (scr_hash_size(c) == 0) {
    scr_hash_unset_kv_int(map, SCR_FILEMAP_KEY_CKPT, ckpt);
  }

  return SCR_SUCCESS;
}

/* remove all associations for a given rank */
int scr_filemap_remove_rank(scr_filemap* map, int rank)
{
  /* iterate over and remove every checkpoint this rank has */
  struct scr_hash_elem* ckpt_elem = scr_filemap_first_checkpoint_by_rank(map, rank);
  while (ckpt_elem != NULL) {
    /* get the current checkpoint id */
    int ckpt = scr_hash_elem_key_int(ckpt_elem);

    /* get pointer to the next checkpoint, since we will remove the current one from the list */
    ckpt_elem = scr_hash_elem_next(ckpt_elem);

    /* remove the rank for this checkpoint */
    scr_filemap_remove_rank_by_checkpoint(map, ckpt, rank);
  }
  return SCR_SUCCESS;
}

/* remove all associations for a given checkpoint */
int scr_filemap_remove_checkpoint(scr_filemap* map, int ckpt)
{
  /* iterate over and remove every rank this checkpoint has */
  struct scr_hash_elem* rank_elem = scr_filemap_first_rank_by_checkpoint(map, ckpt);
  while (rank_elem != NULL) {
    /* get the current rank */
    int rank = scr_hash_elem_key_int(rank_elem);

    /* get pointer to the next rank, since we will remove the current one from the list */
    rank_elem = scr_hash_elem_next(rank_elem);

    /* remove the rank for this checkpoint */
    scr_filemap_remove_rank_by_checkpoint(map, ckpt, rank);
  }
  return SCR_SUCCESS;
}

/* clear the filemap completely */
int scr_filemap_clear(scr_filemap* map)
{
  return scr_hash_unset_all(map);
}

/* returns true if we have a hash for specified rank */
int scr_filemap_have_rank(scr_filemap* map, int rank)
{
  struct scr_hash* hash = scr_filemap_r(map, rank);
  return (hash != NULL);
}

/* returns true if we have a hash for specified rank for the given checkpoint */
int scr_filemap_have_rank_by_checkpoint(scr_filemap* map, int ckpt, int rank)
{
  struct scr_hash* hash = scr_filemap_rc(map, ckpt, rank);
  return (hash != NULL);
}

/* returns the latest checkpoint id (largest int) in given map */
int scr_filemap_latest_checkpoint(scr_filemap* map)
{
  /* initialize with a value indicating that we have no checkpoints */
  int ckpt = -1;

  /* now scan through each checkpoint and find the largest id */
  struct scr_hash* hash = scr_filemap_ch(map);
  if (hash != NULL) {
    struct scr_hash_elem* elem;
    for (elem = scr_hash_elem_first(hash);
         elem != NULL;
         elem = scr_hash_elem_next(elem))
    {
      int c = scr_hash_elem_key_int(elem);
      if (c > ckpt) {
        ckpt = c;
      }
    }
  }
  return ckpt;
}

/* returns the oldest checkpoint id (smallest int larger than younger_than) in given map */
int scr_filemap_oldest_checkpoint(scr_filemap* map, int younger_than)
{
  /* initialize our oldest checkpoint id to be the same as the latest checkpoint id */
  int ckpt = scr_filemap_latest_checkpoint(map);

  /* now scan through each checkpoint and find the smallest id that is larger than younger_than */
  struct scr_hash* hash = scr_filemap_ch(map);
  if (hash != NULL) {
    struct scr_hash_elem* elem;
    for (elem = scr_hash_elem_first(hash);
         elem != NULL;
         elem = scr_hash_elem_next(elem))
    {
      int c = scr_hash_elem_key_int(elem);
      if (c > younger_than && c < ckpt) {
        ckpt = c;
      }
    }
  }
  return ckpt;
}

int scr_filemap_int_cmp_fn(const void* a, const void* b)
{
  return (int) (*(int*)a - *(int*)b);
}

/* given a hash, return a list of all keys converted to ints */
/* TODO: must free list when done with it */
int scr_filemap_get_hash_keys(struct scr_hash* hash, int* n, int** v)
{
  /* assume there aren't any keys */
  *n = 0;
  *v = NULL;

  /* count the number of keys */
  int count = scr_hash_size(hash);
  if (count == 0) {
    return SCR_SUCCESS;
  }

  /* now allocate array of ints to save ranks */
  int* list = (int*) malloc(count * sizeof(int));
  if (list == NULL) {
    scr_err("Failed to allocate integer list at %s:%d",
            __FILE__, __LINE__);
    exit(1);
  }

  /* record rank values in array */
  count = 0;
  struct scr_hash_elem* elem;
  for (elem = scr_hash_elem_first(hash);
       elem != NULL;
       elem = scr_hash_elem_next(elem))
  {
    list[count] = atoi(elem->key);
    count++;
  }

  /* sort the keys */
  qsort(list, count, sizeof(int), &scr_filemap_int_cmp_fn);

  *n = count;
  *v = list;

  return SCR_SUCCESS;
}

/* given a filemap, return a list of ranks */
/* TODO: must free ranks list when done with it */
int scr_filemap_list_ranks(scr_filemap* map, int* n, int** v)
{
  struct scr_hash* rh = scr_filemap_rh(map);
  scr_filemap_get_hash_keys(rh, n, v);
  return SCR_SUCCESS;
}

/* given a filemap, return a list of checkpoints */
/* TODO: must free checkpoints list when done with it */
int scr_filemap_list_checkpoints(scr_filemap* map, int* n, int** v)
{
  struct scr_hash* ch = scr_filemap_ch(map);
  scr_filemap_get_hash_keys(ch, n, v);
  return SCR_SUCCESS;
}

/* given a filemap and a checkpoint, return a list of ranks */
/* TODO: must free ranks list when done with it */
int scr_filemap_list_ranks_by_checkpoint(scr_filemap* map, int ckpt, int* n, int** v)
{
  struct scr_hash* c = scr_filemap_c(map, ckpt);
  struct scr_hash* rh = scr_filemap_rh(c);
  scr_filemap_get_hash_keys(rh, n, v);
  return SCR_SUCCESS;
}

/* given a filemap and a rank, return a list of checkpoints */
/* TODO: must free checkpoints list when done with it */
int scr_filemap_list_checkpoints_by_rank(scr_filemap* map, int rank, int* n, int** v)
{
  struct scr_hash* r = scr_filemap_r(map, rank);
  struct scr_hash* ch = scr_filemap_ch(r);
  scr_filemap_get_hash_keys(ch, n, v);
  return SCR_SUCCESS;
}

/* given a filemap, a checkpoint id, and a rank, return the number of files and a list of the filenames */
int scr_filemap_list_files(scr_filemap* map, int ckpt, int rank, int* n, char*** v)
{
  /* assume there aren't any matching files */
  *n = 0;
  *v = NULL;

  /* get rank element */
  struct scr_hash* fh = scr_filemap_fh(map, ckpt, rank);
  int count = scr_hash_size(fh);
  if (count == 0) {
    return SCR_SUCCESS;
  }

  /* now allocate array of pointers to the filenames */
  char** list = (char**) malloc(count * sizeof(char*));
  if (list == NULL) {
    scr_err("Failed to allocate filename list for checkpoint id %d and rank %d at %s:%d",
            ckpt, rank, __FILE__, __LINE__);
    exit(1);
  }

  /* record pointer values in array */
  count = 0;
  struct scr_hash_elem* file;
  for (file = scr_hash_elem_first(fh);
       file != NULL;
       file = scr_hash_elem_next(file))
  {
    list[count] = scr_hash_elem_key(file);
    count++;
  }

  *n = count;
  *v = list;

  return SCR_SUCCESS;
}

/* given a filemap, return a hash elem pointer to the first rank */
struct scr_hash_elem* scr_filemap_first_rank(scr_filemap* map)
{
  return scr_hash_elem_first(scr_filemap_rh(map));
}

/* given a filemap, return a hash elem pointer to the first rank for a given checkpoint */
struct scr_hash_elem* scr_filemap_first_rank_by_checkpoint(scr_filemap* map, int ckpt)
{
  return scr_hash_elem_first(scr_filemap_rh(scr_filemap_c(map, ckpt)));
}

/* given a filemap, return a hash elem pointer to the first checkpoint */
struct scr_hash_elem* scr_filemap_first_checkpoint(scr_filemap* map)
{
  return scr_hash_elem_first(scr_filemap_ch(map));
}

/* given a filemap, return a hash elem pointer to the first checkpoint for a given rank */
struct scr_hash_elem* scr_filemap_first_checkpoint_by_rank(scr_filemap* map, int rank)
{
  return scr_hash_elem_first(scr_filemap_ch(scr_filemap_r(map, rank)));
}

/* given a filemap, a checkpoint id, and a rank, return a hash elem pointer to the first file */
struct scr_hash_elem* scr_filemap_first_file(scr_filemap* map, int ckpt, int rank)
{
  return scr_hash_elem_first(scr_filemap_fh(map, ckpt, rank));
}

/* return the number of ranks in the hash */
int scr_filemap_num_ranks(scr_filemap* map)
{
  return scr_hash_size(scr_filemap_rh(map));
}

/* return the number of ranks in the hash for a given checkpoint id */
int scr_filemap_num_ranks_by_checkpoint(scr_filemap* map, int ckpt)
{
  return scr_hash_size(scr_filemap_rh(scr_filemap_c(map, ckpt)));
}

/* return the number of checkpoints in the hash */
int scr_filemap_num_checkpoints(scr_filemap* map)
{
  return scr_hash_size(scr_filemap_ch(map));
}

/* return the number of files in the hash for a given checkpoint id and rank */
int scr_filemap_num_files(scr_filemap* map, int ckpt, int rank)
{
  return scr_hash_size(scr_filemap_fh(map, ckpt, rank));
}

/* return the number of expected files in the hash for a given checkpoint id and rank */
int scr_filemap_num_expected_files(scr_filemap* map, int ckpt, int rank)
{
  int num = -1;
  struct scr_hash* hash = scr_hash_get(scr_filemap_rc(map, ckpt, rank), SCR_FILEMAP_KEY_EXPECT);
  struct scr_hash_elem* elem = scr_hash_elem_first(hash);
  if (elem != NULL) {
    num = scr_hash_elem_key_int(elem);
  }
  return num;
}

/* allocate a new filemap structure and return it */
scr_filemap* scr_filemap_new()
{
  scr_filemap* map = scr_hash_new();
  if (map == NULL) {
    scr_err("Failed to allocate filemap @ %s:%d", __FILE__, __LINE__);
  }
  return map;
}

/* free memory resources assocaited with filemap */
int scr_filemap_delete(scr_filemap* map)
{
  scr_hash_delete(map);
  return SCR_SUCCESS;
}

/* adds all files from map2 to map1 and updates num_expected_files to total file count */
int scr_filemap_merge(scr_filemap* map1, scr_filemap* map2)
{
  scr_hash_merge(map1, map2);
  return SCR_SUCCESS;
}

/* extract specified rank from given filemap and return as a new filemap */
scr_filemap* scr_filemap_extract_rank(scr_filemap* map, int rank)
{
  /* get a fresh map */
  scr_filemap* new_map = scr_filemap_new();

  /* get hash reference for this rank */
  struct scr_hash* r = scr_filemap_r(map, rank);
  if (r != NULL) {
    /* create a rank hash for this rank in the new map */
    struct scr_hash* new_r = scr_hash_set_kv_int(new_map, SCR_FILEMAP_KEY_RANK, rank);

    /* copy the rank hash for the given rank to the new map */
    scr_hash_merge(new_r, r);

    /* for each checkpoint we have for this rank, set the CKPT/RANK index the new map */
    struct scr_hash_elem* ckpt_elem;
    for (ckpt_elem = scr_filemap_first_checkpoint_by_rank(map, rank);
         ckpt_elem != NULL;
         ckpt_elem = scr_hash_elem_next(ckpt_elem))
    {
      int ckpt = scr_hash_elem_key_int(ckpt_elem);
      struct scr_hash* c  = scr_hash_set_kv_int(new_map, SCR_FILEMAP_KEY_CKPT, ckpt);
      struct scr_hash* cr = scr_hash_set_kv_int(c,       SCR_FILEMAP_KEY_RANK, rank);
    }
  }

  /* remove the rank from the current map */
  scr_filemap_remove_rank(map, rank);

  /* return the new map */
  return new_map;
}

/* reads specified file and fills in filemap structure */
int scr_filemap_read(const char* file, scr_filemap* map)
{
  /* check that we have a map pointer and a hash within the map */
  if (map == NULL) {
    return SCR_FAILURE;
  }

  /* can't read file, return error (special case so as not to print error message below) */
  if (access(file, R_OK) < 0) {
    return SCR_FAILURE;
  }

  /* ok, now try to read the file */
  if (scr_hash_read(file, map) != SCR_SUCCESS) {
    scr_err("Reading filemap %s @ %s:%d",
            file, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* TODO: check that file count for each rank matches expected count */

  return SCR_SUCCESS;
}

/* writes given filemap to specified file */
int scr_filemap_write(const char* file, scr_filemap* map)
{
  /* check that we have a map pointer */
  if (map == NULL) {
    return SCR_FAILURE;
  }

  /* write out the hash */
  if (scr_hash_write(file, map) != SCR_SUCCESS) {
    scr_err("Writing filemap %s @ %s:%d",
            file, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  return SCR_SUCCESS;
}
