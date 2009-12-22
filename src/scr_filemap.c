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

#define SCR_FILEMAP_HASH_RANK   ("RANK")
#define SCR_FILEMAP_HASH_CKPT   ("CKPT")
#define SCR_FILEMAP_HASH_FILE   ("FILE")
#define SCR_FILEMAP_HASH_EXPECT ("EXPECT")

struct scr_hash* scr_filemap_rh(struct scr_hash* h)
{
  struct scr_hash* rh = scr_hash_get(h, SCR_FILEMAP_HASH_RANK);
  return rh;
}

struct scr_hash* scr_filemap_ch(struct scr_hash* h)
{
  struct scr_hash* ch = scr_hash_get(h, SCR_FILEMAP_HASH_CKPT);
  return ch;
}

struct scr_hash* scr_filemap_r(struct scr_hash* h, int rank)
{
  struct scr_hash* r = scr_hash_get_kv_int(h, SCR_FILEMAP_HASH_RANK, rank);
  return r;
}

struct scr_hash* scr_filemap_c(struct scr_hash* h, int ckpt)
{
  struct scr_hash* c = scr_hash_get_kv_int(h, SCR_FILEMAP_HASH_CKPT, ckpt);
  return c;
}

struct scr_hash* scr_filemap_rc(struct scr_hash* h, int ckpt, int rank)
{
  struct scr_hash* r  = scr_filemap_r(h, rank);
  struct scr_hash* rc = scr_filemap_c(r, ckpt);
  return rc;
}

struct scr_hash* scr_filemap_cr(struct scr_hash* h, int ckpt, int rank)
{
  struct scr_hash* c  = scr_filemap_c(h, ckpt);
  struct scr_hash* cr = scr_filemap_r(c, rank);
  return cr;
}

struct scr_hash* scr_filemap_fh(struct scr_hash* hash, int ckpt, int rank)
{
  struct scr_hash* rc = scr_filemap_rc(hash, ckpt, rank);
  struct scr_hash* fh = scr_hash_get(rc, SCR_FILEMAP_HASH_FILE);
  return fh;
}

struct scr_hash* scr_filemap_rcf(struct scr_hash* hash, int ckpt, int rank, const char* file)
{
  struct scr_hash* rc  = scr_filemap_rc(hash, ckpt, rank);
  struct scr_hash* rcf = scr_hash_get_kv(rc, SCR_FILEMAP_HASH_FILE, file);
  return rcf;
}

struct scr_hash* scr_filemap_crf(struct scr_hash* hash, int ckpt, int rank, const char* file)
{
  struct scr_hash* cr  = scr_filemap_cr(hash, ckpt, rank);
  struct scr_hash* crf = scr_hash_get_kv(cr, SCR_FILEMAP_HASH_FILE, file);
  return crf;
}

/* adds a new filename to the filemap and associates it with a specified checkpoint id and a rank */
int scr_filemap_add_file(struct scr_filemap* map, int ckpt, int rank, const char* file)
{
  /* add file to Rank/CheckpointID/File hash */
  struct scr_hash* r  = scr_hash_set_kv_int(map->hash, SCR_FILEMAP_HASH_RANK, rank);
  struct scr_hash* rc = scr_hash_set_kv_int(r,         SCR_FILEMAP_HASH_CKPT, ckpt);
  scr_hash_set_kv(rc, SCR_FILEMAP_HASH_FILE, file);

  /* add file to CheckpointID/Rank/File hash */
  struct scr_hash* c  = scr_hash_set_kv_int(map->hash, SCR_FILEMAP_HASH_CKPT, ckpt);
  struct scr_hash* cr = scr_hash_set_kv_int(c,         SCR_FILEMAP_HASH_RANK, rank);
  scr_hash_set_kv(cr, SCR_FILEMAP_HASH_FILE, file);

  return SCR_SUCCESS;
}

/* removes a filename for a given checkpoint id and rank from the filemap */
int scr_filemap_remove_file(struct scr_filemap* map, int ckpt, int rank, const char* file)
{
  /* remove file from Rank/CheckpointID/File hash */
  struct scr_hash* r   = scr_filemap_r(map->hash, rank);
  struct scr_hash* rc  = scr_filemap_c(r, ckpt);
  scr_hash_unset_kv(rc, SCR_FILEMAP_HASH_FILE, file);
  if (scr_hash_size(rc) == 0) { scr_hash_unset_kv_int(r, SCR_FILEMAP_HASH_CKPT, ckpt); }
  if (scr_hash_size(r)  == 0) { scr_hash_unset_kv_int(map->hash, SCR_FILEMAP_HASH_RANK, rank); }
  
  /* remove file from CheckpointID/Rank/File hash */
  struct scr_hash* c  = scr_filemap_c(map->hash, ckpt);
  struct scr_hash* cr = scr_filemap_r(c, rank);
  scr_hash_unset_kv(cr, SCR_FILEMAP_HASH_FILE, file);
  if (scr_hash_size(cr) == 0) { scr_hash_unset_kv_int(c, SCR_FILEMAP_HASH_RANK, rank); }
  if (scr_hash_size(c)  == 0) { scr_hash_unset_kv_int(map->hash, SCR_FILEMAP_HASH_CKPT, ckpt); }

  return SCR_SUCCESS;
}

/* set number of files to expect for a given rank in a given checkpoint id */
int scr_filemap_set_expected_files(struct scr_filemap* map, int ckpt, int rank, int expect)
{
  /* add file to Rank/CheckpointID/File hash */
  struct scr_hash* r  = scr_hash_set_kv_int(map->hash, SCR_FILEMAP_HASH_RANK, rank);
  struct scr_hash* rc = scr_hash_set_kv_int(r,         SCR_FILEMAP_HASH_CKPT, ckpt);
  scr_hash_set_kv_int(rc, SCR_FILEMAP_HASH_EXPECT, expect);

  /* add file to CheckpointID/Rank/File hash */
  struct scr_hash* c  = scr_hash_set_kv_int(map->hash, SCR_FILEMAP_HASH_CKPT, ckpt);
  struct scr_hash* cr = scr_hash_set_kv_int(c,         SCR_FILEMAP_HASH_RANK, rank);
  scr_hash_set_kv_int(cr, SCR_FILEMAP_HASH_EXPECT, expect);

  return SCR_SUCCESS;
}

/* unset number of files to expect for a given rank in a given checkpoint id */
int scr_filemap_unset_expected_files(struct scr_filemap* map, int ckpt, int rank)
{
  /* add file to Rank/CheckpointID/File hash */
  struct scr_hash* r  = scr_hash_get_kv_int(map->hash, SCR_FILEMAP_HASH_RANK, rank);
  struct scr_hash* rc = scr_hash_get_kv_int(r,         SCR_FILEMAP_HASH_CKPT, ckpt);
  scr_hash_unset(rc, SCR_FILEMAP_HASH_EXPECT);
  if (scr_hash_size(rc) == 0) { scr_hash_unset_kv_int(r, SCR_FILEMAP_HASH_CKPT, ckpt); }
  if (scr_hash_size(r)  == 0) { scr_hash_unset_kv_int(map->hash, SCR_FILEMAP_HASH_RANK, rank); }

  /* add file to CheckpointID/Rank/File hash */
  struct scr_hash* c  = scr_hash_get_kv_int(map->hash, SCR_FILEMAP_HASH_CKPT, ckpt);
  struct scr_hash* cr = scr_hash_get_kv_int(c,         SCR_FILEMAP_HASH_RANK, rank);
  scr_hash_unset(cr, SCR_FILEMAP_HASH_EXPECT);
  if (scr_hash_size(cr) == 0) { scr_hash_unset_kv_int(c, SCR_FILEMAP_HASH_RANK, rank); }
  if (scr_hash_size(c)  == 0) { scr_hash_unset_kv_int(map->hash, SCR_FILEMAP_HASH_CKPT, ckpt); }

  return SCR_SUCCESS;
}

/* sets a tag/value pair on a given file */
int scr_filemap_set_tag(struct scr_filemap* map, int ckpt, int rank, const char* file, const char* tag, const char* value)
{
  /* define tag in Rank/CheckpointID/File hash */
  struct scr_hash* rcf = scr_filemap_rcf(map->hash, ckpt, rank, file);
  scr_hash_set_kv(rcf, tag, value);

  /* define tag in CheckpointID/Rank/File hash */
  struct scr_hash* crf = scr_filemap_crf(map->hash, ckpt, rank, file);
  scr_hash_set_kv(crf, tag, value);

  return SCR_SUCCESS;
}

/* sets a tag/value pair on a given file */
int scr_filemap_unset_tag(struct scr_filemap* map, int ckpt, int rank, const char* file, const char* tag)
{
  /* define tag in Rank/CheckpointID/File hash */
  struct scr_hash* rcf = scr_filemap_rcf(map->hash, ckpt, rank, file);
  scr_hash_unset(rcf, tag);

  /* define tag in CheckpointID/Rank/File hash */
  struct scr_hash* crf = scr_filemap_crf(map->hash, ckpt, rank, file);
  scr_hash_unset(crf, tag);

  return SCR_SUCCESS;
}

/* copies file data (including tags) from one filemap to another */
int scr_filemap_copy_file(struct scr_filemap* map, struct scr_filemap* src_map, int ckpt, int rank, const char* file)
{
  /* first add the file to the map */
  scr_filemap_add_file(map, ckpt, rank, file);

  /* now copy over all tags found in src_map for this file */
  struct scr_hash* rcf = scr_filemap_rcf(src_map->hash, ckpt, rank, file);
  struct scr_hash_elem* tag_elem;
  for (tag_elem = scr_hash_elem_first(rcf);
       tag_elem != NULL;
       tag_elem = scr_hash_elem_next(tag_elem))
  {
    char* tag   = scr_hash_elem_key(tag_elem);
    char* value = scr_hash_elem_key(scr_hash_elem_first(scr_hash_elem_hash(tag_elem)));
    scr_filemap_set_tag(map, ckpt, rank, file, tag, value);
  }

  return SCR_SUCCESS;
}

/* remove all associations for a given rank in a given checkpoint */
int scr_filemap_remove_rank_by_checkpoint(struct scr_filemap* map, int ckpt, int rank)
{
  /* remove files and expected field on Rank/CheckpointID hash */
  struct scr_hash* r  = scr_hash_get_kv_int(map->hash, SCR_FILEMAP_HASH_RANK, rank);
  scr_hash_unset_kv_int(r, SCR_FILEMAP_HASH_CKPT, ckpt);
  if (scr_hash_size(r) == 0) { scr_hash_unset_kv_int(map->hash, SCR_FILEMAP_HASH_RANK, rank); }

  struct scr_hash* c  = scr_hash_get_kv_int(map->hash, SCR_FILEMAP_HASH_CKPT, ckpt);
  scr_hash_unset_kv_int(c, SCR_FILEMAP_HASH_RANK, rank);
  if (scr_hash_size(c) == 0) { scr_hash_unset_kv_int(map->hash, SCR_FILEMAP_HASH_CKPT, ckpt); }

  return SCR_SUCCESS;
}

/* remove all associations for a given rank */
int scr_filemap_remove_rank(struct scr_filemap* map, int rank)
{
  /* remove this rank for every checkpoint */
  struct scr_hash_elem* next_elem = NULL;
  struct scr_hash_elem* ckpt_elem = scr_filemap_first_checkpoint_by_rank(map, rank);
  while (ckpt_elem != NULL) {
    /* get pointer to the next checkpoint, since we will remove the current one from the list */
    next_elem = scr_hash_elem_next(ckpt_elem);

    /* get the current checkpoint id */
    int ckpt = scr_hash_elem_key_int(ckpt_elem);

    /* remove the rank for this checkpoint */
    scr_filemap_remove_rank_by_checkpoint(map, ckpt, rank);

    /* advance the pointer */
    ckpt_elem = next_elem;
  }
  return SCR_SUCCESS;
}

/* remove all associations for a given checkpoint */
int scr_filemap_remove_checkpoint(struct scr_filemap* map, int ckpt)
{
  /* remove this checkpoint for every rank */
  struct scr_hash_elem* next_elem = NULL;
  struct scr_hash_elem* rank_elem = scr_filemap_first_rank_by_checkpoint(map, ckpt);
  while (rank_elem != NULL) {
    /* get pointer to the next rank, since we will remove the current one from the list */
    next_elem = scr_hash_elem_next(rank_elem);

    /* get the current rank */
    int rank = scr_hash_elem_key_int(rank_elem);

    /* remove the rank for this checkpoint */
    scr_filemap_remove_rank_by_checkpoint(map, ckpt, rank);

    /* advance the pointer */
    rank_elem = next_elem;
  }
  return SCR_SUCCESS;
}

/* returns true if have a hash for specified rank */
int scr_filemap_have_rank(struct scr_filemap* map, int rank)
{
  return (scr_filemap_r(map->hash, rank) != NULL);
}

/* returns true if have a hash for specified rank */
int scr_filemap_have_rank_by_checkpoint(struct scr_filemap* map, int ckpt, int rank)
{
  return (scr_filemap_rc(map->hash, ckpt, rank) != NULL);
}

/* returns the latest checkpoint id (largest int) in given map */
int scr_filemap_latest_checkpoint(struct scr_filemap* map)
{
  int ckpt = -1;
  struct scr_hash* hash = scr_filemap_ch(map->hash);
  if (hash != NULL) {
    struct scr_hash_elem* elem;
    for (elem = scr_hash_elem_first(hash);
         elem != NULL;
         elem = scr_hash_elem_next(elem))
    {
      int c = scr_hash_elem_key_int(elem);
      if (c > ckpt) { ckpt = c; }
    }
  }
  return ckpt;
}

/* returns the oldest checkpoint id (smallest int) in given map */
int scr_filemap_oldest_checkpoint(struct scr_filemap* map)
{
  int ckpt = scr_filemap_latest_checkpoint(map);
  struct scr_hash* hash = scr_filemap_ch(map->hash);
  if (hash != NULL) {
    struct scr_hash_elem* elem;
    for (elem = scr_hash_elem_first(hash);
         elem != NULL;
         elem = scr_hash_elem_next(elem))
    {
      int c = scr_hash_elem_key_int(elem);
      if (c < ckpt) { ckpt = c; }
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
int scr_filemap_list_ranks(struct scr_filemap* map, int* n, int** v)
{
  struct scr_hash* rh = scr_filemap_rh(map->hash);
  scr_filemap_get_hash_keys(rh, n, v);
  return SCR_SUCCESS;
}

/* given a filemap, return a list of checkpoints */
/* TODO: must free checkpoints list when done with it */
int scr_filemap_list_checkpoints(struct scr_filemap* map, int* n, int** v)
{
  struct scr_hash* ch = scr_filemap_ch(map->hash);
  scr_filemap_get_hash_keys(ch, n, v);
  return SCR_SUCCESS;
}

/* given a filemap and a checkpoint, return a list of ranks */
/* TODO: must free ranks list when done with it */
int scr_filemap_list_ranks_by_checkpoint(struct scr_filemap* map, int ckpt, int* n, int** v)
{
  struct scr_hash* c = scr_filemap_c(map->hash, ckpt);
  struct scr_hash* rh = scr_filemap_rh(c);
  scr_filemap_get_hash_keys(rh, n, v);
  return SCR_SUCCESS;
}

/* given a filemap and a rank, return a list of checkpoints */
/* TODO: must free checkpoints list when done with it */
int scr_filemap_list_checkpoints_by_rank(struct scr_filemap* map, int rank, int* n, int** v)
{
  struct scr_hash* r = scr_filemap_r(map->hash, rank);
  struct scr_hash* ch = scr_filemap_ch(r);
  scr_filemap_get_hash_keys(ch, n, v);
  return SCR_SUCCESS;
}

/* given a filemap, a checkpoint id, and a rank, return the number of files and a list of the filenames */
int scr_filemap_list_files(struct scr_filemap* map, int ckpt, int rank, int* n, char*** v)
{
  /* assume there aren't any matching files */
  *n = 0;
  *v = NULL;

  /* get rank element */
  struct scr_hash* fh = scr_filemap_fh(map->hash, ckpt, rank);
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
struct scr_hash_elem* scr_filemap_first_rank(struct scr_filemap* map)
{
  return scr_hash_elem_first(scr_filemap_rh(map->hash));
}

/* given a filemap, return a hash elem pointer to the first rank for a given checkpoint */
struct scr_hash_elem* scr_filemap_first_rank_by_checkpoint(struct scr_filemap* map, int ckpt)
{
  return scr_hash_elem_first(scr_filemap_rh(scr_filemap_c(map->hash, ckpt)));
}

/* given a filemap, return a hash elem pointer to the first checkpoint */
struct scr_hash_elem* scr_filemap_first_checkpoint(struct scr_filemap* map)
{
  return scr_hash_elem_first(scr_filemap_ch(map->hash));
}

/* given a filemap, return a hash elem pointer to the first checkpoint for a given rank */
struct scr_hash_elem* scr_filemap_first_checkpoint_by_rank(struct scr_filemap* map, int rank)
{
  return scr_hash_elem_first(scr_filemap_ch(scr_filemap_r(map->hash, rank)));
}

/* given a filemap, a checkpoint id, and a rank, return a hash elem pointer to the first file */
struct scr_hash_elem* scr_filemap_first_file(struct scr_filemap* map, int ckpt, int rank)
{
  return scr_hash_elem_first(scr_filemap_fh(map->hash, ckpt, rank));
}

/* return the number of ranks in the hash */
int scr_filemap_num_ranks(struct scr_filemap* map)
{
  return scr_hash_size(scr_filemap_rh(map->hash));
}

/* return the number of ranks in the hash for a given checkpoint id */
int scr_filemap_num_ranks_by_checkpoint(struct scr_filemap* map, int ckpt)
{
  return scr_hash_size(scr_filemap_rh(scr_filemap_c(map->hash, ckpt)));
}

/* return the number of checkpoints in the hash */
int scr_filemap_num_checkpoints(struct scr_filemap* map)
{
  return scr_hash_size(scr_filemap_ch(map->hash));
}

/* return the number of files in the hash for a given checkpoint id and rank */
int scr_filemap_num_files(struct scr_filemap* map, int ckpt, int rank)
{
  return scr_hash_size(scr_filemap_fh(map->hash, ckpt, rank));
}

/* return the number of expected files in the hash for a given checkpoint id and rank */
int scr_filemap_num_expected_files(struct scr_filemap* map, int ckpt, int rank)
{
  int num = -1;
  struct scr_hash* hash = scr_hash_get(scr_filemap_rc(map->hash, ckpt, rank), SCR_FILEMAP_HASH_EXPECT);
  struct scr_hash_elem* elem = scr_hash_elem_first(hash);
  if (elem != NULL) { num = scr_hash_elem_key_int(elem); }
  return num;
}

/* allocate a new filemap structure and return it */
struct scr_filemap* scr_filemap_new()
{
  struct scr_filemap* map = (struct scr_filemap*) malloc(sizeof(struct scr_filemap));

  if (map != NULL) {
    /* allocate a new list object for this filemap */
    map->hash = scr_hash_new();
  } else {
    scr_err("Failed to allocate filemap structure @ %s:%d", __FILE__, __LINE__);
  }

  return map;
}

/* free memory resources assocaited with filemap */
int scr_filemap_delete(struct scr_filemap* map)
{
  if (map != NULL) {
    if (map->hash != NULL) { scr_hash_delete(map->hash); map->hash = NULL; }
    free(map); map = NULL;
  }
  return SCR_SUCCESS;
}

/* adds all files from map2 to map1 and updates num_expected_files to total file count */
int scr_filemap_merge(struct scr_filemap* map1, struct scr_filemap* map2)
{
  struct scr_hash_elem* rank_elem;
  for (rank_elem = scr_filemap_first_rank(map2);
       rank_elem != NULL;
       rank_elem = scr_hash_elem_next(rank_elem))
  {
    int rank = scr_hash_elem_key_int(rank_elem);
    struct scr_hash_elem* ckpt_elem;
    for (ckpt_elem = scr_filemap_first_checkpoint_by_rank(map2, rank);
         ckpt_elem != NULL;
         ckpt_elem = scr_hash_elem_next(ckpt_elem))
    {
      int ckpt = scr_hash_elem_key_int(ckpt_elem);
      /* TODO: don't know how to handle a case where there are missing files, so throw a fatal error for now */
      if ((scr_filemap_num_expected_files(map1, ckpt, rank) >= 0 &&
          scr_filemap_num_expected_files(map1, ckpt, rank) != scr_filemap_num_files(map1, ckpt, rank)) ||
          (scr_filemap_num_expected_files(map2, ckpt, rank) >= 0 &&
          scr_filemap_num_expected_files(map2, ckpt, rank) != scr_filemap_num_files(map2, ckpt, rank)))
      {
        fprintf(stderr, "ERROR: Can only merge filemaps if all expected files are accounted for @ %s:%d\n",
                __FILE__, __LINE__
        );
        exit(1);
      }
      struct scr_hash_elem* file_elem;
      for (file_elem = scr_filemap_first_file(map2, ckpt, rank);
           file_elem != NULL;
           file_elem = scr_hash_elem_next(file_elem))
      {
        char* file = scr_hash_elem_key(file_elem);
        scr_filemap_copy_file(map1, map2, ckpt, rank, file);
      }
      scr_filemap_set_expected_files(map1, ckpt, rank, scr_filemap_num_files(map1, ckpt, rank));
    }
  }

  return SCR_SUCCESS;
}

/* extract specified rank from given filemap and return as a new filemap */
struct scr_filemap* scr_filemap_extract_rank(struct scr_filemap* map, int rank)
{
  /* get a fresh map */
  struct scr_filemap* new_map = scr_filemap_new();

  /* for the given rank, add each file in the current map to the new map */
  struct scr_hash_elem* ckpt_elem;
  for (ckpt_elem = scr_filemap_first_checkpoint_by_rank(map, rank);
       ckpt_elem != NULL;
       ckpt_elem = scr_hash_elem_next(ckpt_elem))
  {
    int ckpt = scr_hash_elem_key_int(ckpt_elem);
    struct scr_hash_elem* file_elem;
    for (file_elem = scr_filemap_first_file(map, ckpt, rank);
         file_elem != NULL;
         file_elem = scr_hash_elem_next(file_elem))
    {
      char* file = scr_hash_elem_key(file_elem);
      scr_filemap_copy_file(new_map, map, ckpt, rank, file);
    }

    /* copy the expected number of files over */
    scr_filemap_set_expected_files(new_map, ckpt, rank, scr_filemap_num_expected_files(map, ckpt, rank));
  }

  /* remove the rank from the current map */
  scr_filemap_remove_rank(map, rank);

  /* return the new map */
  return new_map;
}

/* reads specified file and fills in filemap structure */
int scr_filemap_read(const char* map_file, struct scr_filemap* map)
{
  /* check that we have a map pointer and a hash within the map */
  if (map == NULL || map->hash == NULL) { return SCR_FAILURE; }

  /* can't read file, return error (special case so as not to print error message below) */
  if (access(map_file, R_OK) < 0) { return SCR_FAILURE; }

  FILE* fs = fopen(map_file, "r");
  if (fs == NULL) {
    scr_err("Opening filemap for read: fopen(%s, \"r\") errno=%d %m @ %s:%d",
            map_file, errno, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  char field[SCR_MAX_FILENAME];
  char value[SCR_MAX_FILENAME];
  int n;
  int rank, ckpt, exp_files;
  char filename[SCR_MAX_FILENAME];
  int found_end = 0;
  do {
    n = fscanf(fs, "%s %s\n", field, value);
    if (n != EOF) {
      if (strcmp(field, "Rank:") == 0) {
        rank = atoi(value);
      } else if (strcmp(field, "CheckpointID:") == 0) {
        ckpt = atoi(value);
      } else if (strcmp(field, "Expect:") == 0) {
        exp_files = atoi(value);
        scr_filemap_set_expected_files(map, ckpt, rank, exp_files);
      } else if (strcmp(field, "File:") == 0) {
        if (strlen(value) + 1 >= sizeof(filename)) {
          scr_err("Filename of %d bytes is too long for fixed buffer of %d bytes @ %s:%d",
                  strlen(value) + 1, sizeof(filename), __FILE__, __LINE__
          );
          return SCR_FAILURE;
        }
        strcpy(filename, value);
        scr_filemap_add_file(map, ckpt, rank, filename);
      } else if (strcmp(field, "Tag:") == 0) {
        char* field2 = value;
        char* value2 = strchr(value, (int) ':');
        *value2 = '\0'; value2++;
        scr_filemap_set_tag(map, ckpt, rank, filename, field2, value2);
      } else if (strcmp(field, "End:") == 0) {
        found_end = 1;
      }
    }
  } while (n != EOF);

  fclose(fs);

  /* TODO: check that file count for each rank matches expected count */

  /* if we didn't find the End flag, consider the whole file to be missing */
  if (!found_end) {
    scr_hash_delete(map->hash);
    map->hash = scr_hash_new();
    scr_err("Missing End tag in filemap %s @ %s:%d",
            map_file, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  return SCR_SUCCESS;
}

/* writes given filemap to specified file */
int scr_filemap_write(const char* file, struct scr_filemap* map)
{
  /* write out the meta data */
  char buf[SCR_MAX_FILENAME];

  int fd = scr_open(file, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
  if (fd < 0) {
    scr_err("Opening filemap for write: scr_open(%s) errno=%d %m @ %s:%d",
            file, errno, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  struct scr_hash_elem* rank_elem;
  for (rank_elem = scr_filemap_first_rank(map);
       rank_elem != NULL;
       rank_elem = scr_hash_elem_next(rank_elem))
  {
    int r = scr_hash_elem_key_int(rank_elem);
    sprintf(buf, "Rank: %d\n", r);
    scr_write(fd, buf, strlen(buf));
    struct scr_hash_elem* ckpt_elem;
    for (ckpt_elem = scr_filemap_first_checkpoint_by_rank(map, r);
         ckpt_elem != NULL;
         ckpt_elem = scr_hash_elem_next(ckpt_elem))
    {
      int c = scr_hash_elem_key_int(ckpt_elem);
      sprintf(buf, "CheckpointID: %d\n", c);
      scr_write(fd, buf, strlen(buf));
      sprintf(buf, "Expect: %d\n", scr_filemap_num_expected_files(map, c, r));
      scr_write(fd, buf, strlen(buf));
      struct scr_hash_elem* file_elem;
      for (file_elem = scr_filemap_first_file(map, c, r);
           file_elem != NULL;
           file_elem = scr_hash_elem_next(file_elem))
      {
        char* file = scr_hash_elem_key(file_elem);
        sprintf(buf, "File: %s\n", file);
        scr_write(fd, buf, strlen(buf));
        struct scr_hash_elem* tag_elem;
        for (tag_elem = scr_hash_elem_first(scr_hash_elem_hash(file_elem));
             tag_elem != NULL;
             tag_elem = scr_hash_elem_next(tag_elem))
        {
          char* tag   = scr_hash_elem_key(tag_elem);
          char* value = scr_hash_elem_key(scr_hash_elem_first(scr_hash_elem_hash(tag_elem)));
          sprintf(buf, "Tag: %s:%s\n", tag, value);
          scr_write(fd, buf, strlen(buf));
        }
      }
    }
  }

  /* write End flag to file (assume incomplete if End is not found) */
  sprintf(buf, "End: 0\n");
  scr_write(fd, buf, strlen(buf));

  /* flush and close the file */
  scr_close(fd);

  return SCR_SUCCESS;
}
