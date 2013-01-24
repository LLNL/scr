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
 * dataset. */

/*
GOALS:
  - support different number of processes per node on
    a restart
  - support multiple files per rank per dataset
  - support multiple datasets at different cache levels

READ:
  master process on each node reads filemap
  and distributes pieces to others

WRITE:
  all processes send their file info to master
  and master writes it out

  master filemap file
    list of ranks this node has files for
      for each rank, list of dataset ids
        for each dataset id, list of locations (RAM, SSD, PFS, etc)
            for each location, list of files for this rank for this dataset
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
#include "scr_util.h"
#include "scr_hash.h"
#include "scr_filemap.h"

#define SCR_FILEMAP_KEY_RANK    ("RANK")
#define SCR_FILEMAP_KEY_DSET    ("DSET")
#define SCR_FILEMAP_KEY_FILES   ("FILES")
#define SCR_FILEMAP_KEY_FILE    ("FILE")
#define SCR_FILEMAP_KEY_REDDESC ("REDDESC")
#define SCR_FILEMAP_KEY_FLUSH   ("FLUSH")
#define SCR_FILEMAP_KEY_DATA    ("DSETDESC")
#define SCR_FILEMAP_KEY_META    ("META")

/* returns the RANK hash */
static scr_hash* scr_filemap_get_rh(const scr_hash* h)
{
  scr_hash* rh = scr_hash_get(h, SCR_FILEMAP_KEY_RANK);
  return rh;
}

/* returns the DSET hash */
static scr_hash* scr_filemap_get_dh(const scr_hash* h)
{
  scr_hash* dh = scr_hash_get(h, SCR_FILEMAP_KEY_DSET);
  return dh;
}

/* returns the hash associated with a particular rank */
static scr_hash* scr_filemap_get_r(const scr_hash* h, int rank)
{
  scr_hash* r = scr_hash_get_kv_int(h, SCR_FILEMAP_KEY_RANK, rank);
  return r;
}

/* returns the hash associated with a particular dataset */
static scr_hash* scr_filemap_get_d(const scr_hash* h, int dset)
{
  scr_hash* d = scr_hash_get_kv_int(h, SCR_FILEMAP_KEY_DSET, dset);
  return d;
}

/* returns the hash associated with a particular rank and dataset pair */
static scr_hash* scr_filemap_get_rd(const scr_hash* h, int dset, int rank)
{
  scr_hash* r  = scr_filemap_get_r(h, rank);
  scr_hash* rd = scr_filemap_get_d(r, dset);
  return rd;
}

/* returns the FILE hash associated with a particular rank and dataset pair */
static scr_hash* scr_filemap_get_fh(const scr_hash* hash, int dset, int rank)
{
  scr_hash* rd = scr_filemap_get_rd(hash, dset, rank);
  scr_hash* fh = scr_hash_get(rd, SCR_FILEMAP_KEY_FILE);
  return fh;
}

/* returns the hash associated with a particular rank, dataset, and file tuple */
static scr_hash* scr_filemap_get_rdf(const scr_hash* hash, int dset, int rank, const char* file)
{
  scr_hash* fh  = scr_filemap_get_fh(hash, dset, rank);
  scr_hash* rdf = scr_hash_get(fh, file);
  return rdf;
}

/* creates and returns a hash under RANK/DSET, also sets DSET/RANK index */
static scr_hash* scr_filemap_set_rd(scr_filemap* map, int dset, int rank)
{
  /* set RANK/DSET index and get hash references */
  scr_hash* r  = scr_hash_set_kv_int(map, SCR_FILEMAP_KEY_RANK, rank);
  scr_hash* rd = scr_hash_set_kv_int(r,   SCR_FILEMAP_KEY_DSET, dset);

  /* set DSET/RANK index */
  scr_hash* d  = scr_hash_set_kv_int(map, SCR_FILEMAP_KEY_DSET, dset);
                 scr_hash_set_kv_int(d,   SCR_FILEMAP_KEY_RANK, rank);

  return rd;
}

/* unset RANK/DSET and DSET/RANK indicies if the map for this rank and dataset is empty */
static int scr_filemap_unset_if_empty(scr_filemap* map, int dset, int rank)
{
  /* get hash references for this rank and dataset pair */
  scr_hash* r  = scr_filemap_get_r(map, rank);
  scr_hash* d  = scr_filemap_get_d(map, dset);

  /* see if we have anything left in the map for this rank and dataset */
  scr_hash* rd = scr_filemap_get_d(r,   dset);
  if (scr_hash_size(rd) == 0) {
    /* unset the dataset under the rank / dataset index */
    scr_hash_unset_kv_int(r, SCR_FILEMAP_KEY_DSET, dset);

    /* and unset the rank under the dataset / rank index */
    scr_hash_unset_kv_int(d, SCR_FILEMAP_KEY_RANK, rank);
  }

  /* if there is nothing left under this rank, unset the rank */
  if (scr_hash_size(r) == 0) {
    scr_hash_unset_kv_int(map, SCR_FILEMAP_KEY_RANK, rank);
  }

  /* if there is nothing left under this dataset, unset the dataset */
  if (scr_hash_size(d) == 0) {
    scr_hash_unset_kv_int(map, SCR_FILEMAP_KEY_DSET, dset);
  }

  return SCR_SUCCESS;
}

/* adds a new filename to the filemap and associates it with a specified dataset id and a rank */
int scr_filemap_add_file(scr_filemap* map, int dset, int rank, const char* file)
{
  /* set indicies and get hash reference */
  scr_hash* rd = scr_filemap_set_rd(map, dset, rank);

  /* add file to RANK/DSET/FILE hash */
  scr_hash_set_kv(rd, SCR_FILEMAP_KEY_FILE, file);

  return SCR_SUCCESS;
}

/* removes a filename for a given dataset id and rank from the filemap */
int scr_filemap_remove_file(scr_filemap* map, int dset, int rank, const char* file)
{
  /* remove file from RANK/DSET/FILE hash */
  scr_hash* rd = scr_filemap_get_rd(map, dset, rank);
  scr_hash_unset_kv(rd, SCR_FILEMAP_KEY_FILE, file);

  /* unset RANK/DSET and DSET/RANK indicies if the hash is empty */
  scr_filemap_unset_if_empty(map, dset, rank);

  return SCR_SUCCESS;
}

/* sets the redundancy descriptor hash for the given rank and dataset id */
int scr_filemap_set_desc(scr_filemap* map, int dset, int rank, scr_hash* hash)
{
  /* set indicies and get hash reference */
  scr_hash* rd = scr_filemap_set_rd(map, dset, rank);

  /* set the REDDESC value under the RANK/DSET hash */
  scr_hash_unset(rd, SCR_FILEMAP_KEY_REDDESC);
  scr_hash* desc = scr_hash_new();
  scr_hash_merge(desc, hash);
  scr_hash_set(rd, SCR_FILEMAP_KEY_REDDESC, desc);

  return SCR_SUCCESS;
}

/* copies the redundancy descriptor hash for the given rank and dataset id into hash */
int scr_filemap_get_desc(const scr_filemap* map, int dset, int rank, scr_hash* hash)
{
  /* get RANK/DSET hash */
  scr_hash* rd = scr_filemap_get_rd(map, dset, rank);

  /* get the REDDESC value under the RANK/DSET hash */
  scr_hash* desc = scr_hash_get(rd, SCR_FILEMAP_KEY_REDDESC);
  if (desc != NULL) {
    scr_hash_merge(hash, desc);
    return SCR_SUCCESS;
  }

  return SCR_FAILURE; 
}

/* unset the redundancy descriptor hash for the given rank and dataset id */
int scr_filemap_unset_desc(scr_filemap* map, int dset, int rank)
{
  /* unset REDDESC value */
  scr_hash* rd = scr_filemap_get_rd(map, dset, rank);
  scr_hash_unset(rd, SCR_FILEMAP_KEY_REDDESC);

  /* unset RANK/DSET and DSET/RANK indicies if the hash is empty */
  scr_filemap_unset_if_empty(map, dset, rank);

  return SCR_SUCCESS;
}

/* sets the flush/scavenge descriptor hash for the given rank and dataset id */
int scr_filemap_set_flushdesc(scr_filemap* map, int dset, int rank, scr_hash* hash)
{
  /* set indicies and get hash reference */
  scr_hash* rd = scr_filemap_set_rd(map, dset, rank);

  /* set the FLUSH value under the RANK/DSET hash */
  scr_hash_unset(rd, SCR_FILEMAP_KEY_FLUSH);
  scr_hash* desc = scr_hash_new();
  scr_hash_merge(desc, hash);
  scr_hash_set(rd, SCR_FILEMAP_KEY_FLUSH, desc);

  return SCR_SUCCESS;
}

/* copies the flush/scavenge descriptor hash for the given rank and dataset id into hash */
int scr_filemap_get_flushdesc(const scr_filemap* map, int dset, int rank, scr_hash* hash)
{
  /* get RANK/DSET hash */
  scr_hash* rd = scr_filemap_get_rd(map, dset, rank);

  /* get the FLUSH value under the RANK/DSET hash */
  scr_hash* desc = scr_hash_get(rd, SCR_FILEMAP_KEY_FLUSH);
  if (desc != NULL) {
    scr_hash_merge(hash, desc);
    return SCR_SUCCESS;
  }

  return SCR_FAILURE; 
}

/* unset the flush/scavenge descriptor hash for the given rank and dataset id */
int scr_filemap_unset_flushdesc(scr_filemap* map, int dset, int rank)
{
  /* unset FLUSH value */
  scr_hash* rd = scr_filemap_get_rd(map, dset, rank);
  scr_hash_unset(rd, SCR_FILEMAP_KEY_FLUSH);

  /* unset RANK/DSET and DSET/RANK indicies if the hash is empty */
  scr_filemap_unset_if_empty(map, dset, rank);

  return SCR_SUCCESS;
}

/* sets the dataset hash for the given rank and dataset id */
int scr_filemap_set_dataset(scr_filemap* map, int dset, int rank, scr_hash* hash)
{
  /* set indicies and get hash reference */
  scr_hash* rd = scr_filemap_set_rd(map, dset, rank);

  /* set the DATA value under the RANK/DSET hash */
  scr_hash_unset(rd, SCR_FILEMAP_KEY_DATA);
  scr_hash* desc = scr_hash_new();
  scr_hash_merge(desc, hash);
  scr_hash_set(rd, SCR_FILEMAP_KEY_DATA, desc);

  return SCR_SUCCESS;
}

/* copies the dataset hash for the given rank and dataset id into hash */
int scr_filemap_get_dataset(const scr_filemap* map, int dset, int rank, scr_hash* hash)
{
  /* get RANK/CKPT hash */
  scr_hash* rd = scr_filemap_get_rd(map, dset, rank);

  /* get the REDDESC value under the RANK/DSET hash */
  scr_hash* desc = scr_hash_get(rd, SCR_FILEMAP_KEY_DATA);
  if (desc != NULL) {
    scr_hash_merge(hash, desc);
    return SCR_SUCCESS;
  }

  return SCR_FAILURE; 
}

/* unset the dataset hash for the given rank and dataset id */
int scr_filemap_unset_dataset(scr_filemap* map, int dset, int rank)
{
  /* unset DATA value */
  scr_hash* rd = scr_filemap_get_rd(map, dset, rank);
  scr_hash_unset(rd, SCR_FILEMAP_KEY_DATA);

  /* unset RANK/DSET and DSET/RANK indicies if the hash is empty */
  scr_filemap_unset_if_empty(map, dset, rank);

  return SCR_SUCCESS;
}

/* set number of files to expect for a given rank in a given dataset id */
int scr_filemap_set_expected_files(scr_filemap* map, int dset, int rank, int expect)
{
  /* set indicies and get hash reference */
  scr_hash* rd = scr_filemap_set_rd(map, dset, rank);

  /* set the FILES value under the RANK/DSET hash */
  scr_hash_util_set_int(rd, SCR_FILEMAP_KEY_FILES, expect);

  return SCR_SUCCESS;
}

/* return the number of expected files in the hash for a given dataset id and rank */
int scr_filemap_get_expected_files(const scr_filemap* map, int dset, int rank)
{
  int num = -1;
  scr_hash* rd = scr_filemap_get_rd(map, dset, rank);
  scr_hash_util_get_int(rd, SCR_FILEMAP_KEY_FILES, &num);
  return num;
}

/* unset number of files to expect for a given rank in a given dataset id */
int scr_filemap_unset_expected_files(scr_filemap* map, int dset, int rank)
{
  /* unset FILES value */
  scr_hash* rd = scr_filemap_get_rd(map, dset, rank);
  scr_hash_unset(rd, SCR_FILEMAP_KEY_FILES);

  /* unset RANK/DSET and DSET/RANK indicies if the hash is empty */
  scr_filemap_unset_if_empty(map, dset, rank);

  return SCR_SUCCESS;
}

/* sets metadata for file */
int scr_filemap_set_meta(scr_filemap* map, int dset, int rank, const char* file, const scr_meta* meta)
{
  /* get RANK/DSET/FILE hash */
  scr_hash* rdf = scr_filemap_get_rdf(map, dset, rank, file);

  /* add metadata */
  if (rdf != NULL) {
    scr_hash_unset(rdf, SCR_FILEMAP_KEY_META);
    scr_meta* meta_copy = scr_meta_new();
    scr_meta_copy(meta_copy, meta);
    scr_hash_set(rdf, SCR_FILEMAP_KEY_META, meta_copy);
    return SCR_SUCCESS;
  }

  return SCR_FAILURE;
}

/* gets metadata for file */
int scr_filemap_get_meta(const scr_filemap* map, int dset, int rank, const char* file, scr_meta* meta)
{
  /* get RANK/DSET/FILE hash */
  scr_hash* rdf = scr_filemap_get_rdf(map, dset, rank, file);

  /* copy metadata if it is set */
  scr_meta* meta_copy = scr_hash_get(rdf, SCR_FILEMAP_KEY_META);
  if (meta_copy != NULL) {
    scr_meta_copy(meta, meta_copy);
    return SCR_SUCCESS;
  }

  return SCR_FAILURE;
}

/* unsets metadata for file */
int scr_filemap_unset_meta(scr_filemap* map, int dset, int rank, const char* file)
{
  /* set indicies and get hash reference */
  scr_hash* rdf = scr_filemap_get_rdf(map, dset, rank, file);

  /* unset metadata */
  scr_hash_unset(rdf, SCR_FILEMAP_KEY_META);

  return SCR_SUCCESS;
}

/* remove all associations for a given rank in a given dataset */
int scr_filemap_remove_rank_by_dataset(scr_filemap* map, int dset, int rank)
{
  /* remove dataset from the RANK/DSET index, and remove RANK if that was the last item */
  scr_hash* r = scr_filemap_get_r(map, rank);
  scr_hash_unset_kv_int(r, SCR_FILEMAP_KEY_DSET, dset);
  if (scr_hash_size(r) == 0) {
    scr_hash_unset_kv_int(map, SCR_FILEMAP_KEY_RANK, rank);
  }

  /* remove rank from the DSET/RANK index, and remove DSET if that was the last item */
  scr_hash* d = scr_filemap_get_d(map, dset);
  scr_hash_unset_kv_int(d, SCR_FILEMAP_KEY_RANK, rank);
  if (scr_hash_size(d) == 0) {
    scr_hash_unset_kv_int(map, SCR_FILEMAP_KEY_DSET, dset);
  }

  return SCR_SUCCESS;
}

/* remove all associations for a given rank */
int scr_filemap_remove_rank(scr_filemap* map, int rank)
{
  /* iterate over and remove every dataset this rank has */
  scr_hash_elem* dset_elem = scr_filemap_first_dataset_by_rank(map, rank);
  while (dset_elem != NULL) {
    /* get the current dataset id */
    int dset = scr_hash_elem_key_int(dset_elem);

    /* get pointer to the next dataset, since we will remove the current one from the list */
    dset_elem = scr_hash_elem_next(dset_elem);

    /* remove the rank for this dataset */
    scr_filemap_remove_rank_by_dataset(map, dset, rank);
  }
  return SCR_SUCCESS;
}

/* remove all associations for a given dataset */
int scr_filemap_remove_dataset(scr_filemap* map, int dset)
{
  /* iterate over and remove every rank this dataset has */
  scr_hash_elem* rank_elem = scr_filemap_first_rank_by_dataset(map, dset);
  while (rank_elem != NULL) {
    /* get the current rank */
    int rank = scr_hash_elem_key_int(rank_elem);

    /* get pointer to the next rank, since we will remove the current one from the list */
    rank_elem = scr_hash_elem_next(rank_elem);

    /* remove the rank for this dataset */
    scr_filemap_remove_rank_by_dataset(map, dset, rank);
  }
  return SCR_SUCCESS;
}

/* clear the filemap completely */
int scr_filemap_clear(scr_filemap* map)
{
  return scr_hash_unset_all(map);
}

/* returns true if we have a hash for specified rank */
int scr_filemap_have_rank(const scr_filemap* map, int rank)
{
  scr_hash* hash = scr_filemap_get_r(map, rank);
  return (hash != NULL);
}

/* returns true if we have a hash for specified rank for the given dataset */
int scr_filemap_have_rank_by_dataset(const scr_filemap* map, int dset, int rank)
{
  scr_hash* hash = scr_filemap_get_rd(map, dset, rank);
  return (hash != NULL);
}

/* returns the latest dataset id (largest int) in given map */
int scr_filemap_latest_dataset(const scr_filemap* map)
{
  /* initialize with a value indicating that we have no datasets */
  int dset = -1;

  /* now scan through each dataset and find the largest id */
  scr_hash* hash = scr_filemap_get_dh(map);
  if (hash != NULL) {
    scr_hash_elem* elem;
    for (elem = scr_hash_elem_first(hash);
         elem != NULL;
         elem = scr_hash_elem_next(elem))
    {
      int d = scr_hash_elem_key_int(elem);
      if (d > dset) {
        dset = d;
      }
    }
  }
  return dset;
}

/* returns the oldest dataset id (smallest int larger than younger_than) in given map */
int scr_filemap_oldest_dataset(const scr_filemap* map, int younger_than)
{
  /* initialize our oldest dataset id to be the same as the latest dataset id */
  int dset = scr_filemap_latest_dataset(map);

  /* now scan through each dataset and find the smallest id that is larger than younger_than */
  scr_hash* hash = scr_filemap_get_dh(map);
  if (hash != NULL) {
    scr_hash_elem* elem;
    for (elem = scr_hash_elem_first(hash);
         elem != NULL;
         elem = scr_hash_elem_next(elem))
    {
      int d = scr_hash_elem_key_int(elem);
      if (d > younger_than && d < dset) {
        dset = d;
      }
    }
  }
  return dset;
}

/* given a filemap, return a list of ranks */
/* TODO: must free ranks list when done with it */
int scr_filemap_list_ranks(const scr_filemap* map, int* n, int** v)
{
  scr_hash* rh = scr_filemap_get_rh(map);
  scr_hash_list_int(rh, n, v);
  return SCR_SUCCESS;
}

/* given a filemap, return a list of datasets */
int scr_filemap_list_datasets(const scr_filemap* map, int* n, int** v)
{
  scr_hash* dh = scr_filemap_get_dh(map);
  scr_hash_list_int(dh, n, v);
  return SCR_SUCCESS;
}

/* given a filemap and a dataset, return a list of ranks */
/* TODO: must free datasets list when done with it */
int scr_filemap_list_ranks_by_dataset(const scr_filemap* map, int dset, int* n, int** v)
{
  scr_hash* d = scr_filemap_get_d(map, dset);
  scr_hash* rh = scr_filemap_get_rh(d);
  scr_hash_list_int(rh, n, v);
  return SCR_SUCCESS;
}

/* given a filemap, a dataset id, and a rank, return the number of files and a list of the filenames */
int scr_filemap_list_files(const scr_filemap* map, int dset, int rank, int* n, char*** v)
{
  /* assume there aren't any matching files */
  *n = 0;
  *v = NULL;

  /* get rank element */
  scr_hash* fh = scr_filemap_get_fh(map, dset, rank);
  int count = scr_hash_size(fh);
  if (count == 0) {
    return SCR_SUCCESS;
  }

  /* now allocate array of pointers to the filenames */
  char** list = (char**) malloc(count * sizeof(char*));
  if (list == NULL) {
    scr_err("Failed to allocate filename list for dataset id %d and rank %d at %s:%d",
            dset, rank, __FILE__, __LINE__);
    exit(1);
  }

  /* record pointer values in array */
  count = 0;
  scr_hash_elem* file;
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
scr_hash_elem* scr_filemap_first_rank(const scr_filemap* map)
{
  scr_hash* rh = scr_filemap_get_rh(map);
  scr_hash_elem* elem = scr_hash_elem_first(rh);
  return elem;
}

/* given a filemap, return a hash elem pointer to the first rank for a given dataset */
scr_hash_elem* scr_filemap_first_rank_by_dataset(const scr_filemap* map, int dset)
{
  scr_hash* d  = scr_filemap_get_d(map, dset);
  scr_hash* rh = scr_filemap_get_rh(d);
  scr_hash_elem* elem = scr_hash_elem_first(rh);
  return elem;
}

/* given a filemap, return a hash elem pointer to the first dataset */
scr_hash_elem* scr_filemap_first_dataset(const scr_filemap* map)
{
  scr_hash* dh = scr_filemap_get_dh(map);
  scr_hash_elem* elem = scr_hash_elem_first(dh);
  return elem;
}

/* given a filemap, return a hash elem pointer to the first dataset for a given rank */
scr_hash_elem* scr_filemap_first_dataset_by_rank(const scr_filemap* map, int rank)
{
  scr_hash* r  = scr_filemap_get_r(map, rank);
  scr_hash* dh = scr_filemap_get_dh(r);
  scr_hash_elem* elem = scr_hash_elem_first(dh);
  return elem;
}

/* given a filemap, a dataset id, and a rank, return a hash elem pointer to the first file */
scr_hash_elem* scr_filemap_first_file(const scr_filemap* map, int dset, int rank)
{
  scr_hash* fh = scr_filemap_get_fh(map, dset, rank);
  scr_hash_elem* elem = scr_hash_elem_first(fh);
  return elem;
}

/* return the number of ranks in the hash */
int scr_filemap_num_ranks(const scr_filemap* map)
{
  scr_hash* rh = scr_filemap_get_rh(map);
  int size = scr_hash_size(rh);
  return size;
}

/* return the number of ranks in the hash for a given dataset id */
int scr_filemap_num_ranks_by_dataset(const scr_filemap* map, int dset)
{
  scr_hash* d  = scr_filemap_get_d(map, dset);
  scr_hash* rh = scr_filemap_get_rh(d);
  int size = scr_hash_size(rh);
  return size;
}

/* return the number of datasets in the hash */
int scr_filemap_num_datasets(const scr_filemap* map)
{
  scr_hash* dh = scr_filemap_get_dh(map);
  int size = scr_hash_size(dh);
  return size;
}

/* return the number of files in the hash for a given dataset id and rank */
int scr_filemap_num_files(const scr_filemap* map, int dset, int rank)
{
  scr_hash* fh = scr_filemap_get_fh(map, dset, rank);
  int size = scr_hash_size(fh);
  return size;
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
int scr_filemap_delete(scr_filemap** ptr_map)
{
  scr_hash_delete(ptr_map);
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
  scr_hash* r = scr_filemap_get_r(map, rank);
  if (r != NULL) {
    /* create a rank hash for this rank in the new map */
    scr_hash* new_r = scr_hash_set_kv_int(new_map, SCR_FILEMAP_KEY_RANK, rank);

    /* copy the rank hash for the given rank to the new map */
    scr_hash_merge(new_r, r);

    /* for each dataset we have for this rank, set the DSET/RANK index the new map */
    scr_hash_elem* dset_elem;
    for (dset_elem = scr_filemap_first_dataset_by_rank(map, rank);
         dset_elem != NULL;
         dset_elem = scr_hash_elem_next(dset_elem))
    {
      int dset = scr_hash_elem_key_int(dset_elem);
      scr_filemap_set_rd(new_map, dset, rank);
    }
  }

  /* remove the rank from the current map */
  scr_filemap_remove_rank(map, rank);

  /* return the new map */
  return new_map;
}

/* reads specified file and fills in filemap structure */
int scr_filemap_read(const scr_path* path_file, scr_filemap* map)
{
  /* check that we have a map pointer and a hash within the map */
  if (map == NULL) {
    return SCR_FAILURE;
  }

  /* assume we'll fail */
  int rc = SCR_FAILURE;

  /* get file name */
  char* file = scr_path_strdup(path_file);

  /* can't read file, return error (special case so as not to print error message below) */
  if (scr_file_is_readable(file) != SCR_SUCCESS) {
    goto cleanup;
  }

  /* ok, now try to read the file */
  if (scr_hash_read(file, map) != SCR_SUCCESS) {
    scr_err("Reading filemap %s @ %s:%d",
      file, __FILE__, __LINE__
    );
    goto cleanup;
  }

  /* TODO: check that file count for each rank matches expected count */

  /* success if we make it this far */
  rc = SCR_SUCCESS;

cleanup:
  /* free file name string */
  scr_free(&file);

  return rc;
}

/* writes given filemap to specified file */
int scr_filemap_write(const scr_path* file, const scr_filemap* map)
{
  /* check that we have a map pointer */
  if (map == NULL) {
    return SCR_FAILURE;
  }

  /* write out the hash */
  if (scr_hash_write_path(file, map) != SCR_SUCCESS) {
    char path_err[SCR_MAX_FILENAME];
    scr_path_strcpy(path_err, sizeof(path_err), file);
    scr_err("Writing filemap %s @ %s:%d",
      path_err, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  return SCR_SUCCESS;
}
