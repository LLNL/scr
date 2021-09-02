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

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>

#include "mpi.h"

#include "scr_globals.h"
#include "scr.h"
#include "scr_io.h"
#include "scr_err.h"
#include "scr_util.h"
#include "scr_cache_index.h"

#include "spath.h"
#include "kvtree.h"
#include "kvtree_util.h"

#define SCR_CINDEX_KEY_CURRENT ("CURRENT")
#define SCR_CINDEX_KEY_DSET    ("DSET")
#define SCR_CINDEX_KEY_DATA      ("DSETDESC")
#define SCR_CINDEX_KEY_PATH      ("PATH")
#define SCR_CINDEX_KEY_BYPASS    ("BYPASS")

/* returns the DSET hash */
static kvtree* scr_cache_index_get_dh(const kvtree* h)
{
  kvtree* dh = kvtree_get(h, SCR_CINDEX_KEY_DSET);
  return dh;
}

/* returns the hash associated with a particular dataset */
static kvtree* scr_cache_index_get_d(const kvtree* h, int dset)
{
  kvtree* d = kvtree_get_kv_int(h, SCR_CINDEX_KEY_DSET, dset);
  return d;
}

/* creates and returns a hash under DSET */
static kvtree* scr_cache_index_set_d(scr_cache_index* cindex, int dset)
{
  /* set DSET index */
  kvtree* d = kvtree_set_kv_int(cindex, SCR_CINDEX_KEY_DSET, dset);
  return d;
}

/* unset DSET index if the index for this dataset is empty */
static int scr_cache_index_unset_if_empty(scr_cache_index* cindex, int dset)
{
  /* get hash references for this dataset */
  kvtree* d  = scr_cache_index_get_d(cindex, dset);

  /* if there is nothing left under this dataset, unset the dataset */
  if (kvtree_size(d) == 0) {
    kvtree_unset_kv_int(cindex, SCR_CINDEX_KEY_DSET, dset);
  }

  return SCR_SUCCESS;
}

/* set the CURRENT name, used to rememeber if we already proccessed
 * a SCR_CURRENT name a user may have provided to set the current value,
 * we ignore that request in later runs and use this marker to remember */
int scr_cache_index_set_current(const kvtree* h, const char* current)
{
  kvtree_util_set_str((kvtree*) h, SCR_CINDEX_KEY_CURRENT, current);
  return SCR_SUCCESS;
}

/* returns the CURRENT name */
int scr_cache_index_get_current(const kvtree* h, char** current)
{
  int kvtree_rc = kvtree_util_get_str(h, SCR_CINDEX_KEY_CURRENT, current);
  int rc = (kvtree_rc == KVTREE_SUCCESS) ? SCR_SUCCESS : SCR_FAILURE;
  return rc;
}

/* sets the dataset hash for the given dataset id */
int scr_cache_index_set_dataset(scr_cache_index* cindex, int dset, kvtree* hash)
{
  /* set indicies and get hash reference */
  kvtree* d = scr_cache_index_set_d(cindex, dset);

  /* set the DATA value under the RANK/DSET hash */
  kvtree_unset(d, SCR_CINDEX_KEY_DATA);
  kvtree* desc = kvtree_new();
  kvtree_merge(desc, hash);
  kvtree_set(d, SCR_CINDEX_KEY_DATA, desc);

  return SCR_SUCCESS;
}

/* copies the dataset hash for the given dataset id into hash */
int scr_cache_index_get_dataset(const scr_cache_index* cindex, int dset, kvtree* hash)
{
  /* get RANK/CKPT hash */
  kvtree* d = scr_cache_index_get_d(cindex, dset);

  /* get the REDDESC value under the RANK/DSET hash */
  kvtree* desc = kvtree_get(d, SCR_CINDEX_KEY_DATA);
  if (desc != NULL) {
    kvtree_merge(hash, desc);
    return SCR_SUCCESS;
  }

  return SCR_FAILURE; 
}

/* unset the dataset hash for the given dataset id */
int scr_cache_index_unset_dataset(scr_cache_index* cindex, int dset)
{
  /* unset DATA value */
  kvtree* d = scr_cache_index_get_d(cindex, dset);
  kvtree_unset(d, SCR_CINDEX_KEY_DATA);

  /* unset DSET if the hash is empty */
  scr_cache_index_unset_if_empty(cindex, dset);

  return SCR_SUCCESS;
}

/* record directory where dataset is stored */
int scr_cache_index_set_dir(scr_cache_index* cindex, int dset, const char* path)
{
  /* set indicies and get hash reference */
  kvtree* d = scr_cache_index_set_d(cindex, dset);

  /* set the DATA value under the RANK/DSET hash */
  kvtree_util_set_str(d, SCR_CINDEX_KEY_PATH, path);

  return SCR_SUCCESS;
}

/* returns pointer to directory for dataset */
int scr_cache_index_get_dir(const scr_cache_index* cindex, int dset, char** path)
{
  /* get RANK/CKPT hash */
  kvtree* d = scr_cache_index_get_d(cindex, dset);

  /* get the REDDESC value under the RANK/DSET hash */
  if (kvtree_util_get_str(d, SCR_CINDEX_KEY_PATH, path) == KVTREE_SUCCESS) {
    return SCR_SUCCESS;
  }

  return SCR_FAILURE; 
}

/* unset the dataset hash for the given dataset id */
int scr_cache_index_unset_dir(scr_cache_index* cindex, int dset)
{
  /* unset DATA value */
  kvtree* d = scr_cache_index_get_d(cindex, dset);
  kvtree_unset(d, SCR_CINDEX_KEY_PATH);

  /* unset DSET if the hash is empty */
  scr_cache_index_unset_if_empty(cindex, dset);

  return SCR_SUCCESS;
}

/* mark dataset as cache bypass (read/write direct to prefix dir) */
int scr_cache_index_set_bypass(scr_cache_index* cindex, int dset, int bypass)
{
  /* set indicies and get hash reference */
  kvtree* d = scr_cache_index_set_d(cindex, dset);

  /* set the DATA value under the RANK/DSET hash */
  kvtree_util_set_int(d, SCR_CINDEX_KEY_BYPASS, bypass);

  return SCR_SUCCESS;
}

/* get value of bypass flag for dataset */
int scr_cache_index_get_bypass(const scr_cache_index* cindex, int dset, int* bypass)
{
  /* assume the dataset has not been marked as bypass */
  *bypass = 0;

  /* get RANK/CKPT hash */
  kvtree* d = scr_cache_index_get_d(cindex, dset);

  /* get the REDDESC value under the RANK/DSET hash */
  if (kvtree_util_get_int(d, SCR_CINDEX_KEY_BYPASS, bypass) == KVTREE_SUCCESS) {
    return SCR_SUCCESS;
  }

  return SCR_FAILURE; 
}

/* remove all associations for a given dataset */
int scr_cache_index_remove_dataset(scr_cache_index* cindex, int dset)
{
  kvtree_unset_kv_int(cindex, SCR_CINDEX_KEY_DSET, dset);
  return SCR_SUCCESS;
}

/* clear the cache index completely */
int scr_cache_index_clear(scr_cache_index* cindex)
{
  return kvtree_unset_all(cindex);
}

/* returns the latest dataset id (largest int) in given index */
int scr_cache_index_latest_dataset(const scr_cache_index* cindex)
{
  /* initialize with a value indicating that we have no datasets */
  int dset = -1;

  /* now scan through each dataset and find the largest id */
  kvtree* hash = scr_cache_index_get_dh(cindex);
  if (hash != NULL) {
    kvtree_elem* elem;
    for (elem = kvtree_elem_first(hash);
         elem != NULL;
         elem = kvtree_elem_next(elem))
    {
      int d = kvtree_elem_key_int(elem);
      if (d > dset) {
        dset = d;
      }
    }
  }
  return dset;
}

/* given a cache index, return a list of datasets */
int scr_cache_index_list_datasets(const scr_cache_index* cindex, int* n, int** v)
{
  kvtree* dh = scr_cache_index_get_dh(cindex);
  kvtree_list_int(dh, n, v);
  return SCR_SUCCESS;
}

/* given a cache index, return a hash elem pointer to the first dataset */
kvtree_elem* scr_cache_index_first_dataset(const scr_cache_index* cindex)
{
  kvtree* dh = scr_cache_index_get_dh(cindex);
  kvtree_elem* elem = kvtree_elem_first(dh);
  return elem;
}

/* return the number of datasets in the hash */
int scr_cache_index_num_datasets(const scr_cache_index* cindex)
{
  kvtree* dh = scr_cache_index_get_dh(cindex);
  int size = kvtree_size(dh);
  return size;
}

/* allocate a new cache index structure and return it */
scr_cache_index* scr_cache_index_new()
{
  scr_cache_index* cindex = kvtree_new();
  if (cindex == NULL) {
    scr_err("Failed to allocate cache index @ %s:%d", __FILE__, __LINE__);
  }
  return cindex;
}

/* free memory resources assocaited with cache index */
int scr_cache_index_delete(scr_cache_index** ptr_cindex)
{
  kvtree_delete(ptr_cindex);
  return SCR_SUCCESS;
}

/* adds cindex2 into cindex1 */
int scr_cache_index_merge(scr_cache_index* cindex1, scr_cache_index* cindex2)
{
  kvtree_merge(cindex1, cindex2);
  return SCR_SUCCESS;
}
