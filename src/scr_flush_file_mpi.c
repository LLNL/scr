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

#include "scr_globals.h"
#include "scr_flush_nompi.h"

#include "kvtree.h"
#include "kvtree_util.h"

/*
=========================================
Flush file functions
=========================================
*/

/* returns true if the given dataset id needs to be flushed */
int scr_flush_file_need_flush(int id)
{
  int need_flush = 0;

  /* just have rank 0 read the file */
  if (scr_my_rank_world == 0) {
    /* read the flush file */
    kvtree* hash = kvtree_new();
    kvtree_read_path(scr_flush_file, hash);

    /* if we have the dataset in cache, but not on the parallel file system,
     * then it needs to be flushed */
    kvtree* dset_hash = kvtree_get_kv_int(hash, SCR_FLUSH_KEY_DATASET, id);
    kvtree* in_cache  = kvtree_get_kv(dset_hash, SCR_FLUSH_KEY_LOCATION, SCR_FLUSH_KEY_LOCATION_CACHE);
    kvtree* in_pfs    = kvtree_get_kv(dset_hash, SCR_FLUSH_KEY_LOCATION, SCR_FLUSH_KEY_LOCATION_PFS);
    if (in_cache != NULL && in_pfs == NULL) {
      need_flush = 1;
    }

    /* free the hash object */
    kvtree_delete(&hash);
  }

  /* broadcast decision from rank 0 */
  MPI_Bcast(&need_flush, 1, MPI_INT, 0, scr_comm_world);

  return need_flush;
}

/* checks whether the specified dataset id is currently being flushed */
int scr_flush_file_is_flushing(int id)
{
  /* assume we are not flushing this checkpoint */
  int is_flushing = 0;

  /* only rank 0 tests the file */
  if (scr_my_rank_world == 0) {
    /* read flush file into hash */
    kvtree* hash = kvtree_new();
    kvtree_read_path(scr_flush_file, hash);

    /* attempt to look up the FLUSHING state for this checkpoint */
    kvtree* dset_hash = kvtree_get_kv_int(hash, SCR_FLUSH_KEY_DATASET, id);
    kvtree* flushing_hash = kvtree_get_kv(dset_hash, SCR_FLUSH_KEY_LOCATION, SCR_FLUSH_KEY_LOCATION_FLUSHING);
    if (flushing_hash != NULL) {
      is_flushing = 1;
    }

    /* delete the hash */
    kvtree_delete(&hash);
  }

  /* broadcast decision from rank 0 */
  MPI_Bcast(&is_flushing, 1, MPI_INT, 0, scr_comm_world);

  return is_flushing;
}

/* removes entries in flush file for given dataset id */
int scr_flush_file_dataset_remove(int id)
{
  /* only rank 0 needs to write the file */
  if (scr_my_rank_world == 0) {
    scr_flush_file_dataset_remove_with_path(id, scr_flush_file);
  }
  return SCR_SUCCESS;
}

/* adds a location for the specified dataset id to the flush file */
int scr_flush_file_location_set(int id, const char* location)
{
  /* only rank 0 updates the file */
  if (scr_my_rank_world == 0) {
    /* read the flush file into hash */
    kvtree* hash = kvtree_new();
    kvtree_read_path(scr_flush_file, hash);

    /* set the location for this dataset */
    kvtree* dset_hash = kvtree_set_kv_int(hash, SCR_FLUSH_KEY_DATASET, id);
    kvtree_set_kv(dset_hash, SCR_FLUSH_KEY_LOCATION, location);

    /* write the hash back to the flush file */
    kvtree_write_path(scr_flush_file, hash);

    /* delete the hash */
    kvtree_delete(&hash);
  }
  return SCR_SUCCESS;
}

/* returns SCR_SUCCESS if specified dataset id is at specified location */
int scr_flush_file_location_test(int id, const char* location)
{
  /* only rank 0 checks the status, bcasts the results to everyone else */
  int at_location = 0;
  if (scr_my_rank_world == 0) {
    /* read the flush file into hash */
    kvtree* hash = kvtree_new();
    kvtree_read_path(scr_flush_file, hash);

    /* check the location for this dataset */
    kvtree* dset_hash = kvtree_get_kv_int(hash, SCR_FLUSH_KEY_DATASET, id);
    kvtree* value     = kvtree_get_kv(dset_hash, SCR_FLUSH_KEY_LOCATION, location);
    if (value != NULL) {
      at_location = 1;
    }

    /* delete the hash */
    kvtree_delete(&hash);
  }
  MPI_Bcast(&at_location, 1, MPI_INT, 0, scr_comm_world);

  if (! at_location) {
    return SCR_FAILURE;
  }
  return SCR_SUCCESS;
}

/* removes a location for the specified dataset id from the flush file */
int scr_flush_file_location_unset(int id, const char* location)
{
  /* only rank 0 updates the file */
  if (scr_my_rank_world == 0) {
    char* scr_flush_path = spath_strdup(scr_flush_file);
    scr_flush_file_location_unset_with_path(id, location, scr_flush_path);
    scr_free(&scr_flush_path);
  }
  return SCR_SUCCESS;
}

/* create an entry in the flush file for a dataset for scavenge,
 * including name, location, and flags */
int scr_flush_file_new_entry(int id, const char* name, const scr_dataset* dataset, const char* location, int ckpt, int output)
{
  /* only rank 0 updates the file */
  if (scr_my_rank_world == 0) {
    /* read the flush file into hash */
    kvtree* hash = kvtree_new();
    kvtree_read_path(scr_flush_file, hash);

    /* set the name, location, and flags for this dataset */
    kvtree* dset_hash = kvtree_set_kv_int(hash, SCR_FLUSH_KEY_DATASET, id);
    kvtree_util_set_str(dset_hash, SCR_FLUSH_KEY_NAME, name);
    kvtree_util_set_str(dset_hash, SCR_FLUSH_KEY_LOCATION, location);
    if (ckpt) {
      kvtree_util_set_int(dset_hash, SCR_FLUSH_KEY_CKPT, ckpt);
    }
    if (output) {
      kvtree_util_set_int(dset_hash, SCR_FLUSH_KEY_OUTPUT, output);
    }

    /* record metadata for dataset */
    /* TODO: this feels hacky since it breaks abstraction of scr_dataset type */
    kvtree* dataset_copy = kvtree_new();
    kvtree_merge(dataset_copy, dataset);
    kvtree_set(dset_hash, SCR_FLUSH_KEY_DSETDESC, dataset_copy);

    /* write the hash back to the flush file */
    kvtree_write_path(scr_flush_file, hash);

    /* delete the hash */
    kvtree_delete(&hash);
  }
  return SCR_SUCCESS;
}

