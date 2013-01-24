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

/*
=========================================
Flush file functions
=========================================
*/

/* returns true if the given dataset id needs to be flushed */
int scr_bool_need_flush(int id)
{
  int need_flush = 0;

  /* just have rank 0 read the file */
  if (scr_my_rank_world == 0) {
    /* read the flush file */
    scr_hash* hash = scr_hash_new();
    scr_hash_read_path(scr_flush_file, hash);

    /* if we have the dataset in cache, but not on the parallel file system,
     * then it needs to be flushed */
    scr_hash* dset_hash = scr_hash_get_kv_int(hash, SCR_FLUSH_KEY_DATASET, id);
    scr_hash* in_cache = scr_hash_get_kv(dset_hash, SCR_FLUSH_KEY_LOCATION, SCR_FLUSH_KEY_LOCATION_CACHE);
    scr_hash* in_pfs   = scr_hash_get_kv(dset_hash, SCR_FLUSH_KEY_LOCATION, SCR_FLUSH_KEY_LOCATION_PFS);
    if (in_cache != NULL && in_pfs == NULL) {
      need_flush = 1;
    }

    /* free the hash object */
    scr_hash_delete(&hash);
  }
  MPI_Bcast(&need_flush, 1, MPI_INT, 0, scr_comm_world);

  return need_flush;
}

/* checks whether the specified dataset id is currently being flushed */
int scr_bool_is_flushing(int id)
{
  /* assume we are not flushing this checkpoint */
  int is_flushing = 0;

  /* only rank 0 tests the file */
  if (scr_my_rank_world == 0) {
    /* read flush file into hash */
    scr_hash* hash = scr_hash_new();
    scr_hash_read_path(scr_flush_file, hash);

    /* attempt to look up the FLUSHING state for this checkpoint */
    scr_hash* dset_hash = scr_hash_get_kv_int(hash, SCR_FLUSH_KEY_DATASET, id);
    scr_hash* flushing_hash = scr_hash_get_kv(dset_hash, SCR_FLUSH_KEY_LOCATION, SCR_FLUSH_KEY_LOCATION_FLUSHING);
    if (flushing_hash != NULL) {
      is_flushing = 1;
    }

    /* delete the hash */
    scr_hash_delete(&hash);
  }
  MPI_Bcast(&is_flushing, 1, MPI_INT, 0, scr_comm_world);

  return is_flushing;
}

/* removes entries in flush file for given dataset id */
int scr_flush_file_dataset_remove(int id)
{
  /* only rank 0 needs to write the file */
  if (scr_my_rank_world == 0) {
    /* read the flush file into hash */
    scr_hash* hash = scr_hash_new();
    scr_hash_read_path(scr_flush_file, hash);

    /* delete this dataset id from the flush file */
    scr_hash_unset_kv_int(hash, SCR_FLUSH_KEY_DATASET, id);

    /* write the hash back to the flush file */
    scr_hash_write_path(scr_flush_file, hash);

    /* delete the hash */
    scr_hash_delete(&hash);
  }
  return SCR_SUCCESS;
}

/* adds a location for the specified dataset id to the flush file */
int scr_flush_file_location_set(int id, const char* location)
{
  /* only rank 0 updates the file */
  if (scr_my_rank_world == 0) {
    /* read the flush file into hash */
    scr_hash* hash = scr_hash_new();
    scr_hash_read_path(scr_flush_file, hash);

    /* set the location for this dataset */
    scr_hash* dset_hash = scr_hash_set_kv_int(hash, SCR_FLUSH_KEY_DATASET, id);
    scr_hash_set_kv(dset_hash, SCR_FLUSH_KEY_LOCATION, location);

    /* write the hash back to the flush file */
    scr_hash_write_path(scr_flush_file, hash);

    /* delete the hash */
    scr_hash_delete(&hash);
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
    scr_hash* hash = scr_hash_new();
    scr_hash_read_path(scr_flush_file, hash);

    /* check the location for this dataset */
    scr_hash* dset_hash = scr_hash_get_kv_int(hash, SCR_FLUSH_KEY_DATASET, id);
    scr_hash* value     = scr_hash_get_kv(dset_hash, SCR_FLUSH_KEY_LOCATION, location);
    if (value != NULL) {
      at_location = 1;
    }

    /* delete the hash */
    scr_hash_delete(&hash);
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
    /* read the flush file into hash */
    scr_hash* hash = scr_hash_new();
    scr_hash_read_path(scr_flush_file, hash);

    /* unset the location for this dataset */
    scr_hash* dset_hash = scr_hash_get_kv_int(hash, SCR_FLUSH_KEY_DATASET, id);
    scr_hash_unset_kv(dset_hash, SCR_FLUSH_KEY_LOCATION, location);

    /* write the hash back to the flush file */
    scr_hash_write_path(scr_flush_file, hash);

    /* delete the hash */
    scr_hash_delete(&hash);
  }
  return SCR_SUCCESS;
}

/* we track the subdirectory name within the prefix directory
 * so that we can specify where to create the summary file in scavenge */
int scr_flush_file_subdir_set(int id, const char* subdir)
{
  /* only rank 0 updates the file */
  if (scr_my_rank_world == 0) {
    /* read the flush file into hash */
    scr_hash* hash = scr_hash_new();
    scr_hash_read_path(scr_flush_file, hash);

    /* set the location for this dataset */
    scr_hash* dset_hash = scr_hash_set_kv_int(hash, SCR_FLUSH_KEY_DATASET, id);
    scr_hash_set_kv(dset_hash, SCR_FLUSH_KEY_DIRECTORY, subdir);

    /* write the hash back to the flush file */
    scr_hash_write_path(scr_flush_file, hash);

    /* delete the hash */
    scr_hash_delete(&hash);
  }
  return SCR_SUCCESS;
}


