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

/* All rights reserved. This program and the accompanying materials
 * are made available under the terms of the BSD-3 license which accompanies this
 * distribution in LICENSE.TXT
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the BSD-3  License in
 * LICENSE.TXT for more details.
 *
 * GOVERNMENT LICENSE RIGHTS-OPEN SOURCE SOFTWARE
 * The Government's rights to use, modify, reproduce, release, perform,
 * display, or disclose this software are subject to the terms of the BSD-3
 * License as provided in Contract No. B609815.
 * Any reproduction of computer software, computer software documentation, or
 * portions thereof marked with this legend must also reproduce the markings.
 *
 * Author: Christopher Holguin <christopher.a.holguin@intel.com>
 *
 * (C) Copyright 2015-2016 Intel Corporation.
 */

#include "scr_globals.h"

/*
=========================================
Distribute and file rebuild functions
=========================================
*/

/* broadcast dataset hash from smallest rank we can find that has a copy */
static int scr_distribute_datasets(scr_cache_index* cindex, int id)
{
  /* attempt to read dataset from our index */
  int source_rank = scr_ranks_world;
  scr_dataset* dataset = scr_dataset_new();
  if (scr_cache_index_get_dataset(cindex, id, dataset) == SCR_SUCCESS) {
    source_rank = scr_my_rank_world;
  }

  /* identify the smallest rank that has the dataset */
  int min_rank;
  MPI_Allreduce(&source_rank, &min_rank, 1, MPI_INT, MPI_MIN, scr_comm_world);

  /* if there is no rank, return with failure */
  if (min_rank >= scr_ranks_world) {
    scr_dataset_delete(&dataset);
    return SCR_FAILURE;
  }

  /* otherwise, bcast the dataset from the minimum rank */
  if (scr_my_rank_world != min_rank) {
    kvtree_unset_all(dataset);
  }
  kvtree_bcast(dataset, min_rank, scr_comm_world);

  /* attempt to read bypass property from our index */
  source_rank = scr_ranks_world;
  int bypass;
  if (scr_cache_index_get_bypass(cindex, id, &bypass) == SCR_SUCCESS) {
    source_rank = scr_my_rank_world;
  }

  /* identify the smallest rank that has the value */
  MPI_Allreduce(&source_rank, &min_rank, 1, MPI_INT, MPI_MIN, scr_comm_world);

  /* if there is no rank, return with failure */
  if (min_rank >= scr_ranks_world) {
    scr_dataset_delete(&dataset);
    return SCR_FAILURE;
  }

  /* otherwise, bcast the bypass property from the minimum rank */
  MPI_Bcast(&bypass, 1, MPI_INT, min_rank, scr_comm_world);

  /* record the descriptor in our cache index */
  scr_cache_index_set_dataset(cindex, id, dataset);
  scr_cache_index_set_bypass(cindex, id, bypass);
  scr_cache_index_write(scr_cindex_file, cindex);

  /* free off dataset object */
  scr_dataset_delete(&dataset);

  return SCR_SUCCESS;
}

/* broadcast dir from smallest rank and lookup store descriptor we can find that has a copy */
static int scr_distribute_dir(scr_cache_index* cindex, int id, char** hidden_dir)
{
  /* initialize output path to NULL */
  *hidden_dir = NULL;

  /* determine whether we have the cache directory for this dataset */
  char* dir  = NULL;
  char* path = NULL;
  int source_rank = scr_ranks_world;
  if (scr_cache_index_get_dir(cindex, id, &path) == SCR_SUCCESS) {
    /* we've got a copy, make a copy */
    dir = strdup(path);
    source_rank = scr_my_rank_world;
  }

  /* identify the smallest rank that has the dataset */
  int min_rank;
  MPI_Allreduce(&source_rank, &min_rank, 1, MPI_INT, MPI_MIN, scr_comm_world);

  /* if there is no rank, return with failure */
  if (min_rank >= scr_ranks_world) {
    return SCR_FAILURE;
  }

  /* if we're not bcasting the string, free our copy if we have one,
   * we'll get a new copy from the bcast */
  if (scr_my_rank_world != min_rank) {
    scr_free(&dir);
  }

  /* bcast the dataset from the minimum rank */
  scr_str_bcast(&dir, min_rank, scr_comm_world);

  /* record the directory in the cache index */
  scr_cache_index_set_dir(cindex, id, dir);
  scr_cache_index_write(scr_cindex_file, cindex);

  /* lookup store descriptor for this path */
  int store_index = scr_storedescs_index_from_child_path(dir);

  /* if someone fails to find their descriptor give up */
  if (! scr_alltrue(store_index >= 0, scr_comm_world)) {
    scr_free(&dir);
    return SCR_FAILURE;
  }

  /* define hidden directory, which we'll also return to the caller */
  spath* path_scr = spath_from_str(dir);
  spath_append_str(path_scr, ".scr");
  *hidden_dir = spath_strdup(path_scr);
  spath_delete(&path_scr);

  /* get store descriptor */
  scr_storedesc* store = &scr_storedescs[store_index];

  /* create the directory */
  scr_storedesc_dir_create(store, dir);

  /* create the hidden directory */
  scr_storedesc_dir_create(store, *hidden_dir);

  /* free direcotry string */
  scr_free(&dir);

  return SCR_SUCCESS;
}

/* distribute and rebuild files in cache */
int scr_cache_rebuild(scr_cache_index* cindex)
{
  int rc = SCR_FAILURE;

  /* start timer */
  time_t time_t_start;
  double time_start = 0.0;
  if (scr_my_rank_world == 0) {
    time_t_start = scr_log_seconds();
    time_start = MPI_Wtime();
  }

  /* we set this variable to 1 if we actually try to distribute
   * files for a restart */
  int distribute_attempted = 0;

  /* clean any incomplete files from our cache */
  //scr_cache_clean(cindex);

  /* set the current marker to the value held on the lowest rank
   * that has a value */
  int source_rank = scr_ranks_world;
  char* current_name = NULL;
  char* current_tmp;
  if (scr_cache_index_get_current(cindex, &current_tmp) == SCR_SUCCESS) {
    /* we have a current marker, make a copy of its value */
    source_rank = scr_my_rank_world;
    current_name = strdup(current_tmp);
  }

  /* determine min rank that has a value, if any */
  int min_rank;
  MPI_Allreduce(&source_rank, &min_rank, 1, MPI_INT, MPI_MIN, scr_comm_world);

  /* if any rank has a value, bcast value from that rank,
   * and set value consistently on all nodes */
  if (min_rank < scr_ranks_world) {
    /* if we're not bcasting the string, free our copy if we have one,
     * we'll get a new copy from the bcast */
    if (scr_my_rank_world != min_rank) {
      scr_free(&current_name);
    }

    /* bcast the current value from the minimum rank */
    scr_str_bcast(&current_name, min_rank, scr_comm_world);

    /* set current marker in our cache index */
    scr_cache_index_set_current(cindex, current_name);
    scr_cache_index_write(scr_cindex_file, cindex);
  }
  scr_free(&current_name);

  /* get ordered list of datasets we have in our cache */
  int ndsets;
  int* dsets;
  scr_cache_index_list_datasets(cindex, &ndsets, &dsets);

  /* TODO: put dataset selection logic into a function */

  /* TODO: also attempt to recover datasets which we were in the
   * middle of flushing */
  int current_id;
  int dset_index = 0;
  int output_failed_rebuild = 0;
  do {
    /* get the smallest index across all processes (returned in current_id),
     * this also updates our dset_index value if appropriate */
    scr_next_dataset(ndsets, dsets, &dset_index, &current_id);

    /* if we found a dataset, try to distribute and rebuild it */
    if (current_id != -1) {
      /* remember that we made an attempt to distribute at least one dataset */
      distribute_attempted = 1;
      
      /* log the attempt */
      if (scr_my_rank_world == 0) {
        scr_dbg(1, "Attempting to distribute and rebuild dataset %d", current_id);
        if (scr_log_enable) {
          scr_log_event("REBUILD_START", NULL, &current_id, NULL, NULL, NULL);
        }
      }

      /* assume we'll fail to rebuild */
      int rebuild_succeeded = 0;

      /* distribute dataset descriptor for this dataset */
      if (scr_distribute_datasets(cindex, current_id) == SCR_SUCCESS) {
        /* get dataset for this id */
        scr_dataset* dataset = scr_dataset_new();
        scr_cache_index_get_dataset(cindex, current_id, dataset);

        /* get and recreate directory from cindex */
        char* path;
        if (scr_distribute_dir(cindex, current_id, &path) == SCR_SUCCESS) {
          /* rebuild files for this dataset */
          int tmp_rc = scr_reddesc_recover(cindex, current_id, path);
          if (tmp_rc == SCR_SUCCESS) {
            /* rebuild succeeded */
            rebuild_succeeded = 1;

            /* if we have a checkpoint, update dataset and checkpoint counters,
             * however skip this if we failed to rebuild an output set, in this
             * case we'll restart from the checkpoint before the lost output set */
            int is_ckpt = scr_dataset_is_ckpt(dataset);
            if (is_ckpt && !output_failed_rebuild) {
              /* if we rebuild any checkpoint, return success */
              rc = SCR_SUCCESS;

              /* if id of dataset we just rebuilt is newer,
               * update scr_dataset_id */
              if (current_id > scr_dataset_id) {
                scr_dataset_id = current_id;
              }

              /* get checkpoint id for dataset */
              int ckpt_id;
              scr_dataset_get_ckpt(dataset, &ckpt_id);

              /* if checkpoint id of dataset we just rebuilt is newer,
               * update scr_checkpoint_id and scr_ckpt_dset_id */
              if (ckpt_id > scr_checkpoint_id) {
                /* got a more recent checkpoint, update our checkpoint info */
                scr_checkpoint_id = ckpt_id;
                scr_ckpt_dset_id = current_id;
              }
            }

            /* update our flush file to indicate this dataset is in cache */
            scr_flush_file_location_set(current_id, SCR_FLUSH_KEY_LOCATION_CACHE);

            /* TODO: if storing flush file in control directory on each node,
             * if we find any process that has marked the dataset as flushed,
             * marked it as flushed in every flush file */

            /* TODO: would like to restore flushing status to datasets that
             * were in the middle of a flush, but we need to better manage
             * the transfer file to do this, so for now just forget about
             * flushing this dataset */
            scr_flush_file_location_unset(current_id, SCR_FLUSH_KEY_LOCATION_FLUSHING);
          }

          /* free path */
          scr_free(&path);
        }

        /* remember if we fail to rebuild an output set */
        int is_output = scr_dataset_is_output(dataset);
        if (!rebuild_succeeded && is_output) {
          output_failed_rebuild = 1;
        }

        /* free dataset */
        scr_dataset_delete(&dataset);
      } else {
        /* if we failed to distribute dataset info, then we can't know
         * whether this was output or not, so we have to assume it was */
        output_failed_rebuild = 1;
      }

      /* if the distribute or rebuild failed, delete the dataset */
      if (! rebuild_succeeded) {
        /* log that we failed */
        if (scr_my_rank_world == 0) {
          scr_dbg(1, "Failed to rebuild dataset %d", current_id);
          if (scr_log_enable) {
            scr_log_event("REBUILD_FAIL", NULL, &current_id, NULL, NULL, NULL);
          }
        }

        /* TODO: there is a bug here, since scr_cache_delete needs to read
         * the redundancy descriptor from the filemap in order to delete the
         * cache directory, but we may have failed to distribute the reddescs
         * above so not every task has one */

        /* rebuild failed, delete this dataset from cache */
        scr_cache_delete(cindex, current_id);
      } else {
        /* rebuid worked, log success */
        if (scr_my_rank_world == 0) {
          scr_dbg(1, "Rebuilt dataset %d", current_id);
          if (scr_log_enable) {
            scr_log_event("REBUILD_SUCCESS", NULL, &current_id, NULL, NULL, NULL);
          }
        }
      }
    }
  } while (current_id != -1);

  /* free our list of dataset ids */
  scr_free(&dsets);

  /* get an updated list of datasets since we may have rebuilt/deleted some */
  scr_cache_index_list_datasets(cindex, &ndsets, &dsets);

  /* delete all datasets following the most recent checkpoint */
  dset_index = 0;
  do {
    /* get the smallest index across all processes (returned in current_id),
     * this also updates our dset_index value if appropriate */
    scr_next_dataset(ndsets, dsets, &dset_index, &current_id);

    /* if we found a dataset, try to distribute and rebuild it */
    if (current_id != -1 && current_id > scr_ckpt_dset_id) {
      /* rebuild failed, delete this dataset from cache */
      scr_cache_delete(cindex, current_id);
    }
  } while (current_id != -1);

  /* free our list of dataset ids */
  scr_free(&dsets);

  /* stop timer and report performance */
  if (scr_my_rank_world == 0) {
    double time_end = MPI_Wtime();
    double time_diff = time_end - time_start;

    if (distribute_attempted) {
      if (rc == SCR_SUCCESS) {
        scr_dbg(1, "Scalable restart succeeded for checkpoint %d, took %f secs",
          scr_checkpoint_id, time_diff
        );
        if (scr_log_enable) {
          scr_log_event("RESTART_SUCCESS", NULL, &scr_dataset_id, NULL, &time_t_start, &time_diff);
        }
      } else {
        /* scr_checkpoint_id is not defined */
        scr_dbg(1, "Scalable restart failed, took %f secs", time_diff);
        if (scr_log_enable) {
          scr_log_event("RESTART_FAIL", NULL, NULL, NULL, &time_t_start, &time_diff);
        }
      }
    }
  }

  return rc;
}

/* remove any dataset ids from flush file which are not in cache,
 * and add any datasets in cache that are not in the flush file */
int scr_flush_file_rebuild(const scr_cache_index* cindex)
{
  if (scr_my_rank_world == 0) {
    /* read the flush file */
    kvtree* hash = kvtree_new();
    kvtree_read_path(scr_flush_file, hash);

    /* get ordered list of dataset ids in flush file */
    int flush_ndsets;
    int* flush_dsets;
    kvtree* flush_dsets_hash = kvtree_get(hash, SCR_FLUSH_KEY_DATASET);
    kvtree_list_int(flush_dsets_hash, &flush_ndsets, &flush_dsets);

    /* get ordered list of dataset ids in cache */
    int cache_ndsets;
    int* cache_dsets;
    scr_cache_index_list_datasets(cindex, &cache_ndsets, &cache_dsets);

    int flush_index = 0;
    int cache_index = 0;
    while (flush_index < flush_ndsets && cache_index < cache_ndsets) {
      /* get next smallest index from flush file and cache */
      int flush_dset = flush_dsets[flush_index];
      int cache_dset = cache_dsets[cache_index];

      if (flush_dset < cache_dset) {
        /* dataset exists in flush file but not in cache,
         * delete it from the flush file */
        kvtree_unset_kv_int(hash, SCR_FLUSH_KEY_DATASET, flush_dset);
        flush_index++;
      } else if (cache_dset < flush_dset) {
        /* dataset exists in cache but not flush file,
         * add it to the flush file */
        kvtree* dset_hash = kvtree_set_kv_int(hash, SCR_FLUSH_KEY_DATASET, cache_dset);
        kvtree_set_kv(dset_hash, SCR_FLUSH_KEY_LOCATION, SCR_FLUSH_KEY_LOCATION_CACHE);
        cache_index++;
      } else {
        /* dataset exists in cache and the flush file,
         * ensure that it is listed as being in the cache */
        kvtree* dset_hash = kvtree_set_kv_int(hash, SCR_FLUSH_KEY_DATASET, cache_dset);
        kvtree_unset_kv(dset_hash, SCR_FLUSH_KEY_LOCATION, SCR_FLUSH_KEY_LOCATION_CACHE);
        kvtree_set_kv(dset_hash, SCR_FLUSH_KEY_LOCATION, SCR_FLUSH_KEY_LOCATION_CACHE);
        flush_index++;
        cache_index++;
      }
    }
    while (flush_index < flush_ndsets) {
      /* dataset exists in flush file but not in cache,
       * delete it from the flush file */
      int flush_dset = flush_dsets[flush_index];
      kvtree_unset_kv_int(hash, SCR_FLUSH_KEY_DATASET, flush_dset);
      flush_index++;
    }
    while (cache_index < cache_ndsets) {
      /* dataset exists in cache but not flush file,
       * add it to the flush file */
      int cache_dset = cache_dsets[cache_index];
      kvtree* dset_hash = kvtree_set_kv_int(hash, SCR_FLUSH_KEY_DATASET, cache_dset);
      kvtree_set_kv(dset_hash, SCR_FLUSH_KEY_LOCATION, SCR_FLUSH_KEY_LOCATION_CACHE);
      cache_index++;
    }

    /* free our list of cache dataset ids */
    scr_free(&cache_dsets);

    /* free our list of flush file dataset ids */
    scr_free(&flush_dsets);

    /* write the hash back to the flush file */
    kvtree_write_path(scr_flush_file, hash);

    /* delete the hash */
    kvtree_delete(&hash);
  }
  return SCR_SUCCESS;
}
