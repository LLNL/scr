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

#include "spath.h"
#include "kvtree.h"
#include "kvtree_util.h"
#include "filo.h"

/*
=========================================
Fetch functions
=========================================
*/

/* Overview of fetch process:
 *   1) Read index file from prefix directory
 *   2) Find most recent complete checkpoint in index file
 *      (that we've not marked as bad)
 *   3) Exit with failure if no checkpoints remain
 *   4) Read and scatter summary file information for this checkpoint
 *   5) Copy files from checkpoint directory to cache
 *        - Flow control from rank 0 via sliding window
 *        - File data may exist as physical file on parallel file
 *          system or be encapsulated in a "container" (physical file
 *          that contains bytes for one or more application files)
 *        - Optionally check CRC32 values as files are read in
 *   6) If successful, stop, otherwise mark this checkpoint as bad
 *      and repeat #2
 */

/* read contents of summary file */
static int scr_fetch_summary(
  const char* summary_dir,
  kvtree* summary_hash)
{
  /* assume that we won't succeed in our fetch attempt */
  int rc = SCR_SUCCESS;

  /* check whether summary file exists and is readable */
  if (scr_my_rank_world == 0) {
    /* check that we can access the directory */
    if (scr_file_is_readable(summary_dir) != SCR_SUCCESS) {
      scr_err("Failed to access summary directory %s @ %s:%d",
        summary_dir, __FILE__, __LINE__
      );
      rc = SCR_FAILURE;
    }
  }

  /* broadcast success code from rank 0 */
  MPI_Bcast(&rc, 1, MPI_INT, 0, scr_comm_world);
  if (rc != SCR_SUCCESS) {
    return rc;
  }

  /* add path to summary info */
  kvtree_util_set_str(summary_hash, SCR_KEY_PATH, summary_dir);

  /* build path to summary file */
  spath* meta_path = spath_from_str(summary_dir);
  spath_reduce(meta_path);

  /* rank 0 reads the summary file */
  kvtree* header = kvtree_new();
  if (scr_my_rank_world == 0) {
    /* build path to summary file */
    spath* summary_path = spath_dup(meta_path);
    spath_append_str(summary_path, "summary.scr");
    const char* summary_file = spath_strdup(summary_path);

    /* open file for reading */
    int fd = scr_open(summary_file, O_RDONLY);
    if (fd >= 0) {
      /* read summary hash */
      ssize_t header_size = kvtree_read_fd(summary_file, fd, header);
      if (header_size < 0) {
        rc = SCR_FAILURE;
      }

      /* TODO: check that the version is correct */

      /* close the file */
      scr_close(summary_file, fd);
    } else {
      scr_err("Failed to open summary file %s @ %s:%d",
        summary_file, __FILE__, __LINE__
      );
      rc = SCR_FAILURE;
    }

    /* free summary path and string */
    scr_free(&summary_file);
    spath_delete(&summary_path);
  }

  /* broadcast success code from rank 0 */
  MPI_Bcast(&rc, 1, MPI_INT, 0, scr_comm_world);
  if (rc != SCR_SUCCESS) {
    goto cleanup;
  }

  /* broadcast the summary hash */
  kvtree_bcast(header, 0, scr_comm_world);

  /* extract and record the datast in summary info */
  kvtree* dataset_hash = kvtree_new();
  scr_dataset* dataset = kvtree_get(header, SCR_SUMMARY_6_KEY_DATASET);
  kvtree_merge(dataset_hash, dataset);
  kvtree_set(summary_hash, SCR_SUMMARY_6_KEY_DATASET, dataset_hash);

cleanup:
  /* free the header hash */
  kvtree_delete(&header);

  /* free path for dataset directory */
  spath_delete(&meta_path);

  return rc;
}

/* fetch files from fetch_dir into cache_dir and update filemap */
static int scr_fetch_data(
  const kvtree* summary_hash,
  const char* fetch_dir,
  const char* cache_dir,
  scr_cache_index* cindex,
  int id)
{
  int i;
  int rc = SCR_SUCCESS;

  /* TODO: get list of files */
  /* TODO: register files in filemap */

  /* build path to rank2file map */
  spath* rank2file_path = spath_from_str(fetch_dir);
  spath_append_str(rank2file_path, "rank2file");
  const char* mapfile = spath_strdup(rank2file_path);

  /* fetch data using filo */
  int num_files = 0;
  char** src_filelist = NULL;
  char** dest_filelist = NULL;
  const scr_storedesc* storedesc = scr_cache_get_storedesc(cindex, id);

  if (Filo_Fetch(mapfile, scr_prefix, cache_dir, &num_files, &src_filelist,
    &dest_filelist, scr_comm_world, storedesc->type) != FILO_SUCCESS) {
    rc = SCR_FAILURE;
  }

  /* create a filemap for the files we just read in */
  scr_filemap* map = scr_filemap_new();
  for (i = 0; i < num_files; i++) {
    /* get source and destination file names */
    const char* src_file  = src_filelist[i];
    const char* dest_file = dest_filelist[i];

    /* add file to map */
    scr_filemap_add_file(map, dest_file);

    /* define meta for file */
    scr_meta* meta = scr_meta_new();
    scr_meta_set_complete(meta, 1);
    scr_meta_set_ranks(meta, scr_ranks_world);
    scr_meta_set_orig(meta, src_file);

    /* build absolute path to file */
    spath* path_abs = spath_from_str(src_file);
    spath_reduce(path_abs);

    /* cut absolute path into direcotry and file name */
    spath* path_name = spath_cut(path_abs, -1);

    /* store the full path and name of the original file */
    char* path = spath_strdup(path_abs);
    char* name = spath_strdup(path_name);
    scr_meta_set_origpath(meta, path);
    scr_meta_set_origname(meta, name);
    scr_free(&name);
    scr_free(&path);

    /* free directory and file name paths */
    spath_delete(&path_name);
    spath_delete(&path_abs);

    /* get file size */
    unsigned long filesize = scr_file_size(dest_file);
    scr_meta_set_filesize(meta, filesize);

    /* add meta to map */
    scr_filemap_set_meta(map, dest_file, meta);
    scr_meta_delete(&meta);
  }

  /* write out filemap */
  scr_cache_set_map(cindex, id, map);
  scr_filemap_delete(&map);

  /* free memory allocated for file list */
  for (i = 0; i < num_files; i++) {
    /* free filename strings */
    scr_free(&src_filelist[i]);
    scr_free(&dest_filelist[i]);
  }
  scr_free(&src_filelist);
  scr_free(&dest_filelist);

  scr_free(&mapfile);
  spath_delete(&rank2file_path);

  return rc;
}

/* fetch files from given dataset from parallel file system */
int scr_fetch_dset(
  scr_cache_index* cindex,
  int dset_id,
  const char* dset_name,
  int* dataset_id,
  int* checkpoint_id)
{
  /* get path to dataset metadata directory in prefix as string */
  spath* path = spath_from_str(scr_prefix_scr);
  spath_append_strf(path, "scr.dataset.%d", dset_id);
  char* fetch_dir = spath_strdup(path);
  spath_delete(&path);

  /* this may take a while, so tell user what we're doing */
  if (scr_my_rank_world == 0) {
    scr_dbg(1, "Attempting fetch: %s", dset_name);
  }

  /* make sure all processes make it this far before progressing */
  MPI_Barrier(scr_comm_world);

  /* start timer */
  time_t timestamp_start;
  double time_start;
  if (scr_my_rank_world == 0) {
    timestamp_start = scr_log_seconds();
    time_start = MPI_Wtime();
  }

  /* log the fetch attempt */
  if (scr_my_rank_world == 0) {
    if (scr_log_enable) {
      time_t now = scr_log_seconds();
      scr_log_event("FETCH STARTED", fetch_dir, NULL, &now, NULL);
    }
  }

  /* allocate a new hash to get a list of files to fetch */
  kvtree* summary_hash = kvtree_new();

  /* read the summary file for this dataset */
  if (scr_fetch_summary(fetch_dir, summary_hash) != SCR_SUCCESS) {
    if (scr_my_rank_world == 0) {
      scr_dbg(1, "Failed to read summary file @ %s:%d", __FILE__, __LINE__);
      if (scr_log_enable) {
        double time_end = MPI_Wtime();
        double time_diff = time_end - time_start;
        time_t now = scr_log_seconds();
        scr_log_event("FETCH FAILED", fetch_dir, NULL, &now, &time_diff);
      }
    }
    kvtree_delete(&summary_hash);
    scr_free(&fetch_dir);
    return SCR_FAILURE;
  }

  /* get a pointer to the dataset */
  scr_dataset* dataset = kvtree_get(summary_hash, SCR_KEY_DATASET);

  /* get the dataset id */
  int id;
  if (scr_dataset_get_id(dataset, &id) != SCR_SUCCESS) {
    if (scr_my_rank_world == 0) {
      scr_dbg(1, "Invalid id in summary file @ %s:%d", __FILE__, __LINE__);
      if (scr_log_enable) {
        double time_end = MPI_Wtime();
        double time_diff = time_end - time_start;
        time_t now = scr_log_seconds();
        scr_log_event("FETCH FAILED", fetch_dir, NULL, &now, &time_diff);
      }
    }
    scr_dbg(1, "dataset id in fetch: %d", id);

    kvtree_delete(&summary_hash);
    scr_free(&fetch_dir);
    return SCR_FAILURE;
  }

  /* get the checkpoint id for this dataset */
  int ckpt_id;
  if (scr_dataset_get_ckpt(dataset, &ckpt_id) != SCR_SUCCESS) {
    /* eventually, we'll support reading of non-checkpoint datasets,
     * but we don't yet */
    scr_err("Failed to read checkpoint id from dataset @ %s:%d",
      __FILE__, __LINE__
    );
    kvtree_delete(&summary_hash);
    scr_free(&fetch_dir);
    return SCR_FAILURE;
  }

  /* TODO: need to add some logic to avoid falling over
   * if trying to clear the cache of a dataset that does not exist */
  /* delete any existing files for this dataset id (do this before
   * filemap_read) */
  //scr_cache_delete(cindex, id);

  /* store dataset in cache index */
  scr_cache_index_set_dataset(cindex, id, dataset);

  /* get the redundancy descriptor we'd normally use for this checkpoint id */
  scr_reddesc* ckpt_rd = scr_reddesc_for_checkpoint(ckpt_id, scr_nreddescs, scr_reddescs);

  /* make a copy of the descriptor so we can tweak its settings for bypass */
  scr_reddesc rd;
  scr_reddesc* c = &rd;
  kvtree* rd_hash = kvtree_new();
  scr_reddesc_init(c);
  scr_reddesc_store_to_hash(ckpt_rd, rd_hash);
  scr_reddesc_create_from_hash(c, -1, rd_hash);
  kvtree_delete(&rd_hash);

  /* use bypass on fetch if told to do so */
  if (scr_fetch_bypass) {
    c->bypass = 1;
  }

  /* record bypass property in cache index*/
  scr_cache_index_set_bypass(cindex, id, c->bypass);

  /* get the name of the cache directory */
  char* cache_dir = scr_cache_dir_get(c, id);

  /* store the name of the directory we're about to create */
  scr_cache_index_set_dir(scr_cindex, id, cache_dir);

  /* write the cache index out before creating the directory */
  scr_cache_index_write(scr_cindex_file, cindex);

  /* create the cache directory */
  scr_cache_dir_create(c, id);

  /* we fetch into the cache directory, but we use NULL to indicate
   * that we're in bypass mode and shouldn't actually transfer files */
  char* target_dir = cache_dir;
  if (c->bypass) {
    target_dir = NULL;
  }

  /* now we can finally fetch the actual files */
  int success = 1;
  if (scr_fetch_data(summary_hash, fetch_dir, target_dir, cindex, id) != SCR_SUCCESS) {
    success = 0;
  }

  /* free cache directory name */
  scr_free(&cache_dir);

  /* free the hash holding the summary file data */
  kvtree_delete(&summary_hash);

  /* check that all processes copied their file successfully */
  if (! scr_alltrue(success, scr_comm_world)) {
    /* someone failed, so let's delete the partial checkpoint */
    scr_cache_delete(cindex, id);

    if (scr_my_rank_world == 0) {
      scr_dbg(1, "One or more processes failed to read its files @ %s:%d",
        __FILE__, __LINE__
      );
      if (scr_log_enable) {
        double time_end = MPI_Wtime();
        double time_diff = time_end - time_start;
        time_t now = scr_log_seconds();
        scr_log_event("FETCH FAILED", fetch_dir, &id, &now, &time_diff);
      }
    }

    /* free our temporary fetch redudancy descriptor */
    scr_reddesc_free(c);

    scr_free(&fetch_dir);
    return SCR_FAILURE;
  }

  /* read file map for this dataset */
  scr_filemap* map = scr_filemap_new();
  scr_cache_get_map(cindex, id, map);

  /* apply redundancy scheme */
  double bytes_copied = 0.0;
  int rc = scr_reddesc_apply(map, c, id, &bytes_copied);
  if (rc == SCR_SUCCESS) {
    /* record dataset and checkpoint ids */
    *dataset_id = id;
    *checkpoint_id = ckpt_id;

    /* update our flush file to indicate this checkpoint is in cache
     * as well as the parallel file system */
    /* TODO: should we place SCR_FLUSH_KEY_LOCATION_PFS before
     * scr_reddesc_apply? */
    scr_flush_file_location_set(id, SCR_FLUSH_KEY_LOCATION_CACHE);
    scr_flush_file_location_set(id, SCR_FLUSH_KEY_LOCATION_PFS);
    scr_flush_file_location_unset(id, SCR_FLUSH_KEY_LOCATION_FLUSHING);
  } else {
    /* something went wrong, so delete this checkpoint from the cache */
    scr_cache_delete(scr_cindex, id);
  }

  /* free filemap object */
  scr_filemap_delete(&map);

  /* free our temporary fetch redudancy descriptor */
  scr_reddesc_free(c);

  /* stop timer, compute bandwidth, and report performance */
  double total_bytes = bytes_copied;
  if (scr_my_rank_world == 0) {
    double time_end = MPI_Wtime();
    double time_diff = time_end - time_start;
    double bw = total_bytes / (1024.0 * 1024.0 * time_diff);
    scr_dbg(1, "scr_fetch_dset: %f secs, %e bytes, %f MB/s, %f MB/s per proc",
      time_diff, total_bytes, bw, bw/scr_ranks_world
    );

    /* log data on the fetch to the database */
    if (scr_log_enable) {
      time_t now = scr_log_seconds();
      if (rc == SCR_SUCCESS) {
        scr_log_event("FETCH SUCCEEDED", fetch_dir, &id, &now, &time_diff);
      } else {
        scr_log_event("FETCH FAILED", fetch_dir, &id, &now, &time_diff);
      }

      char* cache_dir = scr_cache_dir_get(c, id);
      scr_log_transfer("FETCH", fetch_dir, cache_dir, &id,
        &timestamp_start, &time_diff, &total_bytes
      );
      scr_free(&cache_dir);
    }
  }

  /* free fetch direcotry string */
  scr_free(&fetch_dir);

  return rc;
}

/* attempt to fetch most recent checkpoint from prefix directory into
 * cache, fills in map if successful and sets fetch_attempted to 1 if
 * any fetch is attempted, returns SCR_SUCCESS if successful */
int scr_fetch_latest(scr_cache_index* cindex, int* fetch_attempted)
{
  /* we only return success if we successfully fetch a checkpoint */
  int rc = SCR_FAILURE;

  double time_start, time_end, time_diff;

  /* start timer */
  if (scr_my_rank_world == 0) {
    time_start = MPI_Wtime();
  }

  /* have rank 0 read the index file */
  kvtree* index_hash = NULL;
  int read_index_file = 0;
  if (scr_my_rank_world == 0) {
    /* create an empty hash to store our index */
    index_hash = kvtree_new();

    /* read the index file */
    if (scr_index_read(scr_prefix_path, index_hash) == SCR_SUCCESS) {
      read_index_file = 1;
    }
  }

  /* don't enter while loop below if rank 0 failed to read index file */
  int continue_fetching = 1;
  MPI_Bcast(&read_index_file, 1, MPI_INT, 0, scr_comm_world);
  if (! read_index_file) {
    continue_fetching = 0;
  }

  /* now start fetching, we keep trying until we exhaust all valid
   * checkpoints */
  char target[SCR_MAX_FILENAME];
  int target_id = -1;
  while (continue_fetching) {
    /* initialize our target directory to empty string */
    strcpy(target, "");

    /* rank 0 determines the directory to fetch from */
    if (scr_my_rank_world == 0) {
      /* read the current directory if it's set */
      char* current_str;
      if (scr_index_get_current(index_hash, &current_str) == SCR_SUCCESS) {
        size_t current_str_len = strlen(current_str) + 1;
        if (current_str_len <= sizeof(target)) {
          strcpy(target, current_str);
        } else {
          /* ERROR */
        }
      }

      /* lookup the checkpoint id */
      int next_id = -1;
      if (strcmp(target, "") != 0) {
        /* we have a name, lookup the checkpoint id
         * corresponding to this name */
        scr_index_get_id_by_name(index_hash, target, &next_id);
      } else {
        /* otherwise, just get the most recent complete checkpoint
         * (that's older than the current id) */
        scr_index_get_most_recent_complete(index_hash, target_id, &next_id, target);
      }
      target_id = next_id;

      /* TODODSET: need to verify that dataset is really a checkpoint
       * and keep searching if not */

      /* if we have a subdirectory (target) name, build the full fetch
       * directory */
      if (strcmp(target, "") != 0) {
        /* record that we're attempting a fetch of this checkpoint in
         * the index file */
        *fetch_attempted = 1;
        if (target_id != -1) {
          scr_index_mark_fetched(index_hash, target_id, target);
          scr_index_write(scr_prefix_path, index_hash);
        }
      }
    }

    /* broadcast target id from rank 0 */
    MPI_Bcast(&target_id, 1, MPI_INT, 0, scr_comm_world);

    /* broadcast target name from rank 0 */
    scr_strn_bcast(target, sizeof(target), 0, scr_comm_world);

    /* check whether we've got a path */
    if (strcmp(target, "") != 0) {
      /* got something, attempt to fetch the checkpoint */
      int dset_id, ckpt_id;
      rc = scr_fetch_dset(cindex, target_id, target, &dset_id, &ckpt_id);
      if (rc == SCR_SUCCESS) {
        /* set the dataset and checkpoint ids */
        scr_dataset_id    = dset_id;
        scr_checkpoint_id = ckpt_id;
        scr_ckpt_dset_id  = dset_id;

        /* we succeeded in fetching this checkpoint, set current to
         * point to it, and stop fetching */
        if (scr_my_rank_world == 0) {
          scr_index_set_current(index_hash, target);
          scr_index_write(scr_prefix_path, index_hash);
        }
        continue_fetching = 0;
      } else {
        /* we tried to fetch, but we failed, mark it as failed in
         * the index file so we don't try it again */
        if (scr_my_rank_world == 0) {
          /* unset the current pointer */
          scr_index_unset_current(index_hash);
          if (target_id != -1 && strcmp(target, "") != 0) {
            scr_index_mark_failed(index_hash, target_id, target);
          }
          scr_index_write(scr_prefix_path, index_hash);
        }
      }
    } else {
      /* we ran out of valid checkpoints in the index file,
       * bail out of the loop */
      continue_fetching = 0;
    }
  }

  /* delete the index hash */
  if (scr_my_rank_world == 0) {
    kvtree_delete(&index_hash);
  }

  /* broadcast whether we actually attempted to fetch anything
   * (only rank 0 knows) */
  MPI_Bcast(fetch_attempted, 1, MPI_INT, 0, scr_comm_world);

  /* stop timer for fetch */
  if (scr_my_rank_world == 0) {
    time_end = MPI_Wtime();
    time_diff = time_end - time_start;
    scr_dbg(1, "scr_fetch_latest: return code %d, %f secs", rc, time_diff);
  }

  return rc;
}
