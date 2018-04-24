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

#include "spath.h"
#include "kvtree.h"
#include "kvtree_util.h"

#include "filo.h"

/*
=========================================
Synchronous flush functions
=========================================
*/

/* flush files specified in list, and record corresponding entries for summary file */
static int scr_flush_files_list(kvtree* file_list, kvtree* summary)
{
  /* assume we will succeed in this flush */
  int rc = SCR_SUCCESS;

  /* get pointer to file list */
  kvtree* files = kvtree_get(file_list, SCR_KEY_FILE);

  /* allocate space to hold list of file names */
  int numfiles = kvtree_size(files);
  const char** src_filelist = (const char**) SCR_MALLOC(numfiles * sizeof(const char*));
  const char** dst_filelist = (const char**) SCR_MALLOC(numfiles * sizeof(const char*));

  /* flush each of my files and fill in summary data structure */
  int i = 0;
  kvtree_elem* elem = NULL;
  for (elem = kvtree_elem_first(files);
       elem != NULL;
       elem = kvtree_elem_next(elem))
  {
    /* get the filename */
    char* file = kvtree_elem_key(elem);

    /* convert file to path and extract name of file */
    spath* path_name = spath_from_str(file);
    spath_basename(path_name);

    /* get the hash for this element */
    kvtree* hash = kvtree_elem_hash(elem);

    /* get meta data for this file */
    scr_meta* meta = kvtree_get(hash, SCR_KEY_META);

    /* get directory to flush file to */
    char* dir;
    if (kvtree_util_get_str(hash, SCR_KEY_PATH, &dir) == KVTREE_SUCCESS) {
      /* create full path of destination file */
      spath* path_full = spath_from_str(dir);
      spath_append(path_full, path_name);
      char* dst_file = spath_strdup(path_full);

      /* add file to our list */
      src_filelist[i] = strdup(file);
      dst_filelist[i] = dst_file;
      i++;

      /* get relative path to flushed file from SCR_PREFIX directory */
      spath* path_relative = spath_relative(scr_prefix_path, path_full);
      if (! spath_is_null(path_relative)) {
        /* record the name of the file in the summary hash, and get reference to a hash for this file */
        char* name = spath_strdup(path_relative);
        kvtree* file_hash = kvtree_set_kv(summary, SCR_SUMMARY_6_KEY_FILE, name);
        scr_free(&name);
      } else {
        scr_abort(-1, "Failed to get relative path to directory %s from %s @ %s:%d",
          dir, scr_prefix, __FILE__, __LINE__
        );
      }

      /* free relative and full paths */
      spath_delete(&path_relative);
      spath_delete(&path_full);
    } else {
      scr_abort(-1, "Failed to read directory to flush file to @ %s:%d",
        __FILE__, __LINE__
      );
    }

    /* free the file name path */
    spath_delete(&path_name);
  }

  /* get the dataset of this flush */
  scr_dataset* dataset = kvtree_get(file_list, SCR_KEY_DATASET);

  /* define path to metadata directory */
  char* dataset_path_str = scr_flush_dataset_metadir(dataset);
  spath* dataset_path = spath_from_str(dataset_path_str);
  spath_reduce(dataset_path);
  scr_free(&dataset_path_str);

  /* define path for rank2file map */
  spath_append_str(dataset_path, "rank2file");
  const char* rankfile = spath_strdup(dataset_path);

  /* flush data */
  if (filo_flush(numfiles, src_filelist, dst_filelist, rankfile, scr_comm_world) != FILO_SUCCESS) {
    rc = SCR_FAILURE;
  }

  /* free path and file name */
  scr_free(&rankfile);
  spath_delete(&dataset_path);

  /* free our file list */
  for (i = 0; i < numfiles; i++) {
    scr_free(&src_filelist[i]);
    scr_free(&dst_filelist[i]);
  }
  scr_free(&src_filelist);
  scr_free(&dst_filelist);

  return rc;
}

/* flushes data for files specified in file_list (with flow control),
 * and records status of each file in data */
static int scr_flush_data(kvtree* file_list, kvtree* data)
{
  int flushed = SCR_SUCCESS;

  /* first, flush each of my files and fill in meta data structure */
  if (scr_flush_files_list(file_list, data) != SCR_SUCCESS) {
    flushed = SCR_FAILURE;
  }

  /* determine whether everyone wrote their files ok */
  if (scr_alltrue((flushed == SCR_SUCCESS), scr_comm_world)) {
    return SCR_SUCCESS;
  }
  return SCR_FAILURE;
}

/* flush files from cache to parallel file system under SCR_PREFIX */
int scr_flush_sync(scr_cache_index* cindex, int id)
{
  int flushed = SCR_SUCCESS;

  /* if user has disabled flush, return failure */
  if (scr_flush <= 0) {
    return SCR_FAILURE;
  }

  /* if we don't need a flush, return right away with success */
  if (! scr_flush_file_need_flush(id)) {
    return SCR_SUCCESS;
  }

  /* this may take a while, so tell user what we're doing */
  if (scr_my_rank_world == 0) {
    scr_dbg(1, "Initiating flush of dataset %d", id);
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

  /* if we are flushing something asynchronously, wait on it */
  if (scr_flush_async_in_progress) {
    scr_flush_async_wait(cindex);

    /* the flush we just waited on could be the requested dataset,
     * so perhaps we're already done */
    if (! scr_flush_file_need_flush(id)) {
      return SCR_SUCCESS;
    }
  }

  /* log the flush start */
  if (scr_my_rank_world == 0) {
    if (scr_log_enable) {
      time_t now = scr_log_seconds();
      scr_log_event("FLUSH STARTED", NULL, &id, &now, NULL);
    }
  }

  /* mark in the flush file that we are flushing the dataset */
  scr_flush_file_location_set(id, SCR_FLUSH_KEY_LOCATION_SYNC_FLUSHING);

  /* get list of files to flush, identify containers,
   * create directories, and create container files */
  kvtree* file_list = kvtree_new();
  if (scr_flush_prepare(cindex, id, file_list) != SCR_SUCCESS) {
    flushed = SCR_FAILURE;
  }

  /* write the data out to files */
  kvtree* data = kvtree_new();
  if (scr_flush_data(file_list, data) != SCR_SUCCESS) {
    flushed = SCR_FAILURE;
  }

  /* write summary file */
  if (scr_flush_complete(id, file_list, data) != SCR_SUCCESS) {
    flushed = SCR_FAILURE;
  }

  /* get number of bytes for this dataset */
  double total_bytes = 0.0;
  if (scr_my_rank_world == 0) {
    if (flushed == SCR_SUCCESS) {
      /* get the dataset corresponding to this id */
      scr_dataset* dataset = scr_dataset_new();
      scr_cache_index_get_dataset(cindex, id, dataset);

      /* get the number of bytes in the dataset */
      unsigned long dataset_bytes;
      if (scr_dataset_get_size(dataset, &dataset_bytes) == SCR_SUCCESS) {
        total_bytes = (double) dataset_bytes;
      }

      /* delete the dataset object */
      scr_dataset_delete(&dataset);
    }
  }

  /* free data structures */
  kvtree_delete(&data);
  kvtree_delete(&file_list);

  /* remove sync flushing marker from flush file */
  scr_flush_file_location_unset(id, SCR_FLUSH_KEY_LOCATION_SYNC_FLUSHING);

  /* stop timer, compute bandwidth, and report performance */
  if (scr_my_rank_world == 0) {
    /* stop timer and compute bandwidth */
    double time_end = MPI_Wtime();
    double time_diff = time_end - time_start;
    double bw = total_bytes / (1024.0 * 1024.0 * time_diff);
    scr_dbg(1, "scr_flush_sync: %f secs, %e bytes, %f MB/s, %f MB/s per proc",
            time_diff, total_bytes, bw, bw/scr_ranks_world
    );

    /* log messages about flush */
    if (flushed == SCR_SUCCESS) {
      /* the flush worked, print a debug message */
      scr_dbg(1, "scr_flush_sync: Flush of dataset %d succeeded", id);

      /* log details of flush */
      if (scr_log_enable) {
        time_t now = scr_log_seconds();
        scr_log_event("FLUSH SUCCEEDED", NULL, &id, &now, &time_diff);
      }
    } else {
      /* the flush failed, this is more serious so print an error message */
      scr_err("scr_flush_sync: Flush of dataset %d failed", id);

      /* log details of flush */
      if (scr_log_enable) {
        time_t now = scr_log_seconds();
        scr_log_event("FLUSH FAILED", NULL, &id, &now, &time_diff);
      }
    }
  }

  return flushed;
}
