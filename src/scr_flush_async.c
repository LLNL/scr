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

/* records the time the async flush started */
static time_t scr_flush_async_timestamp_start;

/* records the time the async flush started */
static double scr_flush_async_time_start;

/* tracks list of files written with flush */
static kvtree* scr_flush_async_file_list = NULL;

/* path to rankfile for ongoing flush */
static char* scr_flush_async_rankfile = NULL;

/*
=========================================
Asynchronous flush functions
=========================================
*/

/* stop all ongoing asynchronous flush operations */
int scr_flush_async_stop()
{
  /* if user has disabled flush, return failure */
  if (scr_flush <= 0) {
    return SCR_FAILURE;
  }

  /* this may take a while, so tell user what we're doing */
  if (scr_my_rank_world == 0) {
    scr_dbg(1, "scr_flush_async_stop_all: Stopping flush");
  }

  /* stop all ongoing transfers */
  if (Filo_Flush_stop(scr_comm_world) != FILO_SUCCESS) {
    return SCR_FAILURE;
  }

  /* remove FLUSHING state from flush file */
  scr_flush_async_in_progress = 0;
  /*
  scr_flush_file_location_unset(id, SCR_FLUSH_KEY_LOCATION_FLUSHING);
  */

  /* clear internal flush_async variables to indicate there is no flush */
  kvtree_delete(&scr_flush_async_file_list);
  scr_free(&scr_flush_async_rankfile);

  /* make sure all processes have made it this far before we leave */
  MPI_Barrier(scr_comm_world);
  return SCR_SUCCESS;
}

/* start an asynchronous flush from cache to parallel file
 * system under SCR_PREFIX */
int scr_flush_async_start(scr_cache_index* cindex, int id)
{
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
    scr_dbg(1, "scr_flush_async_start: Initiating flush of dataset %d", id);
  }

  /* make sure all processes make it this far before progressing */
  MPI_Barrier(scr_comm_world);

  /* start timer */
  if (scr_my_rank_world == 0) {
    scr_flush_async_timestamp_start = scr_log_seconds();
    scr_flush_async_time_start = MPI_Wtime();

    /* log the start of the flush */
    if (scr_log_enable) {
      scr_log_event("ASYNC FLUSH STARTED", NULL, &id,
                    &scr_flush_async_timestamp_start, NULL);
    }
  }

  /* mark that we've started a flush */
  scr_flush_async_in_progress = 1;
  scr_flush_async_dataset_id = id;
  scr_flush_file_location_set(id, SCR_FLUSH_KEY_LOCATION_FLUSHING);

  /* get list of files to flush and create directories */
  scr_flush_async_file_list = kvtree_new();
  if (scr_flush_prepare(cindex, id, scr_flush_async_file_list) != SCR_SUCCESS) {
    if (scr_my_rank_world == 0) {
      scr_err("scr_flush_async_start: Failed to prepare flush @ %s:%d",
        __FILE__, __LINE__
      );
      if (scr_log_enable) {
        double time_end = MPI_Wtime();
        double time_diff = time_end - scr_flush_async_time_start;
        time_t now = scr_log_seconds();
        scr_log_event("ASYNC FLUSH FAILED", "Failed to prepare flush",
                      &id, &now, &time_diff);
      }
    }
    kvtree_delete(&scr_flush_async_file_list);
    scr_flush_async_file_list = NULL;
    return SCR_FAILURE;
  }

  /* allocate list for filo calls */
  int numfiles;
  char** src_filelist;
  char** dst_filelist;
  scr_flush_filolist_alloc(scr_flush_async_file_list, &numfiles, &src_filelist, &dst_filelist);

  /* get the dataset of this flush */
  scr_dataset* dataset = kvtree_get(scr_flush_async_file_list, SCR_KEY_DATASET);

  /* define path to metadata directory for this dataset */
  char* dataset_path_str = scr_flush_dataset_metadir(dataset);
  spath* dataset_path = spath_from_str(dataset_path_str);
  spath_reduce(dataset_path);
  scr_free(&dataset_path_str);

  /* create dataset directory */
  if (scr_my_rank_world == 0) {
    char* path = spath_strdup(dataset_path);
    mode_t mode_dir = scr_getmode(1, 1, 1);
    if (scr_mkdir(path, mode_dir) != SCR_SUCCESS) {
      scr_abort(-1, "Failed to create dataset subdirectory %s @ %s:%d",
        path, __FILE__, __LINE__
      );
    }
    scr_free(&path);
  }
  MPI_Barrier(scr_comm_world);

  /* define path for rank2file map */
  spath_append_str(dataset_path, "rank2file");
  scr_flush_async_rankfile = spath_strdup(dataset_path);
  spath_delete(&dataset_path);

  /* flush data */
  int rc = SCR_SUCCESS;
  if (Filo_Flush_start(scr_flush_async_rankfile, scr_prefix, numfiles, src_filelist, dst_filelist, scr_comm_world) != FILO_SUCCESS) {
    rc = SCR_FAILURE;
  }

  /* free our file list */
  scr_flush_filolist_free(numfiles, &src_filelist, &dst_filelist);

  /* make sure all processes have started before we leave */
  MPI_Barrier(scr_comm_world);

  return rc;
}

/* check whether the flush from cache to parallel file system has completed */
int scr_flush_async_test(scr_cache_index* cindex, int id)
{
  /* if user has disabled flush, return failure */
  if (scr_flush <= 0) {
    return SCR_FAILURE;
  }

  /* test whether transfer is done */
  int transfer_complete = 1;
  if (Filo_Flush_test(scr_flush_async_rankfile, scr_comm_world) != FILO_SUCCESS) {
    transfer_complete = 0;
  }

  /* determine whether the transfer is complete on all tasks */
  if (scr_alltrue(transfer_complete, scr_comm_world)) {
    if (scr_my_rank_world == 0) {
      scr_dbg(0, "#demo SCR async daemon successfully transferred dset %d", id);
    }
    return SCR_SUCCESS;
  }
  return SCR_FAILURE;
}

/* complete the flush from cache to parallel file system */
int scr_flush_async_complete(scr_cache_index* cindex, int id)
{
  int flushed = SCR_SUCCESS;

  /* if user has disabled flush, return failure */
  if (scr_flush <= 0) {
    return SCR_FAILURE;
  }

  /* TODO: have master tell each rank on node whether its files were written successfully */
  scr_dbg(1,"scr_flush_async_complete called @ %s:%d", __FILE__, __LINE__);

  /* wait for transfer to complete */
  if (Filo_Flush_wait(scr_flush_async_rankfile, scr_comm_world) != FILO_SUCCESS) {
    flushed = SCR_FAILURE;
  }

  /* write summary file */
  if (scr_flush_complete(id, scr_flush_async_file_list) != SCR_SUCCESS) {
    flushed = SCR_FAILURE;
  }

  /* mark that we've stopped the flush */
  scr_flush_async_in_progress = 0;
  scr_flush_file_location_unset(id, SCR_FLUSH_KEY_LOCATION_FLUSHING);

  /* free the file list for this checkpoint */
  kvtree_delete(&scr_flush_async_file_list);
  scr_free(&scr_flush_async_rankfile);

  /* stop timer, compute bandwidth, and report performance */
  if (scr_my_rank_world == 0) {
    double time_end = MPI_Wtime();
    double time_diff = time_end - scr_flush_async_time_start;
    double bw = scr_flush_async_bytes / (1024.0 * 1024.0 * time_diff);
    scr_dbg(1, "scr_flush_async_complete: %f secs, %e bytes, %f MB/s, %f MB/s per proc",
      time_diff, scr_flush_async_bytes, bw, bw/scr_ranks_world
    );

    /* log messages about flush */
    if (flushed == SCR_SUCCESS) {
      /* the flush worked, print a debug message */
      scr_dbg(1, "scr_flush_async_complete: Flush of dataset %d succeeded", id);

      /* log details of flush */
      if (scr_log_enable) {
        time_t now = scr_log_seconds();
        scr_log_event("ASYNC FLUSH SUCCEEDED", NULL, &id, &now, &time_diff);
      }
    } else {
      /* the flush failed, this is more serious so print an error message */
      scr_err("scr_flush_async_complete: Flush failed");

      /* log details of flush */
      if (scr_log_enable) {
        time_t now = scr_log_seconds();
        scr_log_event("ASYNC FLUSH FAILED", NULL, &id, &now, &time_diff);
      }
    }
  }

  return flushed;
}

/* wait until the checkpoint currently being flushed completes */
int scr_flush_async_wait(scr_cache_index* cindex)
{
  if (scr_flush_async_in_progress) {
    while (scr_flush_file_is_flushing(scr_flush_async_dataset_id)) {
      /* test whether the flush has completed, and if so complete the flush */
      if (scr_flush_async_test(cindex, scr_flush_async_dataset_id) == SCR_SUCCESS) {
        /* complete the flush */
        scr_flush_async_complete(cindex, scr_flush_async_dataset_id);
      } else {
        /* otherwise, sleep to get out of the way */
        usleep(10*1000*1000);
      }
    }
  }
  return SCR_SUCCESS;
}

/* start any processes for later asynchronous flush operations */
int scr_flush_async_init()
{
  /* TODO: filo async init? */

  return SCR_SUCCESS;
}

/* stop all ongoing asynchronous flush operations */
int scr_flush_async_finalize()
{
  /* if user has disabled flush, return failure */
  if (scr_flush <= 0) {
    return SCR_FAILURE;
  }

  /* this may take a while, so tell user what we're doing */
  if (scr_my_rank_world == 0) {
    scr_dbg(1, "scr_flush_async_shutdown: shutdown async procs");
  }

  /* TODO: filo async finalize? */

  MPI_Barrier(scr_comm_world);

  return SCR_SUCCESS;
}
