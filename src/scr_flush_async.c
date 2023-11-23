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
#include "axl_mpi.h"

#define ASYNC_KEY_OUT_DSET   "DSET"   /* list items by dataset id */
#define ASYNC_KEY_OUT_STATUS "STATUS" /* tracks whether flush has failed in any stage */
#define ASYNC_KEY_OUT_FILES  "FILES"  /* list of files to be transferred */
#define ASYNC_KEY_OUT_AXL    "AXL"    /* tracks AXL id for outstanding transfer */
#define ASYNC_KEY_OUT_TIME   "TIME"   /* start time of transfer from time */
#define ASYNC_KEY_OUT_WTIME  "WTIME"  /* start time of transfer from Wtime */

/* tracks info for all outstanding transfers */
static kvtree* scr_flush_async_list = NULL;

/*
=========================================
Asynchronous flush functions
=========================================
*/

static int scr_axl_start(
  int dset_id,
  const char* dset_name,
  const char* state_file,
  int num_files,
  const char** src_filelist,
  const char** dst_filelist,
  axl_xfer_t xfer_type,
  MPI_Comm comm)
{
  int rc = SCR_SUCCESS;

  /* define a transfer handle */
  int axl_id = AXL_Create_comm(xfer_type, dset_name, state_file, comm);
  if (axl_id < 0) {
    scr_err("Failed to create AXL transfer handle @ %s:%d",
      __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* create record for this transfer in outstanding list, and record AXL id */
  kvtree* dset_hash = kvtree_set_kv_int(scr_flush_async_list, ASYNC_KEY_OUT_DSET, dset_id);
  kvtree_util_set_int(dset_hash, ASYNC_KEY_OUT_AXL, axl_id);

  /* add files to transfer list */
  int i;
  for (i = 0; i < num_files; i++) {
    const char* src_file = src_filelist[i];
    const char* dst_file = dst_filelist[i];
    if (AXL_Add(axl_id, src_file, dst_file) != AXL_SUCCESS) {
      scr_err("Failed to add file to AXL transfer handle %d: %s --> %s @ %s:%d",
        axl_id, src_file, dst_file, __FILE__, __LINE__
      );
      rc = SCR_FAILURE;
    }
  }

  /* verify that all rank added all of their files successfully */
  if (! scr_alltrue(rc == SCR_SUCCESS, comm)) {
    /* some process failed to add its files,
     * release the handle */
    if (AXL_Free_comm(axl_id, comm) != AXL_SUCCESS) {
      scr_err("Failed to free AXL transfer handle %d @ %s:%d",
        axl_id, __FILE__, __LINE__
      );
    }

    /* and skip the dispatch step */
    return SCR_FAILURE;
  }

  /* kick off the transfer */
  if (AXL_Dispatch_comm(axl_id, comm) != AXL_SUCCESS) {
    scr_err("Failed to dispatch AXL transfer handle %d @ %s:%d",
      axl_id, __FILE__, __LINE__
    );
    rc = SCR_FAILURE;
  }

  /* TODO: it would be nice to delete the AXL id from the list if the dispatch
   * fails, but dispatch does not currently clean up properly if some procs failed
   * to dispatch and others succeeded */

  return rc;
}

static int scr_axl_test(int dset_id, MPI_Comm comm)
{
  int rc = SCR_FAILURE;

  /* lookup AXL id in outstanding list */
  int axl_id;
  kvtree* dset_hash = kvtree_get_kv_int(scr_flush_async_list, ASYNC_KEY_OUT_DSET, dset_id);
  if (kvtree_util_get_int(dset_hash, ASYNC_KEY_OUT_AXL, &axl_id) == KVTREE_SUCCESS) {
    /* test whether transfer is still active */
    if (AXL_Test_comm(axl_id, comm) == AXL_SUCCESS) {
      rc = SCR_SUCCESS;
    }
  }

  return rc;
}

static int scr_axl_wait(int dset_id, MPI_Comm comm)
{
  int rc = SCR_SUCCESS;

  /* lookup AXL id in outstanding list */
  int axl_id;
  kvtree* dset_hash = kvtree_get_kv_int(scr_flush_async_list, ASYNC_KEY_OUT_DSET, dset_id);
  if (kvtree_util_get_int(dset_hash, ASYNC_KEY_OUT_AXL, &axl_id) == KVTREE_SUCCESS) {
    /* test whether transfer is still active */
    if (AXL_Wait_comm(axl_id, comm) != AXL_SUCCESS) {
      scr_err("Failed to wait on AXL transfer handle %d @ %s:%d",
        axl_id, __FILE__, __LINE__
      );
      rc = SCR_FAILURE;
    }

    /* release the handle */
    if (AXL_Free_comm(axl_id, comm) != AXL_SUCCESS) {
      scr_err("Failed to free AXL transfer handle %d @ %s:%d",
        axl_id, __FILE__, __LINE__
      );
      rc = SCR_FAILURE;
    }
  } else {
    /* failed to lookup id */
    rc = SCR_FAILURE;
  }

  return rc;
}

/* stop all ongoing asynchronous flush operations */
int scr_flush_async_stop()
{
  /* this may take a while, so tell user what we're doing */
  if (scr_my_rank_world == 0) {
    scr_dbg(1, "Stopping all async flush operations");
  }

  /* stop all ongoing transfers */
  if (AXL_Stop_comm(scr_comm_world) != AXL_SUCCESS) {
    return SCR_FAILURE;
  }

  /* remove FLUSHING state from flush file */
  /*
  scr_flush_file_location_unset(id, SCR_FLUSH_KEY_LOCATION_FLUSHING);
  */

  /* TODO: clear async_list */

  /* make sure all processes have made it this far before we leave */
  MPI_Barrier(scr_comm_world);
  return SCR_SUCCESS;
}

/* returns 1 if any async flush is ongoing, 0 otherwise */
int scr_flush_async_in_progress(void)
{
  int size = kvtree_size(scr_flush_async_list);
  return (size > 0);
}

/* returns 1 if any id is in async list, 0 otherwise */
int scr_flush_async_in_list(int id)
{
  kvtree* dset_hash = kvtree_get_kv_int(scr_flush_async_list, ASYNC_KEY_OUT_DSET, id);
  return (dset_hash != NULL);
}

/* start an asynchronous flush from cache to parallel file
 * system under SCR_PREFIX */
int scr_flush_async_start(scr_cache_index* cindex, int id)
{
  /* if we don't need a flush, return right away with success */
  if (! scr_flush_file_need_flush(id)) {
    /* NOTE: If we don't actually need to flush, e.g., because it has already been flushed,
     * then we don't add an entry for the dataset to scr_flush_async_list in this case.
     * One may get an error if later calling scr_flush_async_test/wait for this same id. */
    return SCR_SUCCESS;
  }

  /* get the dataset corresponding to this id */
  scr_dataset* dataset = scr_dataset_new();
  scr_cache_index_get_dataset(cindex, id, dataset);

  /* lookup dataset name */
  char* dset_name = NULL;
  scr_dataset_get_name(dataset, &dset_name);

  /* this may take a while, so tell user what we're doing */
  if (scr_my_rank_world == 0) {
    scr_dbg(1, "Initiating async flush of dataset %d `%s'", id, dset_name);
  }

  /* make sure all processes make it this far before progressing */
  MPI_Barrier(scr_comm_world);

  /* create record for this transfer in outstanding list */
  kvtree* dset_hash = kvtree_set_kv_int(scr_flush_async_list, ASYNC_KEY_OUT_DSET, id);

  /* flag to indicate whether flush has failed at any stage */
  kvtree_util_set_int(dset_hash, ASYNC_KEY_OUT_STATUS, SCR_SUCCESS);

  /* start timer */
  time_t timestamp_start;
  double time_start = 0.0;
  if (scr_my_rank_world == 0) {
    timestamp_start = scr_log_seconds();
    time_start = MPI_Wtime();
    kvtree_util_set_unsigned_long(dset_hash, ASYNC_KEY_OUT_TIME, (unsigned long)timestamp_start);
    kvtree_util_set_double(dset_hash, ASYNC_KEY_OUT_WTIME, time_start);

    /* log the start of the flush */
    if (scr_log_enable) {
      scr_log_event("ASYNC_FLUSH_START", NULL, &id, dset_name,
                    &timestamp_start, NULL);
    }
  }

  /* mark that we've started a flush */
  scr_flush_file_location_set(id, SCR_FLUSH_KEY_LOCATION_FLUSHING);

  /* get list of files to flush */
  kvtree* file_list = kvtree_new();
  if (scr_flush_prepare(cindex, id, file_list) != SCR_SUCCESS) {
    if (scr_my_rank_world == 0) {
      scr_err("scr_flush_async_start: Failed to prepare flush @ %s:%d",
        __FILE__, __LINE__
      );
      if (scr_log_enable) {
        double time_start_orig;
        kvtree_util_get_double(dset_hash, ASYNC_KEY_OUT_WTIME, &time_start_orig);
        double time_end = MPI_Wtime();
        double time_diff = time_end - time_start_orig;
        scr_log_event("ASYNC_FLUSH_FAIL", "Failed to prepare flush",
                      &id, dset_name, NULL, &time_diff);
      }
    }
    kvtree_util_set_int(dset_hash, ASYNC_KEY_OUT_STATUS, SCR_FAILURE);
    scr_dataset_delete(&dataset);
    kvtree_delete(&file_list);
    return SCR_FAILURE;
  }

  /* allocate lists of source and destination paths */
  int numfiles;
  char** src_filelist;
  char** dst_filelist;
  scr_flush_list_alloc(file_list, &numfiles, &src_filelist, &dst_filelist);

  /* attach file list for this transfer to outstanding list */
  kvtree_set(dset_hash, ASYNC_KEY_OUT_FILES, file_list);

  /* create entry in index file to indicate that dataset may exist,
   * but is not yet complete */
  scr_flush_init_index(dataset);

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
  char* rankfile = spath_strdup(dataset_path);
  spath_delete(&dataset_path);

  /* build a list of files for this rank */
  int i;
  kvtree* filelist = kvtree_new();
  for (i = 0; i < numfiles; i++) {
    /* get path to destination file */
    const char* filename = dst_filelist[i];

    /* compute path relative to prefix directory */
    spath* base = spath_from_str(scr_prefix);
    spath* dest = spath_from_str(filename);
    spath* rel = spath_relative(base, dest);
    char* relfile = spath_strdup(rel);

    kvtree_set_kv(filelist, "FILE", relfile);

    scr_free(&relfile);
    spath_delete(&rel);
    spath_delete(&dest);
    spath_delete(&base);
  }

  /* save our file list to disk */
  kvtree_write_gather(rankfile, filelist, scr_comm_world);
  kvtree_delete(&filelist);
  scr_free(&rankfile);

  /* create directories */
  scr_flush_create_dirs(scr_prefix, numfiles, (const char**) dst_filelist, scr_comm_world);

  /* get AXL transfer type to use */
  const scr_storedesc* storedesc = scr_cache_get_storedesc(cindex, id);
  axl_xfer_t xfer_type = scr_xfer_str_to_axl_type(storedesc->xfer);

  /* TODO: gather list of files to leader of store descriptor,
   * use communicator of leaders for AXL, then bcast result back */

  /* if poststage is active, define path to AXL state file for this rank */
  char* state_file = NULL;
  if (scr_flush_poststage) {
    spath* state_file_spath = spath_dup(dataset_path);
    spath_append_strf(state_file_spath, "rank_%d.state_file", scr_my_rank_world);
    state_file = spath_strdup(state_file_spath);
    spath_delete(&state_file_spath);
  }

  /* start writing files via AXL */
  int rc = SCR_SUCCESS;
  if (scr_axl_start(id, dset_name, state_file, numfiles, (const char**) src_filelist, (const char**) dst_filelist,
    xfer_type, scr_comm_world) != SCR_SUCCESS)
  {
    /* failed to initiate AXL transfer */
    /* TODO: auto delete files? */
    kvtree_util_set_int(dset_hash, ASYNC_KEY_OUT_STATUS, SCR_FAILURE);
    rc = SCR_FAILURE;
  }

  /* free the path to the state file */
  scr_free(&state_file);

  /* free our file list */
  scr_flush_list_free(numfiles, &src_filelist, &dst_filelist);

  /* stop timer, compute bandwidth, and report performance */
  if (scr_my_rank_world == 0) {
    /* get the number of bytes in the dataset */
    double total_bytes = 0.0;
    unsigned long dataset_bytes;
    if (scr_dataset_get_size(dataset, &dataset_bytes) == SCR_SUCCESS) {
      total_bytes = (double) dataset_bytes;
    }

    /* get the number of files in the dataset */
    int total_files = 0.0;
    scr_dataset_get_files(dataset, &total_files);

    /* stop timer and compute bandwidth */
    double time_end = MPI_Wtime();
    double time_diff = time_end - time_start;
    scr_dbg(1, "scr_flush_async_start: %f secs, %d files, %e bytes",
            time_diff, total_files, total_bytes
    );
  }

  /* free the dataset */
  scr_dataset_delete(&dataset);

  return rc;
}

/* check whether the flush from cache to parallel file system has completed,
 * this does not indicate whether the transfer was successful, only that it
 * can be completed with either success or error without waiting */
int scr_flush_async_test(scr_cache_index* cindex, int id)
{
  /* make sure all processes make it this far before progressing */
  MPI_Barrier(scr_comm_world);

  /* if the transfer failed, indicate that transfer has completed */
  int status = SCR_FAILURE;
  kvtree* dset_hash = kvtree_get_kv_int(scr_flush_async_list, ASYNC_KEY_OUT_DSET, id);
  kvtree_util_get_int(dset_hash, ASYNC_KEY_OUT_STATUS, &status);
  if (status != SCR_SUCCESS) {
    return SCR_SUCCESS;
  }

  /* test whether transfer is done */
  int rc = SCR_SUCCESS;
  if (scr_axl_test(id, scr_comm_world) != SCR_SUCCESS) {
    rc = SCR_FAILURE;
  }

  return rc;
}

/* complete the flush from cache to parallel file system */
int scr_flush_async_complete(scr_cache_index* cindex, int id)
{
  /* get the dataset corresponding to this id */
  scr_dataset* dataset = scr_dataset_new();
  scr_cache_index_get_dataset(cindex, id, dataset);

  /* lookup dataset name */
  char* dset_name = NULL;
  scr_dataset_get_name(dataset, &dset_name);

  if (scr_my_rank_world == 0) {
    scr_dbg(1, "Completing async flush of dataset %d `%s' @ %s:%d", id, dset_name, __FILE__, __LINE__);
  }

  /* free the dataset */
  scr_dataset_delete(&dataset);

  /* lookup record for thie dataset */
  kvtree* dset_hash = kvtree_get_kv_int(scr_flush_async_list, ASYNC_KEY_OUT_DSET, id);

  /* wait for transfer to complete */
  if (scr_axl_wait(id, scr_comm_world) != SCR_SUCCESS) {
    kvtree_util_set_int(dset_hash, ASYNC_KEY_OUT_STATUS, SCR_FAILURE);
  }

  /* lookup status of transfer */
  int status = SCR_FAILURE;
  kvtree_util_get_int(dset_hash, ASYNC_KEY_OUT_STATUS, &status);

  /* get list of files for this transfer */
  kvtree* file_list = kvtree_get(dset_hash, ASYNC_KEY_OUT_FILES);

  /* write summary file */
  if (status == SCR_SUCCESS &&
      scr_flush_complete(cindex, id, file_list) != SCR_SUCCESS)
  {
    kvtree_util_set_int(dset_hash, ASYNC_KEY_OUT_STATUS, SCR_FAILURE);
  }

  /* lookup final status of transfer */
  status = SCR_FAILURE;
  kvtree_util_get_int(dset_hash, ASYNC_KEY_OUT_STATUS, &status);

  /* mark that we've stopped the flush */
  scr_flush_file_location_unset(id, SCR_FLUSH_KEY_LOCATION_FLUSHING);

  /* stop timer, compute bandwidth, and report performance */
  if (scr_my_rank_world == 0) {
    /* get the dataset corresponding to this id */
    scr_dataset* dataset = scr_dataset_new();
    scr_cache_index_get_dataset(cindex, id, dataset);

    /* get the number of bytes in the dataset */
    double total_bytes = 0.0;
    unsigned long dataset_bytes;
    if (scr_dataset_get_size(dataset, &dataset_bytes) == SCR_SUCCESS) {
      total_bytes = (double) dataset_bytes;
    }

    /* get the number of files in the dataset */
    int total_files = 0.0;
    scr_dataset_get_files(dataset, &total_files);

    /* lookup dataset name */
    char* dset_name = NULL;
    scr_dataset_get_name(dataset, &dset_name);

    /* stop timer and compute bandwidth */
    double time_start;
    kvtree_util_get_double(dset_hash, ASYNC_KEY_OUT_WTIME, &time_start);
    double time_end = MPI_Wtime();
    double time_diff = time_end - time_start;
    double bw = 0.0;
    if (time_diff > 0.0) {
      bw = total_bytes / (1024.0 * 1024.0 * time_diff);
    }
    scr_dbg(1, "scr_flush_async_complete: %f secs, %d files, %e bytes, %f MB/s, %f MB/s per proc",
      time_diff, total_files, total_bytes, bw, bw/scr_ranks_world
    );

    /* log messages about flush */
    if (status == SCR_SUCCESS) {
      /* the flush worked, print a debug message */
      scr_dbg(1, "Flush succeeded for dataset %d `%s'", id, dset_name);

      /* log details of flush */
      if (scr_log_enable) {
        scr_log_event("ASYNC_FLUSH_SUCCESS", NULL, &id, dset_name, NULL, &time_diff);
      }
    } else {
      /* the flush failed, this is more serious so print an error message */
      scr_err("Flush failed for dataset %d `%s'", id, dset_name);

      /* log details of flush */
      if (scr_log_enable) {
        scr_log_event("ASYNC_FLUSH_FAIL", NULL, &id, dset_name, NULL, &time_diff);
      }
    }

    /* log transfer stats */
    if (scr_log_enable) {
      unsigned long starttime;
      kvtree_util_get_unsigned_long(dset_hash, ASYNC_KEY_OUT_TIME, &starttime);
      time_t timestamp_start = (time_t) starttime;

      char* dir = NULL;
      scr_cache_index_get_dir(cindex, id, &dir);

      scr_log_transfer("FLUSH_ASYNC", dir, scr_prefix, &id, dset_name,
        &timestamp_start, &time_diff, &total_bytes, &total_files
      );
    }

    /* delete the dataset object */
    scr_dataset_delete(&dataset);
  }

  /* remove dset from async_list */
  kvtree_unset_kv_int(scr_flush_async_list, ASYNC_KEY_OUT_DSET, id);

  return status;
}

/* wait until the checkpoint currently being flushed completes */
int scr_flush_async_wait(scr_cache_index* cindex, int id)
{
  if (scr_flush_async_in_progress()) {
    /* get the dataset corresponding to this id */
    scr_dataset* dataset = scr_dataset_new();
    scr_cache_index_get_dataset(cindex, id, dataset);

    /* lookup dataset name */
    char* dset_name = NULL;
    scr_dataset_get_name(dataset, &dset_name);

    /* this may take a while, so tell user what we're doing */
    if (scr_my_rank_world == 0) {
      scr_dbg(1, "Waiting on async flush of dataset %d `%s'", id, dset_name);
    }

    /* delete the dataset object */
    scr_dataset_delete(&dataset);

    while (scr_flush_file_is_flushing(id)) {
      /* test whether the flush has completed, and if so complete the flush */
      if (scr_flush_async_test(cindex, id) == SCR_SUCCESS) {
        /* complete the flush */
        scr_flush_async_complete(cindex, id);
      } else {
        /* otherwise, sleep for a bit to get out of the way */
        usleep(scr_flush_async_usleep);
      }
    }
  }
  return SCR_SUCCESS;
}

/* wait until the checkpoint currently being flushed completes */
int scr_flush_async_waitall(scr_cache_index* cindex)
{
  if (scr_flush_async_in_progress()) {
    kvtree* dsets = kvtree_get(scr_flush_async_list, ASYNC_KEY_OUT_DSET);

    /* get ordered list of dataset ids */
    int num;
    int* ids;
    kvtree_list_int(dsets, &num, &ids);

    /* iterate over each dataset and wait for it to complete */
    int i;
    for (i = 0; i < num; i++) {
      int id = ids[i];
      scr_flush_async_wait(cindex, id);
    }

    /* free memory holding list of dataset ids */
    scr_free(&ids);
  }

  return SCR_SUCCESS;
}

/* progress each dataset in turn until all are complete,
 * or we find the first that is still going */
int scr_flush_async_progall(scr_cache_index* cindex)
{
  if (scr_flush_async_in_progress()) {
    kvtree* dsets = kvtree_get(scr_flush_async_list, ASYNC_KEY_OUT_DSET);

    /* get ordered list of dataset ids */
    int num;
    int* ids;
    kvtree_list_int(dsets, &num, &ids);

    /* iterate over each dataset and wait for it to complete */
    int i;
    for (i = 0; i < num; i++) {
      int id = ids[i];
      if (scr_flush_file_is_flushing(id)) {
        /* test whether the flush has completed, and if so complete the flush */
        if (scr_flush_async_test(cindex, id) == SCR_SUCCESS) {
          /* complete the flush */
          scr_flush_async_complete(cindex, id);
        } else {
          /* TODO: allow flushes to complete out of order */

          /* flush is still going, so that we don't complete datasets
           * out of order, break the loop and stop here */
          break;
        }
      }
    }

    /* free memory holding list of dataset ids */
    scr_free(&ids);
  }

  return SCR_SUCCESS;
}

/* get ordered list of ids being flushed,
 * caller is responsible for freeing ids with scr_free */
int scr_flush_async_get_list(scr_cache_index* cindex, int* num, int** ids)
{
  /* assume we don't have any */
  *num = 0;
  *ids = NULL;

  /* get ordered list of dataset ids if we have any */
  if (scr_flush_async_in_progress()) {
    kvtree* dsets = kvtree_get(scr_flush_async_list, ASYNC_KEY_OUT_DSET);
    kvtree_list_int(dsets, num, ids);
  }

  return SCR_SUCCESS;
}

/* start any processes for later asynchronous flush operations */
int scr_flush_async_init()
{
  scr_flush_async_list = kvtree_new();

  return SCR_SUCCESS;
}

/* stop all ongoing asynchronous flush operations */
int scr_flush_async_finalize()
{
  kvtree_delete(&scr_flush_async_list);

  return SCR_SUCCESS;
}
