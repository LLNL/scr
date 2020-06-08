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

#define FILO_KEY_OUT_NAME "NAME"
#define FILO_KEY_OUT_AXL  "AXL"

/* records the time the async flush started */
static time_t scr_flush_async_timestamp_start;

/* records the time the async flush started */
static double scr_flush_async_time_start;

/* tracks list of files written with flush */
static kvtree* scr_flush_async_file_list = NULL;

/* tracks AXL id for outstanding transfer */
static kvtree* scr_flush_async_axl_list = NULL;

/* path to rankfile for ongoing flush */
static char* scr_flush_async_rankfile = NULL;

/* flag indicating whether we have detected failure
 * at any point in process of async flush */
static int scr_flush_async_flushed = SCR_FAILURE;

/*
=========================================
Asynchronous flush functions
=========================================
*/

static int scr_axl_start(
  const char* name,
  int num_files,
  const char** src_filelist,
  const char** dest_filelist,
  axl_xfer_t xfer_type,
  MPI_Comm comm)
{
  int rc = SCR_SUCCESS;

  /* define a transfer handle */
  int id = AXL_Create_comm(xfer_type, name, comm);
  if (id < 0) {
    scr_err("Failed to create AXL transfer handle @ %s:%d",
      __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* create record for this transfer in outstanding list,
   * record AXL id in outstanding list */
  kvtree* name_hash = kvtree_set_kv(scr_flush_async_axl_list, FILO_KEY_OUT_NAME, name);
  kvtree_util_set_int(name_hash, FILO_KEY_OUT_AXL, id);

  /* add files to transfer list */
  int i;
  for (i = 0; i < num_files; i++) {
    const char* src_file  = src_filelist[i];
    const char* dest_file = dest_filelist[i];
    if (AXL_Add(id, src_file, dest_file) != AXL_SUCCESS) {
      scr_err("Failed to add file to AXL transfer handle %d: %s --> %s @ %s:%d",
        id, src_file, dest_file, __FILE__, __LINE__
      );
      rc = SCR_FAILURE;
    }
  }

  /* kick off the transfer */
  if (AXL_Dispatch_comm(id, comm) != AXL_SUCCESS) {
    scr_err("Failed to dispatch AXL transfer handle %d @ %s:%d",
      id, __FILE__, __LINE__
    );
    rc = SCR_FAILURE;
  }

  /* TODO: it would be nice to delete the AXL id from the list if the dispatch
   * fails, but dispatch does not currently clean up properly if some procs failed
   * to dispatch and others succeeded */

  return rc;
}

static int scr_axl_test(const char* name, MPI_Comm comm)
{
  int rc = SCR_FAILURE;

  /* lookup AXL id in outstanding list */
  int id;
  kvtree* name_hash = kvtree_get_kv(scr_flush_async_axl_list, FILO_KEY_OUT_NAME, name);
  if (kvtree_util_get_int(name_hash, FILO_KEY_OUT_AXL, &id) == KVTREE_SUCCESS) {
    /* test whether transfer is still active */
    if (AXL_Test_comm(id, comm) == AXL_SUCCESS) {
      rc = SCR_SUCCESS;
    }
  }

  return rc;
}

static int scr_axl_wait(const char* name, MPI_Comm comm)
{
  int rc = SCR_SUCCESS;

  /* lookup AXL id in outstanding list */
  int id;
  kvtree* name_hash = kvtree_get_kv(scr_flush_async_axl_list, FILO_KEY_OUT_NAME, name);
  if (kvtree_util_get_int(name_hash, FILO_KEY_OUT_AXL, &id) == KVTREE_SUCCESS) {
    /* test whether transfer is still active */
    if (AXL_Wait_comm(id, comm) != AXL_SUCCESS) {
      scr_err("Failed to test AXL transfer handle %d @ %s:%d",
        id, __FILE__, __LINE__
      );
      rc = SCR_FAILURE;
    }

    /* release the handle */
    if (AXL_Free_comm(id, comm) != AXL_SUCCESS) {
      scr_err("Failed to free AXL transfer handle %d @ %s:%d",
        id, __FILE__, __LINE__
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
  /* if user has disabled flush, return failure */
  if (scr_flush <= 0) {
    return SCR_FAILURE;
  }

  /* this may take a while, so tell user what we're doing */
  if (scr_my_rank_world == 0) {
    scr_dbg(1, "scr_flush_async_stop_all: Stopping flush");
  }

  /* stop all ongoing transfers */
  if (AXL_Stop_comm(scr_comm_world) != AXL_SUCCESS) {
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

int scr_flush_async_filo_start(
  const char* rank2file,
  const char* basepath,
  int num_files,
  const char** src_filelist,
  const char** dest_filelist,
  axl_xfer_t xfer_type,
  MPI_Comm comm)
{
  int rc = SCR_SUCCESS;

  /* build a list of files for this rank */
  int i;
  kvtree* filelist = kvtree_new();
  for (i = 0; i < num_files; i++) {
    const char* filename = dest_filelist[i];

    /* if basepath is valid, compute relative path,
     * otherwise use dest path verbatim */
    if (basepath != NULL) {
      /* generate relateive path to destination file */
      spath* base = spath_from_str(basepath);
      spath* dest = spath_from_str(filename);
      spath* rel = spath_relative(base, dest);
      char* relfile = spath_strdup(rel);

      kvtree_set_kv(filelist, "FILE", relfile);

      scr_free(&relfile);
      spath_delete(&rel);
      spath_delete(&dest);
      spath_delete(&base);
    } else {
      /* use destination file name verbatim */
      kvtree_set_kv(filelist, "FILE", filename);
    }
  }

  /* save our file list to disk */
  kvtree_write_gather(rank2file, filelist, comm);

  /* free the list of files */
  kvtree_delete(&filelist);

  /* create directories */
  rc = scr_flush_create_dirs(basepath, num_files, dest_filelist, comm);

  /* write files (via AXL) */
  int success = 1;
  if (scr_axl_start(rank2file, num_files, src_filelist, dest_filelist,
    xfer_type, comm) != SCR_SUCCESS)
  {
    success = 0;
  }

  /* check that all processes started to copy successfully */
  if (! scr_alltrue(success, comm)) {
    /* TODO: auto delete files? */
    rc = SCR_FAILURE;
  }

  return rc;
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

  /* start timer */
  if (scr_my_rank_world == 0) {
    scr_flush_async_timestamp_start = scr_log_seconds();
    scr_flush_async_time_start = MPI_Wtime();

    /* log the start of the flush */
    if (scr_log_enable) {
      scr_log_event("ASYNC_FLUSH_START", NULL, &id, dset_name,
                    &scr_flush_async_timestamp_start, NULL);
    }
  }

  /* mark that we've started a flush */
  scr_flush_async_in_progress = 1;
  scr_flush_async_dataset_id = id;
  scr_flush_file_location_set(id, SCR_FLUSH_KEY_LOCATION_FLUSHING);

  /* this flag will remember whether any stage fails */
  scr_flush_async_flushed = SCR_SUCCESS;

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
        scr_log_event("ASYNC_FLUSH_FAIL", "Failed to prepare flush",
                      &id, dset_name, NULL, &time_diff);
      }
    }
    scr_dataset_delete(&dataset);
    kvtree_delete(&scr_flush_async_file_list);
    scr_flush_async_flushed = SCR_FAILURE;
    return SCR_FAILURE;
  }

  /* allocate list for filo calls */
  int numfiles;
  char** src_filelist;
  char** dst_filelist;
  scr_flush_filolist_alloc(scr_flush_async_file_list, &numfiles, &src_filelist, &dst_filelist);

  /* create enty in index file to indicate that dataset may exist,
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
  scr_flush_async_rankfile = spath_strdup(dataset_path);
  spath_delete(&dataset_path);

  /* get AXL transfer type to use */
  const scr_storedesc* storedesc = scr_cache_get_storedesc(cindex, id);
  axl_xfer_t xfer_type = axl_xfer_str_to_type(storedesc->type);

  /* flush data */
  int rc = SCR_SUCCESS;
  if (scr_flush_async_filo_start(scr_flush_async_rankfile, scr_prefix,
      numfiles, src_filelist, dst_filelist,
      xfer_type, scr_comm_world) != SCR_SUCCESS)
  {
    rc = SCR_FAILURE;
    scr_flush_async_flushed = SCR_FAILURE;
  }

  /* free our file list */
  scr_flush_filolist_free(numfiles, &src_filelist, &dst_filelist);

  /* free the dataset */
  scr_dataset_delete(&dataset);

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
  int rc = SCR_SUCCESS;
  if (scr_axl_test(scr_flush_async_rankfile, scr_comm_world) != SCR_SUCCESS) {
    rc = SCR_FAILURE;
  }
  return rc;
}

/* complete the flush from cache to parallel file system */
int scr_flush_async_complete(scr_cache_index* cindex, int id)
{
  /* if user has disabled flush, return failure */
  if (scr_flush <= 0) {
    return SCR_FAILURE;
  }

  /* get the dataset corresponding to this id */
  scr_dataset* dataset = scr_dataset_new();
  scr_cache_index_get_dataset(cindex, id, dataset);

  /* lookup dataset name */
  char* dset_name = NULL;
  scr_dataset_get_name(dataset, &dset_name);

  if (scr_my_rank_world == 0) {
    scr_dbg(1, "Completing flush of dataset %d %s @ %s:%d", id, dset_name, __FILE__, __LINE__);
  }

  /* TODO: wait on Filo if we failed to start? */
  /* wait for transfer to complete */
  if (scr_axl_wait(scr_flush_async_rankfile, scr_comm_world) != SCR_SUCCESS) {
    scr_flush_async_flushed = SCR_FAILURE;
  }

  /* write summary file */
  if (scr_flush_async_flushed == SCR_SUCCESS &&
      scr_flush_complete(cindex, id, scr_flush_async_file_list) != SCR_SUCCESS)
  {
    scr_flush_async_flushed = SCR_FAILURE;
  }

  /* mark that we've stopped the flush */
  scr_flush_async_in_progress = 0;
  scr_flush_file_location_unset(id, SCR_FLUSH_KEY_LOCATION_FLUSHING);

  /* free the file list for this checkpoint */
  kvtree_delete(&scr_flush_async_file_list);
  scr_free(&scr_flush_async_rankfile);

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

    /* delete the dataset object */
    scr_dataset_delete(&dataset);

    /* stop timer and compute bandwidth */
    double time_end = MPI_Wtime();
    double time_diff = time_end - scr_flush_async_time_start;
    double bw = 0.0;
    if (time_diff > 0.0) {
      bw = scr_flush_async_bytes / (1024.0 * 1024.0 * time_diff);
    }
    scr_dbg(1, "scr_flush_async_complete: %f secs, %e bytes, %f MB/s, %f MB/s per proc",
      time_diff, scr_flush_async_bytes, bw, bw/scr_ranks_world
    );

    /* log messages about flush */
    if (scr_flush_async_flushed == SCR_SUCCESS) {
      /* the flush worked, print a debug message */
      scr_dbg(1, "scr_flush_async_complete: Flush of dataset succeeded %d `%s'", id, dset_name);

      /* log details of flush */
      if (scr_log_enable) {
        scr_log_event("ASYNC_FLUSH_SUCCESS", NULL, &id, dset_name, NULL, &time_diff);
      }
    } else {
      /* the flush failed, this is more serious so print an error message */
      scr_err("scr_flush_async_complete: Flush of dataset failed %d `%s'", id, dset_name);

      /* log details of flush */
      if (scr_log_enable) {
        scr_log_event("ASYNC_FLUSH_FAIL", NULL, &id, dset_name, NULL, &time_diff);
      }
    }

    /* log transfer stats */
    if (scr_log_enable) {
      char* dir = NULL;
      scr_cache_index_get_dir(cindex, id, &dir);
      scr_log_transfer("FLUSH_ASYNC", dir, scr_prefix, &id, dset_name,
        &scr_flush_async_timestamp_start, &time_diff, &total_bytes, &total_files
      );
    }
  }

  /* free the dataset */
  scr_dataset_delete(&dataset);

  return scr_flush_async_flushed;
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
  scr_flush_async_axl_list = kvtree_new();
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

  kvtree_delete(&scr_flush_async_axl_list);

  MPI_Barrier(scr_comm_world);

  return SCR_SUCCESS;
}
