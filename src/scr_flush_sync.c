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

/*
=========================================
Synchronous flush functions
=========================================
*/

/* flushes data for files specified in file_list (with flow control),
 * and records status of each file in data */
static int scr_flush_sync_data(scr_cache_index* cindex, int id, kvtree* file_list)
{
  /* allocate lists for source and destination paths */
  int numfiles;
  char** src_filelist;
  char** dst_filelist;
  scr_flush_list_alloc(file_list, &numfiles, &src_filelist, &dst_filelist);

  /* get the dataset of this flush */
  scr_dataset* dataset = kvtree_get(file_list, SCR_KEY_DATASET);

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

  /* if poststage is active, define path to AXL state file for this rank */
  char* state_file = NULL;
  if (scr_flush_poststage) {
    spath* state_file_path = spath_dup(dataset_path);
    spath_append_strf(state_file_path, "rank_%d.state_file", scr_my_rank_world);
    state_file = spath_strdup(state_file_path);
    spath_delete(&state_file_path);
  }

  /* define path for rank2file map */
  spath_append_str(dataset_path, "rank2file");
  const char* rank2file = spath_strdup(dataset_path);

  /* we can skip transfer if all paths match */
  int skip_transfer = 1;

  /* build a list of files for this rank */
  int i;
  kvtree* filelist = kvtree_new();
  for (i = 0; i < numfiles; i++) {
    /* get path to destination file */
    const char* filename = dst_filelist[i];

    /* found a source and destination path that are different */
    if (strcmp(src_filelist[i], filename) != 0) {
      skip_transfer = 0;
    }

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
  kvtree_write_gather(rank2file, filelist, scr_comm_world);
  kvtree_delete(&filelist);

  /* after writing out file above, see if we can skip the transfer */
  int success = 1;
  if (! scr_alltrue(skip_transfer, scr_comm_world)) {
    /* create directories */
    scr_flush_create_dirs(scr_prefix, numfiles, (const char**) dst_filelist, scr_comm_world);

    /* get name of dataset */
    char* dset_name = NULL;
    scr_dataset_get_name(dataset, &dset_name);

    /* get AXL transfer type to use */
    const scr_storedesc* storedesc = scr_cache_get_storedesc(cindex, id);
    axl_xfer_t xfer_type = scr_xfer_str_to_axl_type(storedesc->xfer);

    /* TODO: gather list of files to leader of store descriptor,
     * use communicator of leaders for AXL, then bcast result back */

    /* write files (via AXL) */
    if (scr_axl(dset_name, state_file, numfiles, (const char**) src_filelist, (const char **) dst_filelist, xfer_type, scr_comm_world) != SCR_SUCCESS) {
      success = 0;
    }
  } else {
    /* just stat the file to check that it exists */
    for (i = 0; i < numfiles; i++) {
      if (access(src_filelist[i], R_OK) < 0) {
        /* either can't read this file or it doesn't exist */
        success = 0;
        break;
      }
    }
  }

  /* free path and file name */
  scr_free(&rank2file);
  scr_free(&state_file);
  spath_delete(&dataset_path);

  /* free our file list */
  scr_flush_list_free(numfiles, &src_filelist, &dst_filelist);

  /* determine whether everyone wrote their files ok */
  int rc = SCR_SUCCESS;
  if (! scr_alltrue(success, scr_comm_world)) {
    /* TODO: auto delete files? */
    rc = SCR_FAILURE;
  }
  return rc;
}

/* flush files from cache to parallel file system under SCR_PREFIX */
int scr_flush_sync(scr_cache_index* cindex, int id)
{
  int flushed = SCR_SUCCESS;

  /* if we don't need a flush, return right away with success */
  if (! scr_flush_file_need_flush(id)) {
    return SCR_SUCCESS;
  }

  /* get the dataset corresponding to this id */
  scr_dataset* dataset = scr_dataset_new();
  scr_cache_index_get_dataset(cindex, id, dataset);

  /* get name of dataset */
  char* dset_name = NULL;
  scr_dataset_get_name(dataset, &dset_name);

  /* this may take a while, so tell user what we're doing */
  if (scr_my_rank_world == 0) {
    scr_dbg(1, "Initiating flush of dataset %d `%s'", id, dset_name);
  }

  /* make sure all processes make it this far before progressing */
  MPI_Barrier(scr_comm_world);

  /* start timer */
  time_t timestamp_start;
  double time_start = 0.0;
  if (scr_my_rank_world == 0) {
    timestamp_start = scr_log_seconds();
    time_start = MPI_Wtime();
  }

  /* if we are flushing anything asynchronously, wait on it */
  if (scr_flush_async_in_progress()) {
    scr_flush_async_waitall(cindex);

    /* the flush we just waited on could be the requested dataset,
     * so perhaps we're already done */
    if (! scr_flush_file_need_flush(id)) {
      scr_dataset_delete(&dataset);
      return SCR_SUCCESS;
    }
  }

  /* log the flush start */
  if (scr_my_rank_world == 0) {
    if (scr_log_enable) {
      scr_log_event("FLUSH_START", NULL, &id, dset_name, NULL, NULL);
    }
  }

  /* mark in the flush file that we are flushing the dataset */
  scr_flush_file_location_set(id, SCR_FLUSH_KEY_LOCATION_SYNC_FLUSHING);

  /* get list of files to flush */
  kvtree* file_list = kvtree_new();
  if (flushed == SCR_SUCCESS &&
      scr_flush_prepare(cindex, id, file_list) != SCR_SUCCESS)
  {
    flushed = SCR_FAILURE;
  }

  /* write the data out to files */
  if (flushed == SCR_SUCCESS &&
      scr_flush_sync_data(cindex, id, file_list) != SCR_SUCCESS)
  {
    flushed = SCR_FAILURE;
  }

  /* write summary file */
  if (flushed == SCR_SUCCESS &&
      scr_flush_complete(cindex, id, file_list) != SCR_SUCCESS)
  {
    flushed = SCR_FAILURE;
  }

  /* free data structures */
  kvtree_delete(&file_list);

  /* remove sync flushing marker from flush file */
  scr_flush_file_location_unset(id, SCR_FLUSH_KEY_LOCATION_SYNC_FLUSHING);

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
    double bw = 0.0;
    if (time_diff > 0.0) {
      bw = total_bytes / (1024.0 * 1024.0 * time_diff);
    }
    scr_dbg(1, "scr_flush_sync: %f secs, %d files, %e bytes, %f MB/s, %f MB/s per proc",
            time_diff, total_files, total_bytes, bw, bw/scr_ranks_world
    );

    /* log messages about flush */
    if (flushed == SCR_SUCCESS) {
      /* the flush worked, print a debug message */
      scr_dbg(1, "scr_flush_sync: Flush succeeded for dataset %d `%s'", id, dset_name);

      /* log details of flush */
      if (scr_log_enable) {
        scr_log_event("FLUSH_SUCCESS", NULL, &id, dset_name, NULL, &time_diff);
      }
    } else {
      /* the flush failed, this is more serious so print an error message */
      scr_err("scr_flush_sync: Flush failed for dataset %d `%s'", id, dset_name);

      /* log details of flush */
      if (scr_log_enable) {
        scr_log_event("FLUSH_FAIL", NULL, &id, dset_name, NULL, &time_diff);
      }
    }

    /* log transfer stats */
    if (scr_log_enable) {
      char* dir = NULL;
      scr_cache_index_get_dir(cindex, id, &dir);
      scr_log_transfer("FLUSH_SYNC", dir, scr_prefix, &id, dset_name,
        &timestamp_start, &time_diff, &total_bytes, &total_files
      );
    }
  }

  /* delete the dataset object */
  scr_dataset_delete(&dataset);

  return flushed;
}
