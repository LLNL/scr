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

/*
 * Flush files to the parallel file system
 *
 * axl_xfer_str: The AXL transfer type you want to use for your transfer
 *               (like "default", "sync", "pthread", "bbapi", etc).
 */
static int scr_flush_filo(
  const char* rank2file,
  const char* basepath,
  int num_files,
  const char** src_filelist,
  const char** dest_filelist,
  axl_xfer_t xfer_type,
  MPI_Comm comm)
{
  int rc = SCR_SUCCESS;

  /* we can skip transfer if all paths match */
  int skip_transfer = 1;

  /* build a list of files for this rank */
  int i;
  kvtree* filelist = kvtree_new();
  for (i = 0; i < num_files; i++) {
    const char* filename = dest_filelist[i];

    /* found a source and destination path that are different */
    if (strcmp(src_filelist[i], filename) != 0) {
      skip_transfer = 0;
    }

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

  /* after writing out file above, see if we can skip the transfer */
  int success = 1;
  if (! scr_alltrue(skip_transfer, comm)) {
    /* create directories */
    rc = scr_flush_create_dirs(basepath, num_files, dest_filelist, comm);

    /* write files (via AXL) */
    if (scr_axl(num_files, src_filelist, dest_filelist, xfer_type, comm) != SCR_SUCCESS) {
      success = 0;
    }
  } else {
    /* just stat the file to check that it exists */
    for (i = 0; i < num_files; i++) {
      if (access(src_filelist[i], R_OK) < 0) {
        /* either can't read this file or it doesn't exist */
        success = 0;
        break;
      }
    }
  }

  /* free the list of files */
  kvtree_delete(&filelist);

  /* check that all processes copied their file successfully */
  if (! scr_alltrue(success, comm)) {
    /* TODO: auto delete files? */
    rc = SCR_FAILURE;
  }

  return rc;
}

/* flushes data for files specified in file_list (with flow control),
 * and records status of each file in data */
static int scr_flush_sync_data(scr_cache_index* cindex, int id, kvtree* file_list)
{
  /* assume we will succeed in this flush */
  int flushed = SCR_SUCCESS;

  /* allocate list for filo calls */
  int numfiles;
  char** src_filelist;
  char** dst_filelist;
  scr_flush_filolist_alloc(file_list, &numfiles, &src_filelist, &dst_filelist);

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

  /* define path for rank2file map */
  spath_append_str(dataset_path, "rank2file");
  const char* rankfile = spath_strdup(dataset_path);

  /* get AXL transfer type to use */
  const scr_storedesc* storedesc = scr_cache_get_storedesc(cindex, id);
  axl_xfer_t xfer_type = axl_xfer_str_to_type(storedesc->type);

  /* flush data */
  if (scr_flush_filo(rankfile, scr_prefix,
      numfiles, src_filelist, dst_filelist,
      xfer_type, scr_comm_world) != SCR_SUCCESS)
  {
    flushed = SCR_FAILURE;
  }

  /* free path and file name */
  scr_free(&rankfile);
  spath_delete(&dataset_path);

  /* free our file list */
  scr_flush_filolist_free(numfiles, &src_filelist, &dst_filelist);

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

  /* we flush bypass datasets regardless of setting of scr_flush */
  int bypass = 0;
  scr_cache_index_get_bypass(cindex, id, &bypass);

  /* if user has disabled flush, return failure */
  if (scr_flush <= 0 && !bypass) {
    return SCR_FAILURE;
  }

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
    scr_dbg(1, "scr_flush_sync: %f secs, %e bytes, %f MB/s, %f MB/s per proc",
            time_diff, total_bytes, bw, bw/scr_ranks_world
    );

    /* log messages about flush */
    if (flushed == SCR_SUCCESS) {
      /* the flush worked, print a debug message */
      scr_dbg(1, "scr_flush_sync: Flush of dataset succeeded %d `%s'", id, dset_name);

      /* log details of flush */
      if (scr_log_enable) {
        scr_log_event("FLUSH_SUCCESS", NULL, &id, dset_name, NULL, &time_diff);
      }
    } else {
      /* the flush failed, this is more serious so print an error message */
      scr_err("scr_flush_sync: Flush of dataset failed %d `%s'", id, dset_name);

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
