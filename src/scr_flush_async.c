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

#ifdef HAVE_LIBCPPR
#include "cppr.h"
#endif

/* records the time the async flush started */
static time_t scr_flush_async_timestamp_start;

/* records the time the async flush started */
static double scr_flush_async_time_start;

/* tracks list of files written with flush */
static scr_hash* scr_flush_async_file_list = NULL;

/* tracks list of files written with flush */
static scr_hash* scr_flush_async_hash = NULL;

/* records the number of files this process must flush */
static int scr_flush_async_num_files = 0;

/*
=========================================
Globals and helper functions needed if libcppr is present
=========================================
*/

/* a possible code cleanup activity would be to make a table of function
 * pointers that can be switched out based upon which underlying file movement
 * service is being used */
#ifdef HAVE_LIBCPPR
struct scr_cppr_info {
  char *src_dir;
  char *dst_dir;
  char *filename;
  bool has_completed;
  bool alloced;
  unsigned long filesize;

  /* below is a placeholder */
  unsigned long previous_bytes_transferred;
};

/* global to contain all metadata for a cppr op
 * please see below; an index in this array contains the metadata for a handle
 * in the cppr_ops array*/
static struct scr_cppr_info* scr_flush_async_cppr_metadata = NULL;

/* global to contain only the CPPR op handles
 * the index in this array is the same index to use in the
 * scr_flush_async_cppr_metadata array for the metadata related to a given handle */
static struct cppr_op_info* cppr_ops = NULL;

/* global to contain the current count of cppr ops */
static int scr_flush_async_cppr_index = 0;

/* size of allocation blocks for calloc and realloc
 * note: this is only a unitless counter.  needs to be converted to bytes
 *   based upon the struct that is being allocated
 * TODO: SHOULD THIS BE USER CONFIGURABLE?? */
static const int scr_flush_async_cppr_alloc = 20;

/* indicates how many times calloc/realloc have been called */
static int scr_cppr_currently_alloced = 0;

/* free memory associated with the scr_cppr_info type
 * lengths of the two arrays will always be the same */
static void __free_scr_cppr_info(struct scr_cppr_info *metadata_ptr,
                                 struct cppr_op_info *handles_ptr,
                                 const int length)
{
  struct scr_cppr_info* tmp_ptr = NULL;
  int i;
  if (metadata_ptr != NULL) {
    for (i = 0; i < length; i++) {
      tmp_ptr = &metadata_ptr[i];

      if (tmp_ptr->alloced == true) {
        free(tmp_ptr->src_dir);
        free(tmp_ptr->dst_dir);
        free(tmp_ptr->filename);
      }
    }

    free(metadata_ptr);
    metadata_ptr = NULL;
  }

  if (handles_ptr != NULL) {
    free(handles_ptr);
    handles_ptr = NULL;
  }
}
#endif /* #ifdef HAVE_LIBCPPR */

/*
=========================================
Asynchronous flush helper functions
========================================
*/

/* dequeues files listed in hash2 from hash1 */
static int scr_flush_async_file_dequeue(scr_hash* hash1, scr_hash* hash2)
{
  /* for each file listed in hash2, remove it from hash1 */
  scr_hash* file_hash = scr_hash_get(hash2, SCR_TRANSFER_KEY_FILES);
  if (file_hash != NULL) {
    scr_hash_elem* elem;
    for (elem = scr_hash_elem_first(file_hash);
         elem != NULL;
         elem = scr_hash_elem_next(elem))
    {
      /* get the filename, and dequeue it */
      char* file = scr_hash_elem_key(elem);
      scr_hash_unset_kv(hash1, SCR_TRANSFER_KEY_FILES, file);
    }
  }
  return SCR_SUCCESS;
}

/*
=========================================
CPPR Asynchronous flush wrapper functions
========================================
*/
#ifdef HAVE_LIBCPPR

/* check whether the flush from cache to parallel file system has completed */
static int scr_cppr_flush_async_test(scr_filemap* map, int id, double* bytes)
{
  /* CPPR: essentially test the grp
   * cppr_return_t cppr_test_all(uint32_t count, struct cppr_op_info info[]);
   * each rank 0 on a node needs to call test_all, then report that in
   * transfer_complete, then call scr_alltrue(transfer_complete)
   * make sure transfer_complete is set to 1 in all non 0 ranks
   */

  /* assume the transfer is complete */
  int transfer_complete = 1;

  /* current operation for easier access */
  struct scr_cppr_info* current_cppr_metadata = NULL;

  /* current handle for easier access */
  struct cppr_op_info* current_cppr_handle = NULL;

  cppr_return_t cppr_retval;

  /* initialize bytes to 0 */
  *bytes = 0.0;

  /* if user has disabled flush, return failure */
  if (scr_flush <= 0) {
    return SCR_FAILURE;
  }
  scr_dbg(1,"scr_cppr_flush_async_test called @ %s:%d",
    __FILE__, __LINE__
  );

  /* have master on each node check whether the flush is complete */
  double bytes_written = 0.0;
  if (scr_storedesc_cntl->rank == 0) {
    cppr_retval = cppr_test_all(scr_flush_async_cppr_index + 1, cppr_ops);

    /* if this fails, treat it as just an incomplete transfer for now */
    if (cppr_retval != CPPR_SUCCESS) {
      scr_dbg(0, "CPPR ERROR WITH initial call to cppr_test(): %d",
        cppr_retval
      );
      transfer_complete = 0;
      goto mpi_collectives;
    }

    /* loop through all the responses and check status */
    int i;
    for (i = 0; i < scr_flush_async_cppr_index; i++) {
      scr_dbg(1, "cppr async test being called for index %d", i);

      /* set the current pointer to the correct index */
      current_cppr_metadata = &scr_flush_async_cppr_metadata[i];
      current_cppr_handle = &cppr_ops[i];

      if (current_cppr_metadata->has_completed == true) {
        /* can skip this handle because it was already marked complete */
        scr_dbg(1, "handle [%d] %x %s was marked complete", i,
                current_cppr_handle->handle,
                current_cppr_metadata->filename
        );
        continue;
      } else {
        /* check the state of the handle */
        /* check the status */
        if (current_cppr_handle->status == CPPR_STATUS_COMPLETE) {
          /* mark as complete */
          current_cppr_metadata->has_completed = true;
          scr_dbg(1, "cppr op status is COMPLETE, so setting transfer complete to \
1: handle %d, file '%s' @ %s:%d",
            i, current_cppr_metadata->filename, __FILE__, __LINE__
          );

          /* check for bad values: */
          if (current_cppr_handle->retcode != CPPR_SUCCESS) {
            scr_dbg(1, "CPPR cppr_test unsuccessful async flush for '%s' %d",
              current_cppr_metadata->filename, current_cppr_handle->retcode
            );
          } else {
            /* the file was transferred successfully */
            bytes_written += current_cppr_metadata->filesize;
            scr_dbg(2, "#bold CPPR successfully transfered file '%s' in async mode",
              current_cppr_metadata->filename
            );
          }
        } else if (current_cppr_handle->retcode == CPPR_OP_EXECUTING) {
          /* if the operation is still executing, handle accordingly */
          /* calculate bytes written */
          double percent_written = (double)
                  (current_cppr_handle->progress) / 100;

          /* bytes_written += percent_written * current_cppr_metadata->filesize; */
          transfer_complete = 0;
          scr_dbg(1,"cppr op status is EXECUTING for file '%s'; percent: \
(int %d, double %f), bytes written %f @ %s:%d",
            current_cppr_metadata->filename, current_cppr_handle->progress,
            percent_written, bytes_written,
            __FILE__, __LINE__
          );
        } else {
          scr_dbg(0,"CPPR ERROR UNHANDLED: cppr_test: unhandled values for \
src:'%s', dst:'%s', file:'%s' status %d and retcode %d; handle:[%d]: %x",
            current_cppr_metadata->src_dir,
            current_cppr_metadata->dst_dir,
            current_cppr_metadata->filename,
            current_cppr_handle->status,
            current_cppr_handle->retcode,
            i,
            current_cppr_handle->handle
          );
        }
      }
    }
  }

mpi_collectives:
  /* compute the total number of bytes written */
  MPI_Allreduce(&bytes_written, bytes, 1, MPI_DOUBLE, MPI_SUM, scr_comm_world);

  /* determine whether the transfer is complete on all tasks */
  if (scr_alltrue(transfer_complete)) {
    if (scr_my_rank_world == 0) {
      scr_dbg(0, "#demo CPPR successfully transferred dset %d", id);
    }
    return SCR_SUCCESS;
  }
  scr_dbg(1, "about to return failure from scr_cppr_flush_async_test @ %s:%d",
    __FILE__, __LINE__
  );
  return SCR_FAILURE;
}

/* complete the flush from cache to parallel file system */
static int scr_cppr_flush_async_complete(scr_filemap* map, int id)
{
  int flushed = SCR_SUCCESS;

  scr_dbg(0,"scr_cppr_flush_async_complete called @ %s:%d",
    __FILE__, __LINE__
  );

  /* if user has disabled flush, return failure */
  if (scr_flush <= 0) {
    return SCR_FAILURE;
  }

  /* allocate structure to hold metadata info */
  scr_hash* data = scr_hash_new();

  /* fill in metadata info for the files this process flushed */
  scr_hash* files = scr_hash_get(scr_flush_async_file_list, SCR_KEY_FILE);
  scr_hash_elem* elem = NULL;
  for (elem = scr_hash_elem_first(files);
       elem != NULL;
       elem = scr_hash_elem_next(elem))
  {
    /* get the filename */
    char* file = scr_hash_elem_key(elem);

    /* get the hash for this file */
    scr_hash* hash = scr_hash_elem_hash(elem);

    /* record the filename in the hash, and get reference to a hash for
     * this file */
    scr_path* path_file = scr_path_from_str(file);
    scr_path_basename(path_file);

    char* name = scr_path_strdup(path_file);
    scr_hash* file_hash = scr_hash_set_kv(data, SCR_SUMMARY_6_KEY_FILE, name);

    scr_free(&name);
    scr_path_delete(&path_file);

    /* get meta data for this file */
    scr_meta* meta = scr_hash_get(hash, SCR_KEY_META);

    /* successfully flushed this file, record the filesize */
    unsigned long filesize = 0;
    if (scr_meta_get_filesize(meta, &filesize) == SCR_SUCCESS) {
      scr_hash_util_set_bytecount(file_hash, SCR_SUMMARY_6_KEY_SIZE, filesize);
    }
    scr_dbg(1, "filesize is %d @ %s:%d", filesize, __FILE__, __LINE__);

    /* record the crc32 if one was computed */
    uLong flush_crc32;
    if (scr_meta_get_crc32(meta, &flush_crc32) == SCR_SUCCESS) {
      scr_hash_util_set_crc32(file_hash, SCR_SUMMARY_6_KEY_CRC, flush_crc32);
    }
  }

  /* write summary file */
  if (scr_flush_complete(id, scr_flush_async_file_list, data) != SCR_SUCCESS) {
    scr_dbg(1, "scr_cppr_flush_async_complete is @ %s:%d", __FILE__, __LINE__);
    flushed = SCR_FAILURE;
  }

  /* have master on each node cleanup the list of CPPR handles and files */
  if (scr_storedesc_cntl->rank == 0) {
          __free_scr_cppr_info(scr_flush_async_cppr_metadata,
                               cppr_ops,
                               scr_flush_async_cppr_index
          );
          scr_dbg(1, "scr_cppr_flush_async_complete is @ %s:%d",
            __FILE__, __LINE__
          );
          scr_flush_async_cppr_index = 0;
          scr_cppr_currently_alloced = 0;
          scr_flush_async_cppr_metadata = NULL;
          cppr_ops = NULL;
  }

  /* mark that we've stopped the flush */
  scr_flush_async_in_progress = 0;
  scr_flush_file_location_unset(id, SCR_FLUSH_KEY_LOCATION_FLUSHING);

  /* free data structures */
  scr_hash_delete(&data);

  /* free the file list for this checkpoint */
  scr_hash_delete(&scr_flush_async_hash);
  scr_hash_delete(&scr_flush_async_file_list);
  scr_flush_async_hash      = NULL;
  scr_flush_async_file_list = NULL;

  /* stop timer, compute bandwidth, and report performance */
  if (scr_my_rank_world == 0) {
    double time_end = MPI_Wtime();
    double time_diff = time_end - scr_flush_async_time_start;
    double bw = scr_flush_async_bytes / (1024.0 * 1024.0 * time_diff);
    scr_dbg(0, "scr_flush_async_complete: %f secs, %e bytes, %f MB/s, \
%f MB/s per proc",
      time_diff,
      scr_flush_async_bytes,
      bw,
      bw/scr_ranks_world
    );

    /* log messages about flush */
    if (flushed == SCR_SUCCESS) {
      /* the flush worked, print a debug message */
      scr_dbg(1, "scr_flush_async_complete: Flush of dataset %d succeeded", id);

      /* log details of flush */
      if (scr_log_enable) {
        time_t now = scr_log_seconds();
        scr_log_event("ASYNC FLUSH SUCCEEDED WITH CPPR",
                      NULL,
                      &id,
                      &now,
                      &time_diff
        );
      }
    } else {
      /* the flush failed, this is more serious so print an error message */
      scr_err("-----------FAILED:scr_flush_async_complete: Flush failed @ %s:%d",
        __FILE__, __LINE__
      );
      scr_dbg(1, "scr_cppr_flush_async_complete is at FAILURE @ %s:%d",
        __FILE__, __LINE__
      );

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
static int scr_cppr_flush_async_wait(scr_filemap* map)
{
  if (scr_flush_async_in_progress) {
    while (scr_flush_file_is_flushing(scr_flush_async_dataset_id)) {
      /* test whether the flush has completed, and if so complete the flush */
      double bytes = 0.0;
      scr_dbg(0, "scr_cppr_flush_async_test being called in cppr_async_wait @ %s:%d",
        __FILE__, __LINE__
      );
      if (scr_cppr_flush_async_test(map, scr_flush_async_dataset_id, &bytes) ==
          SCR_SUCCESS)
      {
        /* complete the flush */
        scr_cppr_flush_async_complete(map, scr_flush_async_dataset_id);
        scr_dbg(1, "scr_cppr_flush_async_wait() completed @ %s:%d",
          __FILE__, __LINE__
        );
      } else {
        /* otherwise, sleep to get out of the way */
        if (scr_my_rank_world == 0) {
          scr_dbg(0, "Flush of checkpoint %d is %d%% complete",
            scr_flush_async_dataset_id,
            (int) (bytes / scr_flush_async_bytes * 100.0)
          );
        }
        usleep(10*1000*1000);
      }
    }
  }
  return SCR_SUCCESS;
}

/* stop all ongoing asynchronous flush operations */
static int scr_cppr_flush_async_stop(void)
{
  /* if user has disabled flush, return failure */
  if (scr_flush <= 0) {
    return SCR_FAILURE;
  }

  /* this may take a while, so tell user what we're doing */
  if (scr_my_rank_world == 0) {
    scr_dbg(1, "scr_cppr_flush_async_stop: Stopping flush");
  }

  /* wait until all tasks know the transfer is stopped */
  scr_cppr_flush_async_wait(NULL);

  /* cleanup CPPR handles */
  if (scr_storedesc_cntl->rank == 0) {
          __free_scr_cppr_info(scr_flush_async_cppr_metadata,
                               cppr_ops,
                               scr_flush_async_cppr_index
          );

          scr_flush_async_cppr_index = 0;
          scr_cppr_currently_alloced = 0;
          scr_flush_async_cppr_metadata = NULL;
          cppr_ops = NULL;
  }

  /* set global status to 0 */
  scr_flush_async_in_progress = 0;

  /* clear internal flush_async variables to indicate there is no flush */
  if (scr_flush_async_hash != NULL) {
    scr_hash_delete(&scr_flush_async_hash);
  }
  if (scr_flush_async_file_list != NULL) {
    scr_hash_delete(&scr_flush_async_file_list);
  }

  /* make sure all processes have made it this far before we leave */
  MPI_Barrier(scr_comm_world);
  return SCR_SUCCESS;
}

/* start an asynchronous flush from cache to parallel file system
 * under SCR_PREFIX */
static int scr_cppr_flush_async_start(scr_filemap* map, int id)
{
  /* todo: consider using CPPR grp API when available */

  /* if user has disabled flush, return failure */
  if (scr_flush <= 0) {
    return SCR_FAILURE;
  }

  scr_dbg(1,"scr_cppr_flush_async_start() called");

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
      scr_log_event("ASYNC FLUSH STARTED",
                    NULL,
                    &id,
                    &scr_flush_async_timestamp_start,
                    NULL
      );
    }
  }

  /* mark that we've started a flush */
  scr_flush_async_in_progress = 1;
  scr_flush_async_dataset_id = id;
  scr_flush_file_location_set(id, SCR_FLUSH_KEY_LOCATION_FLUSHING);

  /* get list of files to flush and create directories */
  scr_flush_async_file_list = scr_hash_new();
  if (scr_flush_prepare(map, id, scr_flush_async_file_list) != SCR_SUCCESS) {
    if (scr_my_rank_world == 0) {
      scr_err("scr_flush_async_start: Failed to prepare flush @ %s:%d",
        __FILE__, __LINE__
      );
      if (scr_log_enable) {
        double time_end = MPI_Wtime();
        double time_diff = time_end - scr_flush_async_time_start;
        time_t now = scr_log_seconds();
        scr_log_event("ASYNC FLUSH FAILED",
                      "Failed to prepare flush",
                      &id,
                      &now,
                      &time_diff
        );
      }
    }
    scr_hash_delete(&scr_flush_async_file_list);
    scr_flush_async_file_list = NULL;
    return SCR_FAILURE;
  }

  /* add each of my files to the transfer file list */
  scr_flush_async_hash = scr_hash_new();
  scr_flush_async_num_files = 0;
  double my_bytes = 0.0;
  scr_hash_elem* elem;
  scr_hash* files = scr_hash_get(scr_flush_async_file_list, SCR_KEY_FILE);
  for (elem = scr_hash_elem_first(files);
       elem != NULL;
       elem = scr_hash_elem_next(elem))
  {
    /* get the filename */
    char* file = scr_hash_elem_key(elem);

    /* get the hash for this file */
    scr_hash* file_hash = scr_hash_elem_hash(elem);

    /* get directory to flush file to */
    char* dest_dir;
    if (scr_hash_util_get_str(file_hash, SCR_KEY_PATH, &dest_dir) !=
        SCR_SUCCESS)
    {
      continue;
    }

    /* get meta data for file */
    scr_meta* meta = scr_hash_get(file_hash, SCR_KEY_META);

    /* get the file size */
    unsigned long filesize = 0;
    if (scr_meta_get_filesize(meta, &filesize) != SCR_SUCCESS) {
      continue;
    }
    my_bytes += (double) filesize;

    /* add this file to the hash, and add its filesize to the number of
     * bytes written */
    scr_hash* transfer_file_hash = scr_hash_set_kv(scr_flush_async_hash,
                                                   SCR_TRANSFER_KEY_FILES,
                                                   file);
    if (file_hash != NULL) {
      /* break file into path and name components */
      scr_path* path_dest_file = scr_path_from_str(file);
      scr_path_basename(path_dest_file);
      scr_path_prepend_str(path_dest_file, dest_dir);
      char* dest_file = scr_path_strdup(path_dest_file);

      scr_hash_util_set_str(transfer_file_hash,
                            SCR_TRANSFER_KEY_DESTINATION,
                            dest_file);

      scr_hash_util_set_bytecount(transfer_file_hash,
                                  SCR_TRANSFER_KEY_SIZE,
                                  filesize);

      scr_hash_util_set_bytecount(transfer_file_hash,
                                  SCR_TRANSFER_KEY_WRITTEN,
                                  0);

      /* delete path and string for the file name */
      scr_free(&dest_file);
      scr_path_delete(&path_dest_file);
    }

    /* add this file to our total count */
    scr_flush_async_num_files++;
  }

  /* have master on each node write the transfer file, everyone else
   * sends data to him */
  if (scr_storedesc_cntl->rank == 0) {
    /* receive hash data from other processes on the same node
     * and merge with our data */
    int i;
    for (i=1; i < scr_storedesc_cntl->ranks; i++) {
      scr_hash* h = scr_hash_new();
      scr_hash_recv(h, i, scr_storedesc_cntl->comm);
      scr_hash_merge(scr_flush_async_hash, h);
      scr_hash_delete(&h);
    }
    scr_dbg(3,"hash output printed: ");
    scr_hash_log(scr_flush_async_hash, 3, 0);
    scr_dbg(3,"----------------end flush_async_hash, begin file list");
    scr_hash_log(scr_flush_async_file_list, 3, 0);
    scr_dbg(3,"printed out the hashes");

    /* get a hash to store file data */
    scr_hash* hash = scr_hash_new();

    int writers;
    MPI_Comm_size(scr_comm_node_across, &writers);

    /* allocate the cppr hash (free them when shutting down)
     * first check to ensure it is NULL.  if it is not NULL, this may be the
     * case if an asyncflush failed. need to free it first if not NULL  */
    if (scr_flush_async_cppr_metadata != NULL) {
      scr_dbg(3, "#bold WHY FREE METADATA AGAIN?? ");
      __free_scr_cppr_info(scr_flush_async_cppr_metadata,
                           cppr_ops,
                           scr_flush_async_cppr_index
      );
    }

    scr_dbg(3, "#bold about to calloc @ %s:%d", __FILE__, __LINE__);
    scr_flush_async_cppr_metadata = calloc(scr_flush_async_cppr_alloc,
                                      sizeof(struct scr_cppr_info));
    scr_dbg(3, "#bold after calloc @ %s:%d", __FILE__, __LINE__);

    if (scr_flush_async_cppr_metadata == NULL) {
      scr_dbg(1,"couldn't allocate enough memory for cppr operation metadata");
      return SCR_FAILURE;
    }

    cppr_ops = calloc(scr_flush_async_cppr_alloc,
                      sizeof(struct cppr_op_info));

    if (cppr_ops == NULL) {
      scr_dbg(1,"couldn't allocate enough memory for cppr operation handles");
      return SCR_FAILURE;
    }

    /* update the currently alloced size */
    scr_cppr_currently_alloced++;

    /*  CPPR just needs to iterate through this combined hash */
    /* call cppr_mv and save the handles */
    files = scr_hash_get(scr_flush_async_file_list, SCR_KEY_FILE);
    for (elem = scr_hash_elem_first(files);
         elem != NULL;
         elem = scr_hash_elem_next(elem))
    {
      /* get the filename */
      char* file = scr_hash_elem_key(elem);

      /* get the hash for this file */
      scr_hash* file_hash = scr_hash_elem_hash(elem);

      /* get directory to flush file to */
      char* dest_dir;
      if (scr_hash_util_get_str(file_hash, SCR_KEY_PATH, &dest_dir) !=
          SCR_SUCCESS)
      {
        continue;
      }

      /* get meta data for file */
      scr_meta* meta = scr_hash_get(file_hash, SCR_KEY_META);

      /* get just the file name */
      char *plain_filename = NULL;
      if (scr_meta_get_filename(meta, &plain_filename) != SCR_SUCCESS) {
        scr_dbg(0,"couldn't get the file name from meta '%s'", file);
        continue;
      }

      /* get the file size */
      unsigned long filesize = 0;
      if (scr_meta_get_filesize(meta, &filesize) != SCR_SUCCESS) {
        continue;
      }

      /* get the information for this file */
      scr_hash* transfer_file_hash = scr_hash_get_kv(scr_flush_async_hash,
                                                     SCR_TRANSFER_KEY_FILES,
                                                     file);
      if (transfer_file_hash != NULL) {
        /* break file into path and name components */
        scr_path* path_dest_file = scr_path_from_str(file);

        /* get full path */
        char *full_path = scr_path_strdup(path_dest_file);

        /* get only the src dir */
        scr_path_dirname(path_dest_file);
        char *only_path_dest = scr_path_strdup(path_dest_file);

        /* get only the file name */
        scr_path_basename(path_dest_file);
        char *basename_path = scr_path_strdup(path_dest_file);

        /* add dest dir to the file name */
        scr_path_prepend_str(path_dest_file, dest_dir);

        char* dest_file = scr_path_strdup(path_dest_file);

        if (scr_path_dirname(path_dest_file) != SCR_SUCCESS) {
          return SCR_FAILURE;
        }

        if (full_path == NULL || basename_path == NULL || dest_file == NULL) {
          scr_dbg(1,"CPPR error allocating for the file paths");
          return SCR_FAILURE;
        }
        scr_dbg(2,"CPPR async dest file paths:'%s', base:'%s', dest:'%s' lone \
filename: '%s' src path? '%s'", full_path,
          basename_path,
          dest_file,
          plain_filename,
          only_path_dest
        );

        if ((scr_flush_async_cppr_index+1) >=
            (scr_cppr_currently_alloced * scr_flush_async_cppr_alloc))
        {
          scr_dbg(1, "CPPR reallocating the CPPR handles array @ %s:%d",
            __FILE__, __LINE__
          );
          int bytes_currently_used = 0;

          /* increment the counter to indicate another alloc has happened */
          scr_cppr_currently_alloced++;

          /* reallocate the cppr metadata array */
          int new_size_to_alloc = sizeof(struct scr_cppr_info) *
                  scr_flush_async_cppr_alloc *
                  scr_cppr_currently_alloced;
          void *new_ptr = realloc((void *)scr_flush_async_cppr_metadata,
                                  new_size_to_alloc);
          if (new_ptr == NULL) {
            scr_dbg(1, "not enough mem for CPPR metadata @ %s:%d",
              __FILE__, __LINE__
            );
            scr_cppr_currently_alloced--;
            return SCR_FAILURE;
          }

          /* update the pointer */
          scr_flush_async_cppr_metadata = (struct scr_cppr_info *)new_ptr;

          /* clear out only the newly allocated space */
          bytes_currently_used = (scr_flush_async_cppr_index+1)*
                                  sizeof(struct scr_cppr_info);
          memset((void *) scr_flush_async_cppr_metadata +
                 bytes_currently_used,
                 0x00,
                 new_size_to_alloc - bytes_currently_used );

          /* realloc the cppr handles array */
          new_size_to_alloc = sizeof(struct cppr_op_info) *
                  scr_flush_async_cppr_alloc *
                  scr_cppr_currently_alloced;

          /* clear the new_ptr before reusing it */
          new_ptr = NULL;

          new_ptr = realloc((void *) cppr_ops, new_size_to_alloc);
          if (new_ptr == NULL) {
            scr_dbg(1, "not enough mem for CPPR handles @ %s:%d",
              __FILE__, __LINE__
            );
            scr_cppr_currently_alloced--;
            return SCR_FAILURE;
          }
          /* update the pointer */
          cppr_ops = (struct cppr_op_info *) new_ptr;

          /* clear out the newly allocated space */
          bytes_currently_used = (scr_flush_async_cppr_index+1)*
                                  sizeof(struct cppr_op_info);
          memset((void *) cppr_ops + bytes_currently_used,
                 0x00,
                 new_size_to_alloc - bytes_currently_used);

          scr_dbg(1, "CPPR reallocate done @ %s:%d", __FILE__, __LINE__);
        }

        scr_dbg(1, "executing cppr_mv for %s", plain_filename);
        scr_flush_async_cppr_metadata[scr_flush_async_cppr_index].src_dir =
                strdup(only_path_dest);
        scr_flush_async_cppr_metadata[scr_flush_async_cppr_index].dst_dir =
                strdup(dest_dir);
        scr_flush_async_cppr_metadata[scr_flush_async_cppr_index].filename =
                strdup(plain_filename);
        scr_flush_async_cppr_metadata[scr_flush_async_cppr_index].filesize =
                filesize;
        scr_flush_async_cppr_metadata[scr_flush_async_cppr_index].alloced =
                true;

        if (cppr_mv(&(cppr_ops[scr_flush_async_cppr_index]),
            NULL,
            CPPR_FLAG_TRACK_PROGRESS,
            NULL,
            dest_dir,
            only_path_dest,
            plain_filename) != CPPR_SUCCESS)
        {
          scr_dbg(0, "----CPPR FAILED KICKING OFF MV FOR: %s", basename_path);
          return SCR_FAILURE;
        }
        scr_dbg(1, "cppr handle array position %d is value %x",
                scr_flush_async_cppr_index,
                cppr_ops[scr_flush_async_cppr_index].handle
        );

        /* critical to update the handle counter */
        scr_flush_async_cppr_index++;

        /* delete path and string for the file name */
        scr_free(&dest_file);
        scr_free(&full_path);
        scr_free(&basename_path);
        scr_path_delete(&path_dest_file);
      } else {
        /* confirmed the bug?? */
        scr_dbg(0,"ERROR NEED TO CHECK THIS why was this value null BUG \
confirmed?: %s", file);
      }
    }

    /* delete the hash */
    scr_hash_delete(&hash);
  } else {
    /* send our transfer hash data to the master on this node */
    scr_hash_send(scr_flush_async_hash, 0, scr_storedesc_cntl->comm);
  }

  /* get the total number of bytes to write */
  scr_flush_async_bytes = 0.0;
  MPI_Allreduce(&my_bytes, &scr_flush_async_bytes, 1, MPI_DOUBLE, MPI_SUM, scr_comm_world);

  /* make sure all processes have started before we leave */
  MPI_Barrier(scr_comm_world);

  return SCR_SUCCESS;
}

/*
=========================================
END: CPPR Asynchronous flush wrapper functions
========================================
*/
#endif /* HAVE_LIBCPPR */

/*
=========================================
Asynchronous flush functions
=========================================
*/

/* given a hash, test whether the files in that hash have completed their flush */
static int scr_flush_async_file_test(const scr_hash* hash, double* bytes)
{
  /* initialize bytes to 0 */
  *bytes = 0.0;

  /* get the FILES hash */
  scr_hash* files_hash = scr_hash_get(hash, SCR_TRANSFER_KEY_FILES);
  if (files_hash == NULL) {
    /* can't tell whether this flush has completed */
    return SCR_FAILURE;
  }

  /* assume we're done, look for a file that says we're not */
  int transfer_complete = 1;

  /* for each file, check whether the WRITTEN field matches the SIZE field,
   * which indicates the file has completed its transfer */
  scr_hash_elem* elem;
  for (elem = scr_hash_elem_first(files_hash);
       elem != NULL;
       elem = scr_hash_elem_next(elem))
  {
    /* get the hash for this file */
    scr_hash* file_hash = scr_hash_elem_hash(elem);
    if (file_hash == NULL) {
      transfer_complete = 0;
      continue;
    }

    /* lookup the values for the size and bytes written */
    unsigned long size, written;
    if (scr_hash_util_get_bytecount(file_hash, "SIZE",    &size)    != SCR_SUCCESS ||
        scr_hash_util_get_bytecount(file_hash, "WRITTEN", &written) != SCR_SUCCESS)
    {
      transfer_complete = 0;
      continue;
    }

    /* check whether the number of bytes written is less than the filesize */
    if (written < size) {
      transfer_complete = 0;
    }

    /* add up number of bytes written */
    *bytes += (double) written;
  }

  /* return our decision */
  if (transfer_complete) {
    return SCR_SUCCESS;
  }
  return SCR_FAILURE;
}

/* writes the specified command to the transfer file */
static int scr_flush_async_command_set(char* command)
{
  /* have the master on each node write this command to the file */
  if (scr_storedesc_cntl->rank == 0) {
    /* get a hash to store file data */
    scr_hash* hash = scr_hash_new();

    /* read the file */
    int fd = -1;
    scr_hash_lock_open_read(scr_transfer_file, &fd, hash);

    /* set the command */
    scr_hash_util_set_str(hash, SCR_TRANSFER_KEY_COMMAND, command);

    /* write the hash back */
    scr_hash_write_close_unlock(scr_transfer_file, &fd, hash);

    /* delete the hash */
    scr_hash_delete(&hash);
  }
  return SCR_SUCCESS;
}

/* waits until all transfer processes are in the specified state */
static int scr_flush_async_state_wait(char* state)
{
  /* wait until each process matches the state */
  int all_valid = 0;
  while (! all_valid) {
    /* assume we match the specified state */
    int valid = 1;

    /* have the master on each node check the state in the transfer file */
    if (scr_storedesc_cntl->rank == 0) {
      /* get a hash to store file data */
      scr_hash* hash = scr_hash_new();

      /* open transfer file with lock */
      scr_hash_read_with_lock(scr_transfer_file, hash);

      /* check for the specified state */
      scr_hash* state_hash = scr_hash_get_kv(hash,
                                             SCR_TRANSFER_KEY_STATE,
                                             state);
      if (state_hash == NULL) {
        valid = 0;
      }

      /* delete the hash */
      scr_hash_delete(&hash);
    }

    /* check whether everyone is at the specified state */
    if (scr_alltrue(valid)) {
      all_valid = 1;
    }

    /* if we're not there yet, sleep for sometime and they try again */
    if (! all_valid) {
      usleep(10*1000*1000);
    }
  }
  return SCR_SUCCESS;
}

/* removes all files from the transfer file */
static int scr_flush_async_file_clear_all()
{
  /* have the master on each node clear the FILES field */
  if (scr_storedesc_cntl->rank == 0) {
    /* get a hash to store file data */
    scr_hash* hash = scr_hash_new();

    /* read the file */
    int fd = -1;
    scr_hash_lock_open_read(scr_transfer_file, &fd, hash);

    /* clear the FILES entry */
    scr_hash_unset(hash, SCR_TRANSFER_KEY_FILES);

    /* write the hash back */
    scr_hash_write_close_unlock(scr_transfer_file, &fd, hash);

    /* delete the hash */
    scr_hash_delete(&hash);
  }
  return SCR_SUCCESS;
}

/* stop all ongoing asynchronous flush operations */
int scr_flush_async_stop()
{
  /* cppr: just call wait_all, once complete, set state, notify everyone */
  /*  cppr_return_t cppr_wait_all(uint32_t count,
   *                       struct cppr_op_info info[],
   *                    uint32_t timeoutMS) */
#ifdef HAVE_LIBCPPR
        return scr_cppr_flush_async_stop();
#endif
  /* if user has disabled flush, return failure */
  if (scr_flush <= 0) {
    return SCR_FAILURE;
  }

  /* this may take a while, so tell user what we're doing */
  if (scr_my_rank_world == 0) {
    scr_dbg(1, "scr_flush_async_stop_all: Stopping flush");
  }

  /* write stop command to transfer file */
  scr_flush_async_command_set(SCR_TRANSFER_KEY_COMMAND_STOP);

  /* wait until all tasks know the transfer is stopped */
  scr_flush_async_state_wait(SCR_TRANSFER_KEY_STATE_STOP);

  /* remove the files list from the transfer file */
  scr_flush_async_file_clear_all();

  /* remove FLUSHING state from flush file */
  scr_flush_async_in_progress = 0;
  /*
  scr_flush_file_location_unset(id, SCR_FLUSH_KEY_LOCATION_FLUSHING);
  */

  /* clear internal flush_async variables to indicate there is no flush */
  if (scr_flush_async_hash != NULL) {
    scr_hash_delete(&scr_flush_async_hash);
  }
  if (scr_flush_async_file_list != NULL) {
    scr_hash_delete(&scr_flush_async_file_list);
  }

  /* make sure all processes have made it this far before we leave */
  MPI_Barrier(scr_comm_world);
  return SCR_SUCCESS;

}

/* start an asynchronous flush from cache to parallel file
 * system under SCR_PREFIX */
int scr_flush_async_start(scr_filemap* map, int id)
{
#ifdef HAVE_LIBCPPR
  return scr_cppr_flush_async_start(map, id) ;
#endif

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
  scr_flush_async_file_list = scr_hash_new();
  if (scr_flush_prepare(map, id, scr_flush_async_file_list) != SCR_SUCCESS) {
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
    scr_hash_delete(&scr_flush_async_file_list);
    scr_flush_async_file_list = NULL;
    return SCR_FAILURE;
  }

  /* add each of my files to the transfer file list */
  scr_flush_async_hash = scr_hash_new();
  scr_flush_async_num_files = 0;
  double my_bytes = 0.0;
  scr_hash_elem* elem;
  scr_hash* files = scr_hash_get(scr_flush_async_file_list, SCR_KEY_FILE);
  for (elem = scr_hash_elem_first(files);
       elem != NULL;
       elem = scr_hash_elem_next(elem))
  {
     /* get the filename */
     char* file = scr_hash_elem_key(elem);

     /* get the hash for this file */
     scr_hash* file_hash = scr_hash_elem_hash(elem);

     /* get directory to flush file to */
     char* dest_dir;
     if (scr_hash_util_get_str(file_hash, SCR_KEY_PATH, &dest_dir) !=
         SCR_SUCCESS) {
       continue;
     }

     /* get meta data for file */
     scr_meta* meta = scr_hash_get(file_hash, SCR_KEY_META);

     /* get the file size */
     unsigned long filesize = 0;
     if (scr_meta_get_filesize(meta, &filesize) != SCR_SUCCESS) {
       continue;
     }
     my_bytes += (double) filesize;

     /* add this file to the hash, and add its filesize
      * to the number of bytes written */
     scr_hash* transfer_file_hash = scr_hash_set_kv(scr_flush_async_hash,
                                                    SCR_TRANSFER_KEY_FILES,
                                                    file);
     /* TODO BUG FIX HERE: file_hash should be transfer_file_hash?? */
     if (file_hash != NULL) {
       /* break file into path and name components */
             scr_path* path_dest_file = scr_path_from_str(file);
             scr_path_basename(path_dest_file);
             scr_path_prepend_str(path_dest_file, dest_dir);
             char* dest_file = scr_path_strdup(path_dest_file);

             scr_hash_util_set_str(transfer_file_hash,
                                   SCR_TRANSFER_KEY_DESTINATION,
                                   dest_file);
             scr_hash_util_set_bytecount(transfer_file_hash,
                                         SCR_TRANSFER_KEY_SIZE,
                                         filesize);
             scr_hash_util_set_bytecount(transfer_file_hash,
                                         SCR_TRANSFER_KEY_WRITTEN,
                                         0);

             /* delete path and string for the file name */
             scr_free(&dest_file);
             scr_path_delete(&path_dest_file);
     }
     else{
       scr_dbg(1,"-----file_hash was null BUG?-----'%s' @ %s:%d", file, __FILE__, __LINE__);

     }

     /* add this file to our total count */
     scr_flush_async_num_files++;
  }

  /* have master on each node write the transfer file, everyone else sends data to him */
  if (scr_storedesc_cntl->rank == 0) {
    /* receive hash data from other processes on the same node and merge with our data */
    int i;
    for (i=1; i < scr_storedesc_cntl->ranks; i++) {
      scr_hash* h = scr_hash_new();
      scr_hash_recv(h, i, scr_storedesc_cntl->comm);
      scr_hash_merge(scr_flush_async_hash, h);
      scr_hash_delete(&h);
    }
    /* get a hash to store file data */
    scr_hash* hash = scr_hash_new();

    /* open transfer file with lock */
    int fd = -1;
    scr_hash_lock_open_read(scr_transfer_file, &fd, hash);

    /* merge our data to the file data */
    scr_hash_merge(hash, scr_flush_async_hash);

    /* set BW if it's not already set */
    /* TODO: somewhat hacky way to determine number of nodes and therefore number of writers */
    int writers;
    MPI_Comm_size(scr_comm_node_across, &writers);
    double bw;
    if (scr_hash_util_get_double(hash, SCR_TRANSFER_KEY_BW, &bw) !=
        SCR_SUCCESS) {
      bw = (double) scr_flush_async_bw / (double) writers;
      scr_hash_util_set_double(hash, SCR_TRANSFER_KEY_BW, bw);
    }

    /* set PERCENT if it's not already set */
    double percent;
    if (scr_hash_util_get_double(hash, SCR_TRANSFER_KEY_PERCENT, &percent) !=
        SCR_SUCCESS) {
      scr_hash_util_set_double(hash, SCR_TRANSFER_KEY_PERCENT,
                               scr_flush_async_percent);
    }

    /* set the RUN command */
    scr_hash_util_set_str(hash, SCR_TRANSFER_KEY_COMMAND,
                          SCR_TRANSFER_KEY_COMMAND_RUN);

    /* unset the DONE flag */
    scr_hash_unset_kv(hash, SCR_TRANSFER_KEY_FLAG, SCR_TRANSFER_KEY_FLAG_DONE);

    /* close the transfer file and release the lock */
    scr_hash_write_close_unlock(scr_transfer_file, &fd, hash);

    /* delete the hash */
    scr_hash_delete(&hash);
  } else {
    /* send our transfer hash data to the master on this node */
    scr_hash_send(scr_flush_async_hash, 0, scr_storedesc_cntl->comm);
  }

  /* get the total number of bytes to write */
  scr_flush_async_bytes = 0.0;
  MPI_Allreduce(&my_bytes, &scr_flush_async_bytes, 1, MPI_DOUBLE, MPI_SUM,
                scr_comm_world);

  /* TODO: start transfer thread / process */

  /* make sure all processes have started before we leave */
  MPI_Barrier(scr_comm_world);

  return SCR_SUCCESS;

}

/* check whether the flush from cache to parallel file system has completed */
int scr_flush_async_test(scr_filemap* map, int id, double* bytes)
{

#ifdef HAVE_LIBCPPR
  scr_dbg(1, "scr_flush_async_cppr_test being called by scr_flush_async_test \
@ %s:%d", __FILE__, __LINE__);
  return scr_cppr_flush_async_test(map, id, bytes);
#endif

  /* initialize bytes to 0 */
  *bytes = 0.0;

  /* if user has disabled flush, return failure */
  if (scr_flush <= 0) {
    return SCR_FAILURE;
  }
  scr_dbg(1,"scr_flush_async_test called @ %s:%d", __FILE__, __LINE__);
  /* assume the transfer is complete */
  int transfer_complete = 1;

  /* have master on each node check whether the flush is complete */
  double bytes_written = 0.0;
  if (scr_storedesc_cntl->rank == 0) {
    /* create a hash to hold the transfer file data */
    scr_hash* hash = scr_hash_new();

    /* read transfer file with lock */
    if (scr_hash_read_with_lock(scr_transfer_file, hash) == SCR_SUCCESS) {
      /* test each file listed in the transfer hash */
      if (scr_flush_async_file_test(hash, &bytes_written) != SCR_SUCCESS) {
        transfer_complete = 0;
      }
    } else {
      /* failed to read the transfer file, can't determine whether the flush is complete */
      transfer_complete = 0;
    }

    /* free the hash */
    scr_hash_delete(&hash);
  }

  /* compute the total number of bytes written */
  MPI_Allreduce(&bytes_written, bytes, 1, MPI_DOUBLE, MPI_SUM, scr_comm_world);

  /* determine whether the transfer is complete on all tasks */
  if (scr_alltrue(transfer_complete)) {
    if (scr_my_rank_world == 0) {
      scr_dbg(0, "#demo SCR async daemon successfully transferred dset %d", id);
    }
    return SCR_SUCCESS;
  }
  return SCR_FAILURE;
}

/* complete the flush from cache to parallel file system */
int scr_flush_async_complete(scr_filemap* map, int id)
{
#ifdef HAVE_LIBCPPR
  return scr_cppr_flush_async_complete(map, id);
#endif
  int flushed = SCR_SUCCESS;

  /* if user has disabled flush, return failure */
  if (scr_flush <= 0) {
    return SCR_FAILURE;
  }

  /* TODO: have master tell each rank on node whether its files were written successfully */
  scr_dbg(1,"scr_flush_async_complete called @ %s:%d", __FILE__, __LINE__);
  /* allocate structure to hold metadata info */
  scr_hash* data = scr_hash_new();

  /* fill in metadata info for the files this process flushed */
  scr_hash* files = scr_hash_get(scr_flush_async_file_list, SCR_KEY_FILE);
  scr_hash_elem* elem = NULL;
  for (elem = scr_hash_elem_first(files);
       elem != NULL;
       elem = scr_hash_elem_next(elem))
  {
    /* get the filename */
    char* file = scr_hash_elem_key(elem);

    /* get the hash for this file */
    scr_hash* hash = scr_hash_elem_hash(elem);

    /* record the filename in the hash, and get reference to a hash for this file */
    scr_path* path_file = scr_path_from_str(file);
    scr_path_basename(path_file);
    char* name = scr_path_strdup(path_file);
    scr_hash* file_hash = scr_hash_set_kv(data, SCR_SUMMARY_6_KEY_FILE, name);
    scr_free(&name);
    scr_path_delete(&path_file);

    /* TODO: check that this file was written successfully */

    /* get meta data for this file */
    scr_meta* meta = scr_hash_get(hash, SCR_KEY_META);

    /* successfully flushed this file, record the filesize */
    unsigned long filesize = 0;
    if (scr_meta_get_filesize(meta, &filesize) == SCR_SUCCESS) {
      scr_hash_util_set_bytecount(file_hash, SCR_SUMMARY_6_KEY_SIZE, filesize);
    }

    /* record the crc32 if one was computed */
    uLong flush_crc32;
    if (scr_meta_get_crc32(meta, &flush_crc32) == SCR_SUCCESS) {
      scr_hash_util_set_crc32(file_hash, SCR_SUMMARY_6_KEY_CRC, flush_crc32);
    }
  }

  /* write summary file */
  if (scr_flush_complete(id, scr_flush_async_file_list, data) != SCR_SUCCESS) {
    flushed = SCR_FAILURE;
  }

  /* have master on each node remove files from the transfer file */
  if (scr_storedesc_cntl->rank == 0) {
    /* get a hash to read from the file */
    scr_hash* transfer_hash = scr_hash_new();

    /* lock the transfer file, open it, and read it into the hash */
    int fd = -1;
    scr_hash_lock_open_read(scr_transfer_file, &fd, transfer_hash);

    /* remove files from the list */
    scr_flush_async_file_dequeue(transfer_hash, scr_flush_async_hash);

    /* set the STOP command */
    scr_hash_util_set_str(transfer_hash, SCR_TRANSFER_KEY_COMMAND, SCR_TRANSFER_KEY_COMMAND_STOP);

    /* write the hash back to the file */
    scr_hash_write_close_unlock(scr_transfer_file, &fd, transfer_hash);

    /* delete the hash */
    scr_hash_delete(&transfer_hash);
  }

  /* mark that we've stopped the flush */
  scr_flush_async_in_progress = 0;
  scr_flush_file_location_unset(id, SCR_FLUSH_KEY_LOCATION_FLUSHING);

  /* free data structures */
  scr_hash_delete(&data);

  /* free the file list for this checkpoint */
  scr_hash_delete(&scr_flush_async_hash);
  scr_hash_delete(&scr_flush_async_file_list);
  scr_flush_async_hash      = NULL;
  scr_flush_async_file_list = NULL;

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
int scr_flush_async_wait(scr_filemap* map)
{
#ifdef HAVE_LIBCPPR
  return scr_cppr_flush_async_wait(map);
#endif
  if (scr_flush_async_in_progress) {
    while (scr_flush_file_is_flushing(scr_flush_async_dataset_id)) {
      /* test whether the flush has completed, and if so complete the flush */
      double bytes = 0.0;
      if (scr_flush_async_test(map, scr_flush_async_dataset_id, &bytes) == SCR_SUCCESS) {
        /* complete the flush */
        scr_flush_async_complete(map, scr_flush_async_dataset_id);
      } else {
        /* otherwise, sleep to get out of the way */
        if (scr_my_rank_world == 0) {
          scr_dbg(1, "Flush of checkpoint %d is %d%% complete",
            scr_flush_async_dataset_id,
            (int) (bytes / scr_flush_async_bytes * 100.0)
          );
        }
        usleep(10*1000*1000);
      }
    }
  }
  return SCR_SUCCESS;
}

/* start any processes for later asynchronous flush operations */
int scr_flush_async_init(){
    return SCR_SUCCESS;
}

/* stop all ongoing asynchronous flush operations */
int scr_flush_async_finalize()
{
#ifdef HAVE_LIBCPPR
        return SCR_SUCCESS;
#endif
  /* if user has disabled flush, return failure */
  if (scr_flush <= 0) {
    return SCR_FAILURE;
  }

  /* this may take a while, so tell user what we're doing */
  if (scr_my_rank_world == 0) {
    scr_dbg(1, "scr_flush_async_shutdown: shutdown async procs");
  }

  /* write stop command to transfer file */
  scr_flush_async_command_set(SCR_TRANSFER_KEY_COMMAND_EXIT);

  /* wait until all tasks know the transfer is shutdown */
  scr_flush_async_state_wait(SCR_TRANSFER_KEY_STATE_EXIT);

  MPI_Barrier(scr_comm_world);
  return SCR_SUCCESS;
}
