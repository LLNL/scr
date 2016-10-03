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

static time_t    scr_flush_async_timestamp_start;  /* records the time the async flush started */
static double    scr_flush_async_time_start;       /* records the time the async flush started */
static scr_hash* scr_flush_async_file_list = NULL; /* tracks list of files written with flush */
static scr_hash* scr_flush_async_hash = NULL;      /* tracks list of files written with flush */
static int       scr_flush_async_num_files = 0;    /* records the number of files this process must flush */

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
      scr_hash* state_hash = scr_hash_get_kv(hash, SCR_TRANSFER_KEY_STATE, state);
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

/* start an asynchronous flush from cache to parallel file system under SCR_PREFIX */
int scr_flush_async_start(scr_filemap* map, int id)
{
  /* if user has disabled flush, return failure */
  if (scr_flush <= 0) {
    return SCR_FAILURE;
  }

  /* if we don't need a flush, return right away with success */
  if (! scr_bool_need_flush(id)) {
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
      scr_log_event("ASYNC FLUSH STARTED", NULL, &id, &scr_flush_async_timestamp_start, NULL);
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
        scr_log_event("ASYNC FLUSH FAILED", "Failed to prepare flush", &id, &now, &time_diff);
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
    if (scr_hash_util_get_str(file_hash, SCR_KEY_PATH, &dest_dir) != SCR_SUCCESS) {
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

    /* add this file to the hash, and add its filesize to the number of bytes written */
    scr_hash* transfer_file_hash = scr_hash_set_kv(scr_flush_async_hash, SCR_TRANSFER_KEY_FILES, file);
    if (file_hash != NULL) {
      /* break file into path and name components */
      scr_path* path_dest_file = scr_path_from_str(file);
      scr_path_basename(path_dest_file);
      scr_path_prepend_str(path_dest_file, dest_dir);
      char* dest_file = scr_path_strdup(path_dest_file);
    
      scr_hash_util_set_str(transfer_file_hash, SCR_TRANSFER_KEY_DESTINATION,   dest_file);
      scr_hash_util_set_bytecount(transfer_file_hash, SCR_TRANSFER_KEY_SIZE,    filesize);
      scr_hash_util_set_bytecount(transfer_file_hash, SCR_TRANSFER_KEY_WRITTEN, 0);

      /* delete path and string for the file name */
      scr_free(&dest_file);
      scr_path_delete(&path_dest_file);
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
    if (scr_hash_util_get_double(hash, SCR_TRANSFER_KEY_BW, &bw) != SCR_SUCCESS) {
      bw = (double) scr_flush_async_bw / (double) writers;
      scr_hash_util_set_double(hash, SCR_TRANSFER_KEY_BW, bw);
    }

    /* set PERCENT if it's not already set */
    double percent;
    if (scr_hash_util_get_double(hash, SCR_TRANSFER_KEY_PERCENT, &percent) != SCR_SUCCESS) {
      scr_hash_util_set_double(hash, SCR_TRANSFER_KEY_PERCENT, scr_flush_async_percent);
    }

    /* set the RUN command */
    scr_hash_util_set_str(hash, SCR_TRANSFER_KEY_COMMAND, SCR_TRANSFER_KEY_COMMAND_RUN);

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
  MPI_Allreduce(&my_bytes, &scr_flush_async_bytes, 1, MPI_DOUBLE, MPI_SUM, scr_comm_world);

  /* TODO: start transfer thread / process */

  /* make sure all processes have started before we leave */
  MPI_Barrier(scr_comm_world);

  return SCR_SUCCESS;
}

/* check whether the flush from cache to parallel file system has completed */
int scr_flush_async_test(scr_filemap* map, int id, double* bytes)
{
  /* initialize bytes to 0 */
  *bytes = 0.0;

  /* if user has disabled flush, return failure */
  if (scr_flush <= 0) {
    return SCR_FAILURE;
  }

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
    return SCR_SUCCESS;
  }
  return SCR_FAILURE;
}

/* complete the flush from cache to parallel file system */
int scr_flush_async_complete(scr_filemap* map, int id)
{
  int flushed = SCR_SUCCESS;

  /* if user has disabled flush, return failure */
  if (scr_flush <= 0) {
    return SCR_FAILURE;
  }

  /* TODO: have master tell each rank on node whether its files were written successfully */

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
  if (scr_flush_async_in_progress) {
    while (scr_bool_is_flushing(scr_flush_async_dataset_id)) {
      /* test whether the flush has completed, and if so complete the flush */
      double bytes = 0.0;
      if (scr_flush_async_test(map, scr_flush_async_dataset_id, &bytes) == SCR_SUCCESS) {
        /* complete the flush */
        scr_flush_async_complete(map, scr_flush_async_dataset_id);
      } else {
        /* otherwise, sleep to get out of the way */
        if (scr_my_rank_world == 0) {
          scr_dbg(1, "Flush of checkpoint %d is %d%% complete",
                  scr_flush_async_dataset_id, (int) (bytes / scr_flush_async_bytes * 100.0)
          );
        }
        usleep(10*1000*1000);
      }
    }
  }
  return SCR_SUCCESS;
}
