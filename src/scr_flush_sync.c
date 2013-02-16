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

/*
=========================================
Synchronous flush functions
=========================================
*/

/* flushes file named in src_file to dst_dir and fills in meta based on flush,
 * returns success of flush */
static int scr_flush_a_file(const char* src_file, const char* dst_dir, scr_meta* meta)
{
  int flushed = SCR_SUCCESS;
  int tmp_rc;

  /* build full name to destination file */
  scr_path* dst_path = scr_path_from_str(src_file);
  scr_path_basename(dst_path);
  scr_path_prepend_str(dst_path, dst_dir);
  scr_path_reduce(dst_path);
  char* dst_file = scr_path_strdup(dst_path);

  /* copy file */
  int crc_valid = 0;
  uLong crc;
  uLong* crc_p = NULL;
  if (scr_crc_on_flush) {
    crc_valid = 1;
    crc_p = &crc;
  }
  tmp_rc = scr_file_copy(src_file, dst_file, scr_file_buf_size, crc_p);
  if (tmp_rc != SCR_SUCCESS) {
    crc_valid = 0;
    flushed = SCR_FAILURE;
  }
  scr_dbg(2, "scr_flush_a_file: Read and copied %s to %s with success code %d @ %s:%d",
    src_file, dst_file, tmp_rc, __FILE__, __LINE__
  );

  /* if file has crc32, check it against the one computed during the copy,
   * otherwise if scr_crc_on_flush is set, record crc32 */
  if (crc_valid) {
    uLong crc_meta;
    if (scr_meta_get_crc32(meta, &crc_meta) == SCR_SUCCESS) {
      if (crc != crc_meta) { 
        /* detected a crc mismatch during the copy */

        /* TODO: unlink the copied file */
        /* scr_file_unlink(dst_file); */

        /* mark the file as invalid */
        scr_meta_set_complete(meta, 0);

        flushed = SCR_FAILURE;
        scr_err("scr_flush_a_file: CRC32 mismatch detected when flushing file %s to %s @ %s:%d",
          src_file, dst_file, __FILE__, __LINE__
        );

        /* TODO: would be good to log this, but right now only rank 0 can write log entries */
        /*
        if (scr_log_enable) {
          time_t now = scr_log_seconds();
          scr_log_event("CRC32 MISMATCH", dst_file, NULL, &now, NULL);
        }
        */
      }
    } else {
      /* the crc was not already in the metafile, but we just computed it, so set it */
      scr_meta_set_crc32(meta, crc);
    }
  }

  /* TODO: check that written filesize matches expected filesize */

  /* fill out meta data, set complete field based on flush success */
  /* (we don't update the meta file here, since perhaps the file in cache is ok and only the flush failed) */
  int complete = (flushed == SCR_SUCCESS);
  scr_meta_set_complete(meta, complete);

  /* free destination file string and path */
  scr_free(&dst_file);
  scr_path_delete(&dst_path);

  return flushed;
}

/* given a filename, its meta data, its list of segments, and list of destination containers,
 * copy file to container files */
static int scr_flush_file_to_containers(const char* file, scr_meta* meta, scr_hash* segments, const scr_hash* containers)
{
  /* check that we got something for a source file */
  if (file == NULL || strcmp(file, "") == 0) {
    scr_err("Invalid source file @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* check that our other arguments are valid */
  if (meta == NULL || segments == NULL || containers == NULL) {
    scr_err("Invalid metadata, segments, or container @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* get mode for each file */
  mode_t mode_file = scr_getmode(1, 1, 0);

  /* open the file for reading */
  int fd_src = scr_open(file, O_RDONLY);
  if (fd_src < 0) {
    scr_err("Opening file to copy: scr_open(%s) errno=%d %s @ %s:%d",
            file, errno, strerror(errno), __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* TODO:
  posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED | POSIX_FADV_SEQUENTIAL)
  that tells the kernel that you don't ever need the pages
  from the file again, and it won't bother keeping them in the page cache.
  */
  posix_fadvise(fd_src, 0, 0, POSIX_FADV_DONTNEED | POSIX_FADV_SEQUENTIAL);

  /* get the buffer size we'll use to write to the file */
  unsigned long buf_size = scr_file_buf_size;

  /* allocate buffer to read in file chunks */
  char* buf = (char*) malloc(buf_size);
  if (buf == NULL) {
    scr_err("Allocating memory: malloc(%llu) errno=%d %s @ %s:%d",
            buf_size, errno, strerror(errno), __FILE__, __LINE__
    );
    scr_close(file, fd_src);
    return SCR_FAILURE;
  }

  /* initialize crc value */
  uLong crc;
  if (scr_crc_on_flush) {
    crc = crc32(0L, Z_NULL, 0);
  }

  int rc = SCR_SUCCESS;

  /* write out each segment */
  scr_hash_sort_int(segments, SCR_HASH_SORT_ASCENDING);
  scr_hash_elem* elem;
  for (elem = scr_hash_elem_first(segments);
       elem != NULL;
       elem = scr_hash_elem_next(elem))
  {
    /* get the container info for this segment */
    scr_hash* hash = scr_hash_elem_hash(elem);

    /* get the offset into the container and the length of the segment (both in bytes) */
    char* container_name;
    unsigned long container_size, container_offset, segment_length;
    if (scr_container_get_name_size_offset_length(hash, containers,
      &container_name, &container_size, &container_offset, &segment_length) != SCR_SUCCESS)
    {
      scr_err("Failed to get segment offset and length @ %s:%d",
              __FILE__, __LINE__
      );
      rc = SCR_FAILURE;
      break;
    }

    /* open container file for writing -- we don't truncate here because more than one
     * process may be writing to the same file */
    int fd_container = scr_open(container_name, O_WRONLY | O_CREAT, mode_file);
    if (fd_container < 0) {
      scr_err("Opening file for writing: scr_open(%s) errno=%d %s @ %s:%d",
              container_name, errno, strerror(errno), __FILE__, __LINE__
      );
      rc = SCR_FAILURE;
      break;
    }

    /* TODO:
    posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED | POSIX_FADV_SEQUENTIAL)
    that tells the kernel that you don't ever need the pages
    from the file again, and it won't bother keeping them in the page cache.
    */
    posix_fadvise(fd_container, 0, 0, POSIX_FADV_DONTNEED | POSIX_FADV_SEQUENTIAL);

    /* seek to offset within container */
    off_t pos = (off_t) container_offset;
    if (lseek(fd_container, pos, SEEK_SET) == (off_t)-1) {
      /* our seek failed, return an error */
      scr_err("Failed to seek to byte %lu in %s @ %s:%d",
              pos, container_name, __FILE__, __LINE__
      );
      rc = SCR_FAILURE;
      break;
    }
    
    /* copy data from file into container in chunks */
    unsigned long remaining = segment_length;
    while (remaining > 0) {
      /* read / write up to buf_size bytes at a time from file */
      unsigned long count = remaining;
      if (count > buf_size) {
        count = buf_size;
      }

      /* attempt to read buf_size bytes from file */
      int nread = scr_read_attempt(file, fd_src, buf, count);

      /* if we read some bytes, write them out */
      if (nread > 0) {
        /* optionally compute crc value as we go */
        if (scr_crc_on_flush) {
          crc = crc32(crc, (const Bytef*) buf, (uInt) nread);
        }

        /* write our nread bytes out */
        int nwrite = scr_write_attempt(container_name, fd_container, buf, nread);

        /* check for a write error or a short write */
        if (nwrite != nread) {
          /* write had a problem, stop copying and return an error */
          rc = SCR_FAILURE;
          break;
        }

        /* subtract the bytes we've processed from the number remaining */
        remaining -= (unsigned long) nread;
      }

      /* assume a short read is an error */
      if (nread < count) {
        /* read had a problem, stop copying and return an error */
        rc = SCR_FAILURE;
        break;
      }

      /* check for a read error, stop copying and return an error */
      if (nread < 0) {
        /* read had a problem, stop copying and return an error */
        rc = SCR_FAILURE;
        break;
      }
    }

    /* close container */
    if (scr_close(container_name, fd_container) != SCR_SUCCESS) {
      rc = SCR_FAILURE;
    }
  }

  /* close the source file */
  if (scr_close(file, fd_src) != SCR_SUCCESS) {
    rc = SCR_FAILURE;
  }

  /* free buffer */
  scr_free(&buf);

  /* verify / set crc value */
  if (rc == SCR_SUCCESS) {
    uLong crc2;
    if (scr_crc_on_flush) {
      if (scr_meta_get_crc32(meta, &crc2) == SCR_SUCCESS) {
        /* if a crc is already set in the meta data, check that we computed the same value */
        if (crc != crc2) {
          scr_err("CRC32 mismatch detected when flushing file %s @ %s:%d",
                  file, __FILE__, __LINE__
          );
          rc = SCR_FAILURE;
        }
      } else {
        /* if there is no crc set, let's set it now */
        scr_meta_set_crc32(meta, crc);
      }
    }
  }

  return rc;
}

/* flush files specified in list, and record corresponding entries for summary file */
static int scr_flush_files_list(scr_hash* file_list, scr_hash* summary)
{
  /* assume we will succeed in this flush */
  int rc = SCR_SUCCESS;

  /* lookup path to summary file */
  char* summary_dir;
  if (scr_hash_util_get_str(file_list, SCR_KEY_PATH, &summary_dir) != SCR_SUCCESS) {
    scr_abort(-1, "Failed to get summary file directory from file list @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* create summary path */
  scr_path* path_summary_dir = scr_path_from_str(summary_dir);

  /* get pointer to containers hash and copy into summary info if one exists */
  scr_hash* containers = scr_hash_get(file_list, SCR_SUMMARY_6_KEY_CONTAINER);
  if (containers != NULL) {
    scr_hash* containers_copy = scr_hash_new();
    scr_hash_merge(containers_copy, containers);
    scr_hash_set(summary, SCR_SUMMARY_6_KEY_CONTAINER, containers_copy);
  }

  /* flush each of my files and fill in summary data structure */
  scr_hash_elem* elem = NULL;
  scr_hash* files = scr_hash_get(file_list, SCR_KEY_FILE);
  for (elem = scr_hash_elem_first(files);
       elem != NULL;
       elem = scr_hash_elem_next(elem))
  {
    /* get the filename */
    char* file = scr_hash_elem_key(elem);

    /* convert file to path and extract name of file */
    scr_path* path_name = scr_path_from_str(file);
    scr_path_basename(path_name);

    /* get the hash for this element */
    scr_hash* hash = scr_hash_elem_hash(elem);

    /* get meta data for this file */
    scr_meta* meta = scr_hash_get(hash, SCR_KEY_META);

    /* if containers are defined, we flush the file to its containers,
     * otherwise we copy the file out as is */
    if (containers != NULL) {
      /* TODO: PRESERVE get original filename here */

      /* add this file to the summary file */
      char* name = scr_path_strdup(path_name);
      scr_hash* file_hash = scr_hash_set_kv(summary, SCR_SUMMARY_6_KEY_FILE, name);
      scr_free(&name);

      /* get segments hash for this file */
      scr_hash* segments = scr_hash_get(hash, SCR_SUMMARY_6_KEY_SEGMENT);

      /* flush the file to the containers listed in its segmenets */
      if (scr_flush_file_to_containers(file, meta, segments, containers) == SCR_SUCCESS) {
        /* successfully flushed this file, record the filesize */
        unsigned long filesize = 0;
        if (scr_meta_get_filesize(meta, &filesize) == SCR_SUCCESS) {
          scr_hash_util_set_bytecount(file_hash, SCR_SUMMARY_6_KEY_SIZE, filesize);
        }

        /* record the crc32 if one was computed */
        uLong crc = 0;
        if (scr_meta_get_crc32(meta, &crc) == SCR_SUCCESS) {
          scr_hash_util_set_crc32(file_hash, SCR_SUMMARY_6_KEY_CRC, crc);
        }

        /* record segment information in summary file */
        scr_hash* segments_copy = scr_hash_new();
        scr_hash_merge(segments_copy, segments);
        scr_hash_set(file_hash, SCR_SUMMARY_6_KEY_SEGMENT, segments_copy);
      } else {
        /* the flush failed */
        rc = SCR_FAILURE;

        /* explicitly mark file as incomplete */
        scr_hash_set_kv_int(file_hash, SCR_SUMMARY_6_KEY_COMPLETE, 0);
      }
    } else {
      /* get directory to flush file to */
      char* dir;
      if (scr_hash_util_get_str(hash, SCR_KEY_PATH, &dir) == SCR_SUCCESS) {
        /* create full path of destination file */
        scr_path* path_full = scr_path_from_str(dir);
        scr_path_append(path_full, path_name);

        /* get relative path to flushed file from directory containing summary file */
        scr_path* path_relative = scr_path_relative(path_summary_dir, path_full);
        if (! scr_path_is_null(path_relative)) {
          /* record the name of the file in the summary hash, and get reference to a hash for this file */
          char* name = scr_path_strdup(path_relative);
          scr_hash* file_hash = scr_hash_set_kv(summary, SCR_SUMMARY_6_KEY_FILE, name);
          scr_free(&name);

          /* flush the file and fill in the meta data for this file */
          if (scr_flush_a_file(file, dir, meta) == SCR_SUCCESS) {
            /* successfully flushed this file, record the filesize */
            unsigned long filesize = 0;
            if (scr_meta_get_filesize(meta, &filesize) == SCR_SUCCESS) {
              scr_hash_util_set_bytecount(file_hash, SCR_SUMMARY_6_KEY_SIZE, filesize);
            }

            /* record the crc32 if one was computed */
            uLong crc = 0;
            if (scr_meta_get_crc32(meta, &crc) == SCR_SUCCESS) {
              scr_hash_util_set_crc32(file_hash, SCR_SUMMARY_6_KEY_CRC, crc);
            }
          } else {
            /* the flush failed */
            rc = SCR_FAILURE;

            /* explicitly mark incomplete files */
            scr_hash_set_kv_int(file_hash, SCR_SUMMARY_6_KEY_COMPLETE, 0);
          }
        } else {
          scr_abort(-1, "Failed to get relative path to directory %s from %s @ %s:%d",
            dir, summary_dir, __FILE__, __LINE__
          );
        }

        /* free relative and full paths */
        scr_path_delete(&path_relative);
        scr_path_delete(&path_full);
      } else {
        scr_abort(-1, "Failed to read directory to flush file to @ %s:%d",
          __FILE__, __LINE__
        );
      }
    }

    /* free the file name path */
    scr_path_delete(&path_name);
  }

  /* free summary dir path */
  scr_path_delete(&path_summary_dir);

  return rc;
}

/* flushes data for files specified in file_list (with flow control),
 * and records status of each file in data */
static int scr_flush_data(scr_hash* file_list, scr_hash* data)
{
  int flushed = SCR_SUCCESS;

  /* flow control the write among processes */
  if (scr_my_rank_world == 0) {
    /* first, flush each of my files and fill in meta data structure */
    if (scr_flush_files_list(file_list, data) != SCR_SUCCESS) {
      flushed = SCR_FAILURE;
    }

    /* now, have a sliding window of w processes write simultaneously */
    int w = scr_flush_width;
    if (w > (scr_ranks_world - 1)) {
      w = scr_ranks_world - 1;
    }

    /* allocate MPI_Request arrays and an array of ints */
    int*         flags = (int*)         malloc(2 * w * sizeof(int));
    MPI_Request* req   = (MPI_Request*) malloc(2 * w * sizeof(MPI_Request));
    MPI_Status status;

    int i = 1;
    int outstanding = 0;
    int index = 0;
    while (i < scr_ranks_world || outstanding > 0) {
      /* issue up to w outstanding sends and receives */
      while (i < scr_ranks_world && outstanding < w) {
        /* post a receive for the response message we'll get back when rank i is done */
        MPI_Irecv(&flags[w + index], 1, MPI_INT, i, 0, scr_comm_world, &req[w + index]);

        /* post a send to tell rank i to start */
        flags[index] = flushed;
        MPI_Isend(&flags[index], 1, MPI_INT, i, 0, scr_comm_world, &req[index]);

        /* update the number of outstanding requests */
        i++;
        outstanding++;
        index++;
      }

      /* wait to hear back from any rank */
      MPI_Waitany(w, &req[w], &index, &status);

      /* someone responded, the send to this rank should also be done, so complete it */
      MPI_Wait(&req[index], &status);

      /* determine whether this rank flushed its file successfully */
      if (flags[w + index] != SCR_SUCCESS) {
        flushed = SCR_FAILURE;
      }

      /* one less request outstanding now */
      outstanding--;
    }

    /* free the MPI_Request arrays */
    scr_free(&req);
    scr_free(&flags);
  } else {
    /* receive signal to start */
    int start = 0;
    MPI_Status status;
    MPI_Recv(&start, 1, MPI_INT, 0, 0, scr_comm_world, &status);

    /* flush files if we've had success so far, otherwise skip the flush and return failure */
    if (start == SCR_SUCCESS) {
      /* flush each of my files and fill in meta data strucutre */
      if (scr_flush_files_list(file_list, data) != SCR_SUCCESS) {
        flushed = SCR_FAILURE;
      }
    } else {
      /* someone failed before we even started, so don't bother */
      flushed = SCR_FAILURE;
    }

    /* send message to rank 0 to report that we're done */
    MPI_Send(&flushed, 1, MPI_INT, 0, 0, scr_comm_world);
  }

  /* determine whether everyone wrote their files ok */
  if (scr_alltrue((flushed == SCR_SUCCESS))) {
    return SCR_SUCCESS;
  }
  return SCR_FAILURE;
}

/* flush files from cache to parallel file system under SCR_PREFIX */
int scr_flush_sync(scr_filemap* map, int id)
{
  int flushed = SCR_SUCCESS;

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
    scr_flush_async_wait(map);
    
    /* the flush we just waited on could be the requested dataset,
     * so perhaps we're already done */
    if (! scr_bool_need_flush(id)) {
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
  scr_hash* file_list = scr_hash_new();
  if (scr_flush_prepare(map, id, file_list) != SCR_SUCCESS) {
    flushed = SCR_FAILURE;
  }

  /* write the data out to files */
  scr_hash* data = scr_hash_new();
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
      scr_filemap_get_dataset(map, id, scr_my_rank_world, dataset);

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
  scr_hash_delete(&data);
  scr_hash_delete(&file_list);

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
