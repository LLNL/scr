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

/* for file name listed in meta, fetch that file from src_dir and store
 * a copy in dst_dir, record full path to copy in newfile, and
 * return whether operation succeeded */
static int scr_fetch_file(
  const char* dst_file,
  const char* src_dir,
  const scr_meta* meta)
{
  int rc = SCR_SUCCESS;

  /* build full path to source file */
  scr_path* path_src_file = scr_path_from_str(dst_file);
  scr_path_basename(path_src_file);
  scr_path_prepend_str(path_src_file, src_dir);
  char* src_file = scr_path_strdup(path_src_file);

  /* fetch the file */
  uLong crc;
  uLong* crc_p = NULL;
  if (scr_crc_on_flush) {
    crc_p = &crc;
  }
  rc = scr_file_copy(src_file, dst_file, scr_file_buf_size, crc_p);

  /* check that crc matches crc stored in meta */
  uLong meta_crc;
  if (scr_meta_get_crc32(meta, &meta_crc) == SCR_SUCCESS) {
    if (rc == SCR_SUCCESS && scr_crc_on_flush && crc != meta_crc) {
      rc = SCR_FAILURE;
      scr_err("CRC32 mismatch detected when fetching file from %s to %s @ %s:%d",
        src_file, dst_file, __FILE__, __LINE__
      );

      /* TODO: would be good to log this, but right now only rank 0
       * can write log entries */
      /*
      if (scr_log_enable) {
        time_t now = scr_log_seconds();
        scr_log_event("CRC32 MISMATCH", filename, NULL, &now, NULL);
      }
      */
    }
  }

  /* free path and string for source file */
  scr_free(&src_file);
  scr_path_delete(&path_src_file);

  return rc;
}

/* extract container name, size, offset, and length values
 * for container that holds the specified segment */
int scr_container_get_name_size_offset_length(
  const scr_hash* segment, const scr_hash* containers,
  char** name, unsigned long* size,
  unsigned long* offset, unsigned long* length)
{
  /* check that our parameters are valid */
  if (segment == NULL || containers == NULL ||
      name == NULL || size == NULL || offset == NULL || length == NULL)
  {
    return SCR_FAILURE;
  }

  /* lookup the segment length */
  if (scr_hash_util_get_bytecount(segment, SCR_SUMMARY_6_KEY_LENGTH, length) != SCR_SUCCESS) {
    return SCR_FAILURE;
  }

  /* get the container hash */
  scr_hash* container = scr_hash_get(segment, SCR_SUMMARY_6_KEY_CONTAINER);

  /* lookup id for container */
  int id;
  if (scr_hash_util_get_int(container, SCR_SUMMARY_6_KEY_ID, &id) != SCR_SUCCESS) {
    return SCR_FAILURE;
  }

  /* lookup the offset value */
  if (scr_hash_util_get_bytecount(container, SCR_SUMMARY_6_KEY_OFFSET, offset) != SCR_SUCCESS) {
    return SCR_FAILURE;
  }

  /* get container with matching id from containers list */
  scr_hash* info = scr_hash_getf(containers, "%d", id);

  /* get name of container */
  if (scr_hash_util_get_str(info, SCR_KEY_NAME, name) != SCR_SUCCESS) {
    return SCR_FAILURE;
  }

  /* get size of container */
  if (scr_hash_util_get_bytecount(info, SCR_KEY_SIZE, size) != SCR_SUCCESS) {
    return SCR_FAILURE;
  }

  return SCR_SUCCESS;
}

/* fetch file in meta from its list of segments and containers and
 * write to specified file name, return whether operation succeeded */
static int scr_fetch_file_from_containers(
  const char* file,
  scr_meta* meta,
  scr_hash* segments,
  const scr_hash* containers)
{
  unsigned long buf_size = scr_file_buf_size;

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

  /* open the file for writing */
  mode_t mode_file = scr_getmode(1, 1, 0);
  int fd_src = scr_open(file, O_WRONLY | O_CREAT | O_TRUNC, mode_file);
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

  /* TODO: align this buffer */
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

  /* read in each segment */
  scr_hash_sort_int(segments, SCR_HASH_SORT_ASCENDING);
  scr_hash_elem* elem;
  for (elem = scr_hash_elem_first(segments);
       elem != NULL;
       elem = scr_hash_elem_next(elem))
  {
    /* get the container info for this segment */
    scr_hash* hash = scr_hash_elem_hash(elem);

    /* get the offset into the container and the length of the
     * segment (both in bytes) */
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

    /* open container file for reading */
    int fd_container = scr_open(container_name, O_RDONLY);
    if (fd_container < 0) {
      scr_err("Opening file for reading: scr_open(%s) errno=%d %s @ %s:%d",
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
    
    /* copy data from container into file in chunks */
    unsigned long remaining = segment_length;
    while (remaining > 0) {
      /* read / write up to buf_size bytes at a time from file */
      unsigned long count = remaining;
      if (count > buf_size) {
        count = buf_size;
      }

      /* attempt to read buf_size bytes from container */
      int nread = scr_read_attempt(container_name, fd_container, buf, count);

      /* if we read some bytes, write them out */
      if (nread > 0) {
        /* optionally compute crc value as we go */
        if (scr_crc_on_flush) {
          crc = crc32(crc, (const Bytef*) buf, (uInt) nread);
        }

        /* write our nread bytes out */
        int nwrite = scr_write_attempt(file, fd_src, buf, nread);

        /* check for a write error or a short write */
        if (nwrite != nread) {
          /* write had a problem, stop copying and return an error */
          rc = SCR_FAILURE;
          break;
        }

        /* subtract bytes we've processed from the number remaining */
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

  /* verify crc value */
  if (rc == SCR_SUCCESS) {
    uLong crc2;
    if (scr_crc_on_flush) {
      if (scr_meta_get_crc32(meta, &crc2) == SCR_SUCCESS) {
        /* if a crc is already set in the meta data, check that we
         * computed the same value */
        if (crc != crc2) {
          scr_err("CRC32 mismatch detected when fetching file %s @ %s:%d",
            file, __FILE__, __LINE__
          );
          rc = SCR_FAILURE;
        }
      }
    }
  }

  return rc;
}

/* fetch files listed in hash into specified cache directory,
 * update filemap and fill in total number of bytes fetched,
 * returns SCR_SUCCESS if successful */
static int scr_fetch_files_list(
  const scr_hash* file_list,
  const char* dir,
  scr_filemap* map)
{
  /* assume we'll succeed in fetching our files */
  int rc = SCR_SUCCESS;

  /* assume we don't have any files to fetch */
  int my_num_files = 0;

  /* get dataset id */
  int id;
  scr_dataset* dataset = scr_hash_get(file_list, SCR_KEY_DATASET);
  scr_dataset_get_id(dataset, &id);

  /* get pointer to containers hash */
  scr_hash* containers = scr_hash_get(file_list, SCR_SUMMARY_6_KEY_CONTAINER);

  /* now iterate through the file list and fetch each file */
  scr_hash_elem* file_elem = NULL;
  scr_hash* files = scr_hash_get(file_list, SCR_KEY_FILE);
  for (file_elem = scr_hash_elem_first(files);
       file_elem != NULL;
       file_elem = scr_hash_elem_next(file_elem))
  {
    /* get the filename */
    char* file = scr_hash_elem_key(file_elem);

    /* get a pointer to the hash for this file */
    scr_hash* hash = scr_hash_elem_hash(file_elem);

    /* check whether we are supposed to fetch this file */
    /* TODO: this is a hacky way to avoid reading a redundancy file
     * back in under the assumption that it's an original file, which
     * breaks our redundancy computation due to a name conflict on
     * the file names */
    scr_hash_elem* no_fetch_hash = scr_hash_elem_get(hash, SCR_SUMMARY_6_KEY_NOFETCH);
    if (no_fetch_hash != NULL) {
      continue;
    }

    /* increment our file count */
    my_num_files++;

    /* build the destination file name */
    scr_path* path_newfile = scr_path_from_str(file);
    scr_path_basename(path_newfile);
    scr_path_prepend_str(path_newfile, dir);
    char* newfile = scr_path_strdup(path_newfile);
      
    /* add the file to our filemap and write it to disk before creating
     * the file, this way we have a record that it may exist before we
     * actually start to fetch it */
    scr_filemap_add_file(map, id, scr_my_rank_world, newfile);
    scr_filemap_write(scr_map_file, map);

    /* get the file size */
    unsigned long filesize = 0;
    if (scr_hash_util_get_unsigned_long(hash, SCR_KEY_SIZE, &filesize) != SCR_SUCCESS) {
      scr_err("Failed to read file size from summary data @ %s:%d",
        __FILE__, __LINE__
      );
      rc = SCR_FAILURE;

      /* free path and string */
      scr_free(&newfile);
      scr_path_delete(&path_newfile);

      break;
    }

    /* check for a complete flag */
    int complete = 1;
    if (scr_hash_util_get_int(hash, SCR_KEY_COMPLETE, &complete) != SCR_SUCCESS) {
      /* in summary file, the absence of a complete flag on a file
       * implies the file is complete */
      complete = 1;
    }

    /* create a new meta data object for this file */
    scr_meta* meta = scr_meta_new();

    /* set the meta data */
    scr_meta_set_filename(meta, newfile);
    scr_meta_set_filetype(meta, SCR_META_FILE_USER);
    scr_meta_set_filesize(meta, filesize);
    scr_meta_set_complete(meta, 1);
    /* TODODSET: move the ranks field elsewhere, for now it's needed
     * by scr_index.c */
    scr_meta_set_ranks(meta, scr_ranks_world);

    /* get the crc, if set, and add it to the meta data */
    uLong crc;
    if (scr_hash_util_get_crc32(hash, SCR_KEY_CRC, &crc) == SCR_SUCCESS) {
      scr_meta_set_crc32(meta, crc);
    }

    /* fetch file from containers if they are defined, otherwise fetch
     * the native file */
    if (containers != NULL) {
      /* lookup segments hash for this file */
      scr_hash* segments = scr_hash_get(hash, SCR_SUMMARY_6_KEY_SEGMENT);

      /* fetch file from containers */
      if (scr_fetch_file_from_containers(newfile, meta, segments, containers) != SCR_SUCCESS) {
        /* failed to fetch file, mark it as incomplete */
        scr_meta_set_complete(meta, 0);
        rc = SCR_FAILURE;
      }
    } else {
      /* fetch native file, lookup directory for this file */
      char* from_dir;
      if (scr_hash_util_get_str(hash, SCR_KEY_PATH, &from_dir) == SCR_SUCCESS) {
        if (scr_fetch_file(newfile, from_dir, meta) != SCR_SUCCESS) {
          /* failed to fetch file, mark it as incomplete */
          scr_meta_set_complete(meta, 0);
          rc = SCR_FAILURE;
        }
      } else {
        /* failed to read source directory, mark file as incomplete */
        scr_meta_set_complete(meta, 0);
        rc = SCR_FAILURE;
      }
    }

    /* TODODSET: want to write out filemap before we start to fetch
     * each file? */

    /* mark the file as complete */
    scr_filemap_set_meta(map, id, scr_my_rank_world, newfile, meta);

    /* free the meta data object */
    scr_meta_delete(&meta);

    /* free path and string */
    scr_free(&newfile);
    scr_path_delete(&path_newfile);
  }

  /* set the expected number of files for this dataset */
  scr_filemap_set_expected_files(map, id, scr_my_rank_world, my_num_files);
  scr_filemap_write(scr_map_file, map);

  return rc;
}

static int scr_fetch_rank2file_map(
  const scr_path* dataset_path,
  int             depth,
  int*            ptr_valid,
  char**          ptr_file,
  unsigned long*  ptr_offset)
{
  int rc = SCR_SUCCESS;

  /* get local variables so we don't have to deference everything */
  int valid            = *ptr_valid;
  char* file           = *ptr_file;
  unsigned long offset = *ptr_offset;

  /* create a hash to hold section of file */
  scr_hash* hash = scr_hash_new();

  /* if we can read from file do it */
  if (valid) {
    /* open file if we haven't already */
    int fd = scr_open(file, O_RDONLY);
    if (fd >= 0) {
      /* read our segment from the file */
      scr_lseek(file, fd, offset, SEEK_SET);
      ssize_t read_rc = scr_hash_read_fd(file, fd, hash);
      if (read_rc < 0) {
        scr_err("Failed to read from %s @ %s:%d",
          file, __FILE__, __LINE__
        );
        rc = SCR_FAILURE;
      }

      /* close the file */
      scr_close(file, fd);
    } else {
      scr_err("Failed to open rank2file map %s @ %s:%d",
        file, __FILE__, __LINE__
      );
      rc = SCR_FAILURE;
    }
  }

  /* check for read errors */
  if (! scr_alltrue(rc == SCR_SUCCESS)) {
    rc = SCR_FAILURE;
    goto cleanup;
  }

  /* create hashes to exchange data */
  scr_hash* send = scr_hash_new();
  scr_hash* recv = scr_hash_new();

  /* copy rank data into send hash */
  if (valid) {
    scr_hash* rank_hash = scr_hash_get(hash, SCR_SUMMARY_6_KEY_RANK);
    scr_hash_merge(send, rank_hash);
  }

  /* exchange hashes */
  scr_hash_exchange_direction(send, recv, scr_comm_world, SCR_HASH_EXCHANGE_RIGHT);

  /* see if anyone sent us anything */
  int newvalid = 0;
  char* newfile = NULL;
  unsigned long newoffset = 0;
  scr_hash_elem* elem = scr_hash_elem_first(recv);
  if (elem != NULL) {
    /* got something, so now we'll read in the next step */
    newvalid = 1;

    /* get file name we should read */
    scr_hash* elem_hash = scr_hash_elem_hash(elem);
    char* value;
    if (scr_hash_util_get_str(elem_hash, SCR_SUMMARY_6_KEY_FILE, &value)
        == SCR_SUCCESS)
    {
      /* return string of full path to file to caller */
      scr_path* newpath = scr_path_dup(dataset_path);
      scr_path_append_str(newpath, value);
      newfile = scr_path_strdup(newpath);
      scr_path_delete(&newpath);
    } else {
      rc = SCR_FAILURE;
    }

    /* get offset we should start reading from */
    if (scr_hash_util_get_bytecount(elem_hash, SCR_SUMMARY_6_KEY_OFFSET, &newoffset)
        != SCR_SUCCESS)
    {
      rc = SCR_FAILURE;
    }
  }

  /* free the send and receive hashes */
  scr_hash_delete(&recv);
  scr_hash_delete(&send);

  /* get level id, and broadcast it from rank 0,
   * which we assume to be a reader in all steps */
  int level_id = -1;
  if (valid) {
    if (scr_hash_util_get_int(hash, SCR_SUMMARY_6_KEY_LEVEL, &level_id)
        != SCR_SUCCESS)
    {
      rc = SCR_FAILURE;
    }
  }
  MPI_Bcast(&level_id, 1, MPI_INT, 0, scr_comm_world);

  /* check for read errors */
  if (! scr_alltrue(rc == SCR_SUCCESS)) {
    rc = SCR_FAILURE;
    goto cleanup;
  }

  /* set parameters for output or next iteration,
   * we already took care of updating ptr_fd earlier */
  if (valid) {
    scr_free(ptr_file);
  }
  *ptr_valid  = newvalid;
  *ptr_file   = newfile;
  *ptr_offset = newoffset;

  /* recurse if we still have levels to read */
  if (level_id > 1) {
    rc = scr_fetch_rank2file_map(dataset_path, depth+1, ptr_valid, ptr_file, ptr_offset);
  }

cleanup:
  /* free the hash */
  scr_hash_delete(&hash);

  return rc;
}

static int scr_summary_read_mpi(
  const scr_path* dataset_path,
  scr_hash* file_list)
{
  int rc = SCR_SUCCESS;

  /* build path to summary file */
  scr_path* meta_path = scr_path_dup(dataset_path);
  scr_path_append_str(meta_path, ".scr");
  scr_path_reduce(meta_path);

  /* rank 0 reads the summary file */
  scr_hash* header = scr_hash_new();
  if (scr_my_rank_world == 0) {
    /* build path to summary file */
    scr_path* summary_path = scr_path_dup(meta_path);
    scr_path_append_str(summary_path, "summary.scr");
    const char* summary_file = scr_path_strdup(summary_path);

    /* open file for reading */
    int fd = scr_open(summary_file, O_RDONLY);
    if (fd >= 0) {
      /* read summary hash */
      ssize_t header_size = scr_hash_read_fd(summary_file, fd, header);
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
    scr_path_delete(&summary_path);
  }

  /* broadcast success code from rank 0 */
  MPI_Bcast(&rc, 1, MPI_INT, 0, scr_comm_world);
  if (rc != SCR_SUCCESS) {
    goto cleanup;
  }

  /* broadcast the summary hash */
  scr_hash_bcast(header, 0, scr_comm_world);

  /* extract and record the datast in file list */
  scr_hash* dataset_hash = scr_hash_new();
  scr_dataset* dataset = scr_hash_get(header, SCR_SUMMARY_6_KEY_DATASET);
  scr_hash_merge(dataset_hash, dataset);
  scr_hash_set(file_list, SCR_SUMMARY_6_KEY_DATASET, dataset_hash);

  /* TODO: containers */
#if 0
    /* TODO: it's overkill to bcast info for all containers, each proc
     * only really needs to know about the containers that contain its
     * files */

    /* broadcast the container file information if we have any */
    scr_hash* container_hash = scr_hash_new();
    if (scr_my_rank_world == 0) {
      scr_dataset* container = scr_hash_get(summary_hash, SCR_SUMMARY_6_KEY_CONTAINER);
      scr_hash_merge(container_hash, container);
    }
    scr_hash_bcast(container_hash, 0, scr_comm_world);
    if (scr_hash_size(container_hash) > 0) {
      scr_hash_set(file_list, SCR_SUMMARY_6_KEY_CONTAINER, container_hash);
    } else {
      scr_hash_delete(&container_hash);
    }
#endif

  /* build path to rank2file map */
  scr_path* rank2file_path = scr_path_dup(meta_path);
  scr_path_append_str(rank2file_path, "rank2file.scr");

  /* fetch file names and offsets containing file hash data */
  int valid = 0;
  char* file = NULL;
  unsigned long offset = 0;
  if (scr_my_rank_world == 0) {
    /* rank 0 is only valid reader to start with */
    valid  = 1;
    file   = scr_path_strdup(rank2file_path);
    offset = 0;
  }
  if (scr_fetch_rank2file_map(dataset_path, 1, &valid, &file, &offset)
      != SCR_SUCCESS)
  {
    rc = SCR_FAILURE;
  }

  /* create hashes to exchange data */
  scr_hash* send = scr_hash_new();
  scr_hash* recv = scr_hash_new();

  /* read data from file */
  if (valid) {
    /* open file if necessary */
    int fd = scr_open(file, O_RDONLY);
    if (fd >= 0) {
      /* create hash to hold file contents */
      scr_hash* save = scr_hash_new();

      /* read hash from file */
      scr_lseek(file, fd, offset, SEEK_SET);
      ssize_t readsize = scr_hash_read_fd(file, fd, save);
      if (readsize < 0) {
        scr_err("Failed to read rank2file map file %s @ %s:%d",
          file, __FILE__, __LINE__
        );
        rc = SCR_FAILURE;
      }

      /* check that the number of ranks match */
      int ranks = 0;
      scr_hash_util_get_int(save, SCR_SUMMARY_6_KEY_RANKS, &ranks);
      if (ranks != scr_ranks_world) {
        scr_err("Invalid number of ranks in %s, got %d expected %d @ %s:%d",
          file, ranks, scr_ranks_world, __FILE__, __LINE__
        );
        rc = SCR_FAILURE;
      }

      /* delete current send hash, set it to values from file,
       * delete file hash */
      scr_hash_delete(&send);
      send = scr_hash_extract(save, SCR_SUMMARY_6_KEY_RANK);
      scr_hash_delete(&save);

      /* close the file */
      scr_close(file, fd);
    } else {
      scr_err("Failed to open rank2file map %s @ %s:%d",
        file, __FILE__, __LINE__
      );
      rc = SCR_FAILURE;
    }

    /* delete file name string */
    scr_free(&file);
  }

  /* check that everyone read the data ok */
  if (! scr_alltrue(rc == SCR_SUCCESS)) {
    rc = SCR_FAILURE;
    goto cleanup_hashes;
  }

  /* scatter to groups */
  scr_hash_exchange_direction(send, recv, scr_comm_world, SCR_HASH_EXCHANGE_RIGHT);

  /* iterate over the ranks that sent data to us, and set up our
   * list of files */
  scr_hash_elem* elem;
  for (elem = scr_hash_elem_first(recv);
       elem != NULL;
       elem = scr_hash_elem_next(elem))
  {
    /* the key is the source rank, which we don't care about,
     * the info we need is in the element hash */
    scr_hash* elem_hash = scr_hash_elem_hash(elem);

    /* TODO: handle containers */

    /* get pointer to file hash */
    scr_hash* file_hash = scr_hash_get(elem_hash, SCR_SUMMARY_6_KEY_FILE);
    if (file_hash != NULL) {
      /* TODO: parse summary file format */
      scr_hash_merge(file_list, elem_hash);
    } else {
      rc = SCR_FAILURE;
    }
  }

  /* fill in file list parameters */
  if (rc == SCR_SUCCESS) {
    /* if we're not using containers, add PATH entry for each of our
     * files */
    scr_hash* files = scr_hash_get(file_list, SCR_KEY_FILE);
    for (elem = scr_hash_elem_first(files);
         elem != NULL;
         elem = scr_hash_elem_next(elem))
    {
      /* get the file name */
      char* file = scr_hash_elem_key(elem);

      /* combine the file name with the summary directory to build a
       * full path to the file */
      scr_path* path_full = scr_path_dup(dataset_path);
      scr_path_append_str(path_full, file);

      /* subtract off last component to get just the path */
      scr_path_dirname(path_full);
      char* path = scr_path_strdup(path_full);

      /* record path in file list */
      scr_hash* hash = scr_hash_elem_hash(elem);
      scr_hash_util_set_str(hash, SCR_KEY_PATH, path);

      /* free the path and string */
      scr_free(&path);
      scr_path_delete(&path_full);
    }
  }

  /* check that everyone read the data ok */
  if (! scr_alltrue(rc == SCR_SUCCESS)) {
    rc = SCR_FAILURE;
    goto cleanup_hashes;
  }

cleanup_hashes:
  /* delete send and receive hashes */
  scr_hash_delete(&recv);
  scr_hash_delete(&send);

  /* free string and path for rank2file map */
  scr_path_delete(&rank2file_path);

cleanup:
  /* free the header hash */
  scr_hash_delete(&header);

  /* free path for dataset directory */
  scr_path_delete(&meta_path);

  return rc;
}

/* read contents of summary file */
static int scr_fetch_summary(
  const char* summary_dir,
  scr_hash* file_list)
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

  /* read summary data */
  scr_path* summary_path = scr_path_from_str(summary_dir);
  rc = scr_summary_read_mpi(summary_path, file_list);
  scr_path_delete(&summary_path);

  return rc;
}

/* fetch files specified in file_list into specified dir and update
 * filemap */
static int scr_fetch_data(
  const scr_hash* file_list,
  const char* dir,
  scr_filemap* map)
{
  int success = SCR_SUCCESS;

  /* flow control rate of file reads from rank 0 */
  if (scr_my_rank_world == 0) {
    /* fetch these files into the directory */
    if (scr_fetch_files_list(file_list, dir, map) != SCR_SUCCESS) {
      success = SCR_FAILURE;
    }

    /* now, have a sliding window of w processes read simultaneously */
    int w = scr_fetch_width;
    if (w > scr_ranks_world-1) {
      w = scr_ranks_world-1;
    }

    /* allocate MPI_Request arrays and an array of ints */
    int* flags       = (int*)         malloc(2 * w * sizeof(int));
    MPI_Request* req = (MPI_Request*) malloc(2 * w * sizeof(MPI_Request));
    MPI_Status status;
    if (flags == NULL || req == NULL) {
      scr_abort(-1, "Failed to allocate memory for flow control @ %s:%d",
        __FILE__, __LINE__
      );
    }

    /* execute our flow control window */
    int outstanding = 0;
    int index = 0;
    int i = 1;
    while (i < scr_ranks_world || outstanding > 0) {
      /* issue up to w outstanding sends and receives */
      while (i < scr_ranks_world && outstanding < w) {
        /* post a receive for the response message we'll get back when
         * rank i is done */
        MPI_Irecv(&flags[index + w], 1, MPI_INT, i, 0, scr_comm_world, &req[index + w]);

        /* send a start signal to this rank */
        flags[index] = success;
        MPI_Isend(&flags[index], 1, MPI_INT, i, 0, scr_comm_world, &req[index]);

        /* update the number of outstanding requests */
        outstanding++;
        index++;
        i++;
      }

      /* wait to hear back from any rank */
      MPI_Waitany(w, &req[w], &index, &status);

      /* the corresponding send must be complete */
      MPI_Wait(&req[index], &status);

      /* check success code from process */
      if (flags[index + w] != SCR_SUCCESS) {
        success = SCR_FAILURE;
      }

      /* one less request outstanding now */
      outstanding--;
    }

    /* free the MPI_Request arrays */
    scr_free(&req);
    scr_free(&flags);
  } else {
    /* wait for start signal from rank 0 */
    MPI_Status status;
    MPI_Recv(&success, 1, MPI_INT, 0, 0, scr_comm_world, &status);

    /* if rank 0 hasn't seen a failure, try to read in our files */
    if (success == SCR_SUCCESS) {
      /* fetch these files into the directory */
      if (scr_fetch_files_list(file_list, dir, map) != SCR_SUCCESS) {
        success = SCR_FAILURE;
      }
    }

    /* tell rank 0 that we're done and send him our success code */
    MPI_Send(&success, 1, MPI_INT, 0, 0, scr_comm_world);
  }

  /* determine whether all processes successfully read their files */
  if (scr_alltrue(success == SCR_SUCCESS)) {
    return SCR_SUCCESS;
  }
  return SCR_FAILURE;
}

/* fetch files from parallel file system */
static int scr_fetch_files(
  scr_filemap* map,
  scr_path* fetch_path,
  int* dataset_id,
  int* checkpoint_id)
{
  /* get fetch directory as string */
  char* fetch_dir = scr_path_strdup(fetch_path);

  /* this may take a while, so tell user what we're doing */
  if (scr_my_rank_world == 0) {
    scr_dbg(1, "Attempting fetch from %s", fetch_dir);
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
  scr_hash* file_list = scr_hash_new();

  /* read the summary file */
  if (scr_fetch_summary(fetch_dir, file_list) != SCR_SUCCESS) {
    if (scr_my_rank_world == 0) {
      scr_dbg(1, "Failed to read summary file @ %s:%d", __FILE__, __LINE__);
      if (scr_log_enable) {
        double time_end = MPI_Wtime();
        double time_diff = time_end - time_start;
        time_t now = scr_log_seconds();
        scr_log_event("FETCH FAILED", fetch_dir, NULL, &now, &time_diff);
      }
    }
    scr_hash_delete(&file_list);
    scr_free(&fetch_dir);
    return SCR_FAILURE;
  }

  /* get a pointer to the dataset */
  scr_dataset* dataset = scr_hash_get(file_list, SCR_KEY_DATASET);

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
    scr_hash_delete(&file_list);
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
    scr_hash_delete(&file_list);
    scr_free(&fetch_dir);
    return SCR_FAILURE;
  }

  /* delete any existing files for this dataset id (do this before
   * filemap_read) */
  scr_cache_delete(map, id);

  /* get the redundancy descriptor for this id */
  scr_reddesc* c = scr_reddesc_for_checkpoint(ckpt_id, scr_nreddescs, scr_reddescs);

  /* store our redundancy descriptor hash in the filemap */
  scr_hash* my_desc_hash = scr_hash_new();
  scr_reddesc_store_to_hash(c, my_desc_hash);
  scr_filemap_set_desc(map, id, scr_my_rank_world, my_desc_hash);
  scr_hash_delete(&my_desc_hash);

  /* write the filemap out before creating the directory */
  scr_filemap_write(scr_map_file, map);

  /* create the cache directory */
  scr_cache_dir_create(c, id);

  /* get the cache directory */
  char cache_dir[SCR_MAX_FILENAME];
  scr_cache_dir_get(c, id, cache_dir);

  /* now we can finally fetch the actual files */
  int success = 1;
  if (scr_fetch_data(file_list, cache_dir, map) != SCR_SUCCESS) {
    success = 0;
  }

  /* free the hash holding the summary file data */
  scr_hash_delete(&file_list);

  /* check that all processes copied their file successfully */
  if (! scr_alltrue(success)) {
    /* someone failed, so let's delete the partial checkpoint */
    scr_cache_delete(map, id);

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
    scr_free(&fetch_dir);
    return SCR_FAILURE;
  }

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
    scr_cache_delete(scr_map, id);
  }

  /* stop timer, compute bandwidth, and report performance */
  double total_bytes = bytes_copied;
  if (scr_my_rank_world == 0) {
    double time_end = MPI_Wtime();
    double time_diff = time_end - time_start;
    double bw = total_bytes / (1024.0 * 1024.0 * time_diff);
    scr_dbg(1, "scr_fetch_files: %f secs, %e bytes, %f MB/s, %f MB/s per proc",
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

      char cache_dir[SCR_MAX_FILENAME];
      scr_cache_dir_get(c, id, cache_dir);
      scr_log_transfer("FETCH", fetch_dir, cache_dir, &id,
        &timestamp_start, &time_diff, &total_bytes
      );
    }
  }

  /* free fetch direcotry string */
  scr_free(&fetch_dir);

  return rc;
}

/* attempt to fetch most recent checkpoint from prefix directory into
 * cache, fills in map if successful and sets fetch_attempted to 1 if
 * any fetch is attempted, returns SCR_SUCCESS if successful */
int scr_fetch_sync(scr_filemap* map, int* fetch_attempted)
{
  /* we only return success if we successfully fetch a checkpoint */
  int rc = SCR_FAILURE;

  double time_start, time_end, time_diff;

  /* start timer */
  if (scr_my_rank_world == 0) {
    time_start = MPI_Wtime();
  }

  /* have rank 0 read the index file */
  scr_hash* index_hash = NULL;
  int read_index_file = 0;
  if (scr_my_rank_world == 0) {
    /* create an empty hash to store our index */
    index_hash = scr_hash_new();

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
  int current_checkpoint_id = -1;
  while (continue_fetching) {
    /* create a new path */
    scr_path* fetch_path = scr_path_new();

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
      int next_checkpoint_id = -1;
      if (strcmp(target, "") != 0) {
        /* we have a subdirectory name, lookup the checkpoint id
         * corresponding to this directory */
        scr_index_get_id_by_dir(index_hash, target, &next_checkpoint_id);
      } else {
        /* otherwise, just get the most recent complete checkpoint
         * (that's older than the current id) */
        scr_index_get_most_recent_complete(index_hash, current_checkpoint_id, &next_checkpoint_id, target);
      }
      current_checkpoint_id = next_checkpoint_id;

      /* TODODSET: need to verify that dataset is really a checkpoint
       * and keep searching if not */

      /* if we have a subdirectory (target) name, build the full fetch
       * directory */
      if (strcmp(target, "") != 0) {
        /* record that we're attempting a fetch of this checkpoint in
         * the index file */
        *fetch_attempted = 1;
        if (current_checkpoint_id != -1) {
          scr_index_mark_fetched(index_hash, current_checkpoint_id, target);
          scr_index_write(scr_prefix_path, index_hash);
        }

        /* we have a subdirectory, now build the full path */
        scr_path_append(fetch_path, scr_prefix_path);
        scr_path_append_str(fetch_path, target);
        scr_path_reduce(fetch_path);
      }
    }

    /* broadcast fetch path from rank 0 */
    scr_path_bcast(fetch_path, 0, scr_comm_world);

    /* check whether we've got a path */
    if (! scr_path_is_null(fetch_path)) {
      /* got something, attempt to fetch the checkpoint */
      int dset_id, ckpt_id;
      rc = scr_fetch_files(map, fetch_path, &dset_id, &ckpt_id);
      if (rc == SCR_SUCCESS) {
        /* set the dataset and checkpoint ids */
        scr_dataset_id = dset_id;
        scr_checkpoint_id = ckpt_id;

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
          if (current_checkpoint_id != -1 && strcmp(target, "") != 0) {
            scr_index_mark_failed(index_hash, current_checkpoint_id, target);
          }
          scr_index_write(scr_prefix_path, index_hash);
        }
      }
    } else {
      /* we ran out of valid checkpoints in the index file,
       * bail out of the loop */
      continue_fetching = 0;
    }

    /* free fetch path */
    scr_path_delete(&fetch_path);
  }

  /* delete the index hash */
  if (scr_my_rank_world == 0) {
    scr_hash_delete(&index_hash);
  }

  /* broadcast whether we actually attempted to fetch anything
   * (only rank 0 knows) */
  MPI_Bcast(fetch_attempted, 1, MPI_INT, 0, scr_comm_world);

  /* stop timer for fetch */
  if (scr_my_rank_world == 0) {
    time_end = MPI_Wtime();
    time_diff = time_end - time_start;
    scr_dbg(1, "scr_fetch_files: return code %d, %f secs", rc, time_diff);
  }

  return rc;
}
