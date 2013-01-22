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

#ifdef HAVE_LIBDTCMP
#include "dtcmp.h"
#endif /* HAVE_LIBDTCMP */

/*
=========================================
Common flush functions
=========================================
*/

/* returns true if the named file needs to be flushed, 0 otherwise */
int scr_bool_flush_file(const scr_filemap* map, int dset, int rank, const char* file)
{
  /* assume we need to flush this file */
  int flush = 1;

  /* read meta info for file */
  scr_meta* meta = scr_meta_new();
  if (scr_filemap_get_meta(map, dset, rank, file, meta) == SCR_SUCCESS) {
    /* don't flush XOR files */
    if (scr_meta_check_filetype(meta, SCR_META_FILE_XOR) == SCR_SUCCESS) {
      flush = 0;
    }
  } else {
    /* TODO: print error */
  }
  scr_meta_delete(meta);

  return flush;
}

int scr_dataset_build_name(int id, int64_t usecs, char* name, int n)
{
#if 0
  /* format timestamp */
  char timestamp[SCR_MAX_FILENAME];
  time_t now = (time_t) (usecs / (int64_t) 1000000);
  strftime(timestamp, sizeof(timestamp), "%Y-%m-%d_%H:%M:%S", localtime(&now));

  /* build the directory name */
  char dirname[SCR_MAX_FILENAME];
  snprintf(name, n, "scr.%s.%s.%d", timestamp, scr_jobid, id);
#endif

  /* build the directory name */
  snprintf(name, n, "scr.dataset.%d", id);

  return SCR_SUCCESS;
}

/*
=========================================
Prepare for flush by building list of files, creating directories,
and creating container files (if any)
=========================================
*/

/* fills in hash with a list of filenames and associated meta data
 * that should be flushed for specified dataset id */
static int scr_flush_identify_files(const scr_filemap* map, int id, scr_hash* file_list)
{
  int rc = SCR_SUCCESS;

  /* lookup dataset from filemap and store in file list */
  scr_dataset* dataset = scr_hash_new();
  scr_filemap_get_dataset(map, id, scr_my_rank_world, dataset);
  scr_hash_set(file_list, SCR_KEY_DATASET, dataset);

  /* identify which files we need to flush as part of the specified dataset id */
  scr_hash_elem* elem = NULL;
  for (elem = scr_filemap_first_file(map, id, scr_my_rank_world);
       elem != NULL;
       elem = scr_hash_elem_next(elem))
  {
    /* get the filename */
    char* file = scr_hash_elem_key(elem);

    /* read meta data for file and attach it to file list */
    scr_meta* meta = scr_meta_new();
    if (scr_filemap_get_meta(map, id, scr_my_rank_world, file, meta) == SCR_SUCCESS) {
      /* don't flush XOR files */
      int flush = 1;
      if (scr_meta_check_filetype(meta, SCR_META_FILE_XOR) == SCR_SUCCESS) {
        flush = 0;
      }

      /* if we need to flush this file, add it to the list and attach its meta data */
      if (flush) {
        scr_hash* file_hash = scr_hash_set_kv(file_list, SCR_KEY_FILE, file);
        scr_hash_set(file_hash, SCR_KEY_META, meta);
        meta = NULL;
      }
    } else {
      /* TODO: print error */
      rc = SCR_FAILURE;
    }

    /* if we didn't attach the meta data, we need to delete it */
    if (meta != NULL) {
      scr_meta_delete(meta);
    }
  }

  return rc;
}

/* resolves par and dir directories, then ensures dir is
 * contained as a subdirectory under parent, and returns subdir */
static int scr_find_subdir(
  const char* par,    /* IN  - full path to parent directory */
  const char* dir,    /* IN  - full path to child directory */
  char* subdir,       /* OUT - top level subdirectory, if exists */
  size_t subdir_size) /* IN  - size of subdir buffer */
{
  /* simplify parent directory */
  char parent[SCR_MAX_FILENAME];
  if (scr_path_resolve(par, parent, sizeof(parent)) != SCR_SUCCESS) {
    scr_abort(-1, "Failed to simplify parent directory %s @ %s:%d",
      par, __FILE__, __LINE__
    );
  }

  /* get number of characters in parent */
  size_t parent_len = strlen(parent);

  /* resolve child directory */
  char child[SCR_MAX_FILENAME];
  if (scr_path_resolve(dir, child, sizeof(child)) != SCR_SUCCESS) {
    scr_abort(-1, "Failed to simplify directory %s @ %s:%d",
      dir, __FILE__, __LINE__
    );
  }

  /* ensure that parent path is prefix of child path */
  if (strncmp(parent, child, parent_len) != 0) {
    scr_abort(-1, "Directory %s not contained in parent %s @ %s:%d",
      dir, parent, __FILE__, __LINE__
    );
  }

  /* get number of components in parent directory */
  int parent_components;
  if (scr_path_length(parent, &parent_components) != SCR_SUCCESS) {
    scr_abort(-1, "Failed to get number of components in parent directory %s @ %s:%d",
      parent, __FILE__, __LINE__
    );
  }

  /* ensure that directory has at least one more component than parent */
  int child_components;
  if (scr_path_length(child, &child_components) != SCR_SUCCESS) {
    scr_abort(-1, "Failed to get number of components in path %s @ %s:%d",
      dir, __FILE__, __LINE__
    );
  }
  if (child_components <= parent_components) {
    scr_abort(-1, "Directory %s must have more components than parent %s @ %s:%d",
      dir, parent, __FILE__, __LINE__
    );
  }

  /* get sub directory name */
  if (scr_path_slice(child, parent_components, 1, subdir, subdir_size)
      != SCR_SUCCESS)
  {
    scr_abort(-1, "Failed to get subdirectory from %s @ %s:%d",
      dir, __FILE__, __LINE__
    );
  }

  return SCR_SUCCESS;
}

/* create all directories needed for file list */
static int scr_flush_identify_dirs(scr_hash* file_list)
{
  /* get the dataset for this list of files */
  scr_dataset* dataset = scr_hash_get(file_list, SCR_KEY_DATASET);

  if (scr_preserve_directories) {
    /* preserving user-defined directories, identify them here */
#ifdef HAVE_LIBDTCMP
    /* get pointer to file hash */
    scr_hash* files = scr_hash_get(file_list, SCR_KEY_FILE);

    /* count the number of files that we need to flush */
    int count = scr_hash_size(files);

    /* allocate buffers to hold the directory needed for each file */
    const char** dirs     = NULL;
    uint64_t* group_id    = NULL;
    uint64_t* group_ranks = NULL;
    uint64_t* group_rank  = NULL;
    const char** subdirs  = NULL;
    if (count > 0) {
      dirs        = (const char**) malloc(sizeof(const char*) * count);
      group_id    = (uint64_t*)    malloc(sizeof(uint64_t)    * count);
      group_ranks = (uint64_t*)    malloc(sizeof(uint64_t)    * count);
      group_rank  = (uint64_t*)    malloc(sizeof(uint64_t)    * count);
      subdirs     = (const char**) malloc(sizeof(const char*)       * count);
      /* TODO: check for allocation error */
    }

    /* lookup directory from meta data for each file */
    int valid_subdir = 1;
    int i = 0;
    scr_hash_elem* elem = NULL;
    for (elem = scr_hash_elem_first(files);
         elem != NULL;
         elem = scr_hash_elem_next(elem))
    {
      /* get meta data for this file */
      scr_hash* hash = scr_hash_elem_hash(elem);
      scr_meta* meta = scr_hash_get(hash, SCR_KEY_META);

      /* lookup original path where application wants file to go */
      dirs[i] = NULL;
      char* dir;
      if (scr_meta_get_origpath(meta, &dir) == SCR_SUCCESS) {
        /* record pointer to directory name */
        dirs[i] = dir;

        /* record original path as path to flush to in file list */
        scr_hash_util_set_str(hash, SCR_KEY_PATH, dir);

        /* get top level subdirectory under prefix */
        char subdir[SCR_MAX_FILENAME];
        if (scr_find_subdir(scr_prefix, dir, subdir, sizeof(subdir)) == SCR_SUCCESS) {
          subdirs[i] = strdup(subdir);
        } else {
          /* dir may not be contained in prefix, or we otherwise
           * failed to acquire it */
          subdirs[i] = NULL;
          valid_subdir = 0;
        }
      } else {
        scr_abort(-1, "Failed to read original path name for a file @ %s:%d",
          __FILE__, __LINE__
        );
      }

      /* add one to our file count */
      i++;
    }

    /* verify that all procs have valid subdirs */
    if (! scr_alltrue(valid_subdir)) {
      if (scr_my_rank_world == 0) {
        scr_abort(-1, "One or more processes found an invalid subdirectory @ %s:%d",
          __FILE__, __LINE__
        );
      }
    }

    /* rank subdirectories and ensure groups is exactly 1 */
    uint64_t groups;
    int dtcmp_rc = DTCMP_Rankv_strings(
      count, subdirs, &groups, group_id, group_ranks, group_rank,
      DTCMP_FLAG_NONE, scr_comm_world
    );
    if (dtcmp_rc != DTCMP_SUCCESS) {
      scr_abort(-1, "Failed to rank strings during flush @ %s:%d",
        __FILE__, __LINE__
      );
    }
    if (groups != 1) {
      if (scr_my_rank_world == 0) {
        scr_abort(-1, "Identified more than one subdirectory during flush @ %s:%d",
          __FILE__, __LINE__
        );
      }
    }

    /* since groups == 1, at least one process specified a directory */
    int rank = scr_ranks_world;
    char root_subdir[SCR_MAX_FILENAME];
    if (count > 0) {
      rank = scr_my_rank_world;
      strcpy(root_subdir, subdirs[0]);
    }

    /* identify lowest rank which specified directory */
    int lowest_rank;
    MPI_Allreduce(&rank, &lowest_rank, 1, MPI_INT, MPI_MIN, scr_comm_world);

    /* broadcast directory from lowest rank so all have a copy */
    scr_strn_bcast(root_subdir, sizeof(root_subdir), lowest_rank, scr_comm_world);

    /* build full path to top level directory */
    char full_subdir[SCR_MAX_FILENAME];
    scr_path_build(full_subdir, sizeof(full_subdir), scr_prefix, root_subdir);

    /* record top level directory for flush */
    scr_hash_util_set_str(file_list, SCR_KEY_PATH, full_subdir);

    /* identify the set of unique directories */
    dtcmp_rc = DTCMP_Rankv_strings(
      count, dirs, &groups, group_id, group_ranks, group_rank,
      DTCMP_FLAG_NONE, scr_comm_world
    );
    if (dtcmp_rc != DTCMP_SUCCESS) {
      scr_abort(-1, "Failed to rank strings during flush @ %s:%d",
        __FILE__, __LINE__
      );
    }

    /* select leader for each directory */
    for (i = 0; i < count; i++) {
      if (group_rank[i] == 0) {
        scr_hash_set_kv(file_list, SCR_KEY_DIRECTORY, dirs[i]);
      }

      /* free subdirs */
      scr_free(&subdirs[i]);
    }

    /* free buffers */
    scr_free(&subdirs);
    scr_free(&group_id);
    scr_free(&group_ranks);
    scr_free(&group_rank);
    scr_free(&dirs);

    /* TODO: PRESERVE need to track directory names in summary file so we can delete them later */

#else /* HAVE_LIBDTCMP */
    /* need DTCMP in order to preserve user-defined directories */
    return SCR_FAILURE;
#endif /* HAVE_LIBDTCMP */
  } else {
    /* create single scr.dataset directory at top level */
    /* get the name of the dataset */
    char* name;
    if (scr_dataset_get_name(dataset, &name) != SCR_SUCCESS) {
      scr_abort(-1, "Failed to get dataset name @ %s:%d",
        __FILE__, __LINE__
      );
    }

    /* build the directory name */
    char dir[SCR_MAX_FILENAME];
    if (scr_path_build(dir, sizeof(dir), scr_prefix, name) != SCR_SUCCESS) {
      scr_abort(-1, "Failed to build path to flush directory @ %s:%d",
        __FILE__, __LINE__
      );
    }

    /* record top level directory for flush */
    scr_hash_util_set_str(file_list, SCR_KEY_PATH, dir);

    /* add the flush directory to each file in the list */
    scr_hash_elem* elem = NULL;
    scr_hash* files = scr_hash_get(file_list, SCR_KEY_FILE);
    for (elem = scr_hash_elem_first(files);
         elem != NULL;
         elem = scr_hash_elem_next(elem))
    {
      /* get meta data for this file */
      scr_hash* hash = scr_hash_elem_hash(elem);
      scr_hash_util_set_str(hash, SCR_KEY_PATH, dir);
    }
  }

  return SCR_SUCCESS;
}

/* given a dataset and a container id, construct full path to container file */
static int scr_container_construct_name(const scr_dataset* dataset, int id, char* file, int len)
{
  /* check that we have a dataset and a buffer to write the name to */
  if (dataset == NULL || file == NULL) {
    return SCR_FAILURE;
  }

  /* get the name of the dataset */
  char* name;
  if (scr_dataset_get_name(dataset, &name) != SCR_SUCCESS) {
    return SCR_FAILURE;
  }

  /* build the name and check that it's not truncated */
  int n = snprintf(file, len, "%s/%s/ctr.%d.scr", scr_prefix, name, id);
  if (n >= len) {
    /* we truncated the container name */
    return SCR_FAILURE;
  }

  return SCR_SUCCESS;
}

/* identify the container to write each file to */
static int scr_flush_identify_containers(scr_hash* file_list)
{
  int rc = SCR_SUCCESS;

  /* get our rank on the node */
  int rank_node;
  MPI_Comm_rank(scr_comm_node, &rank_node);

  /* get the maximum container size */
  unsigned long container_size = scr_container_size;

  /* get the dataset for the file list */
  scr_dataset* dataset = scr_hash_get(file_list, SCR_KEY_DATASET);

  /* compute total number of bytes we'll flush on this process */
  unsigned long my_bytes = 0;
  scr_hash_elem* elem = NULL;
  scr_hash* files = scr_hash_get(file_list, SCR_KEY_FILE);
  for (elem = scr_hash_elem_first(files);
       elem != NULL;
       elem = scr_hash_elem_next(elem))
  {
    /* get meta data for this file */
    scr_hash* hash = scr_hash_elem_hash(elem);
    scr_meta* meta = scr_hash_get(hash, SCR_KEY_META);

    /* get filesize from meta data */
    unsigned long filesize;
    if (scr_meta_get_filesize(meta, &filesize) == SCR_SUCCESS) {
      my_bytes += filesize;
    } else {
       /* TODO: error */
      rc = SCR_FAILURE;
    }
  }

  /* compute total number of bytes we need to write across all processes */
  unsigned long total_bytes;
  MPI_Allreduce(&my_bytes, &total_bytes, 1, MPI_UNSIGNED_LONG, MPI_SUM, scr_comm_world);

  /* compute total number of bytes we need to write on the node */
  unsigned long local_bytes;
  MPI_Reduce(&my_bytes, &local_bytes, 1, MPI_UNSIGNED_LONG, MPI_SUM, 0, scr_comm_node);

  /* compute offset for each node */
  unsigned long local_offset = 0;
  if (rank_node == 0) {
    MPI_Scan(&local_bytes, &local_offset, 1, MPI_UNSIGNED_LONG, MPI_SUM, scr_comm_node_across);
    local_offset -= local_bytes;
  }

  /* compute offset for each process,
   * note that local_offset == 0 for all procs on the node except for rank == 0,
   * which contains the offset for the node */
  unsigned long my_offset = 0;
  local_offset += my_bytes;
  MPI_Scan(&local_offset, &my_offset, 1, MPI_UNSIGNED_LONG, MPI_SUM, scr_comm_node);
  my_offset -= my_bytes;

  /* compute offset for each file on this process */
  unsigned long file_offset = my_offset;
  for (elem = scr_hash_elem_first(files);
       elem != NULL;
       elem = scr_hash_elem_next(elem))
  {
    /* get meta data for this file */
    scr_hash* hash = scr_hash_elem_hash(elem);
    scr_meta* meta = scr_hash_get(hash, SCR_KEY_META);

    /* get filesize from meta data */
    unsigned long filesize;
    if (scr_meta_get_filesize(meta, &filesize) == SCR_SUCCESS) {
      /* compute container id, offset, and length */
      int file_segment = 0;
      unsigned long remaining = filesize;
      while (remaining > 0) {
        /* compute container id, offset, and length */
        int container_id = file_offset / container_size;
        unsigned long container_offset = file_offset - (container_id * container_size);
        unsigned long container_length = container_size - container_offset;
        if (container_length > remaining) {
          container_length = remaining;
        }

        /* store segment length, container id, and container offset under new file segment */
        scr_hash* segment_hash = scr_hash_set_kv_int(hash, SCR_SUMMARY_6_KEY_SEGMENT, file_segment);
        scr_hash_util_set_bytecount(segment_hash, SCR_SUMMARY_6_KEY_LENGTH, container_length);
        scr_hash* container_hash = scr_hash_new();
        scr_hash_util_set_int(container_hash, SCR_SUMMARY_6_KEY_ID, container_id);
        scr_hash_util_set_bytecount(container_hash, SCR_SUMMARY_6_KEY_OFFSET, container_offset);
        scr_hash_set(segment_hash, SCR_SUMMARY_6_KEY_CONTAINER, container_hash);

        /* add entry for container name in the file list */
        scr_hash* details = scr_hash_set_kv_int(file_list, SCR_SUMMARY_6_KEY_CONTAINER, container_id);

        /* compute name of container */
        char container_name[SCR_MAX_FILENAME];
        scr_container_construct_name(dataset, container_id, container_name, sizeof(container_name));
        scr_hash_util_set_str(details, SCR_KEY_NAME, container_name);

        /* compute size of container */
        unsigned long size = container_size;
        if ((container_id+1) * container_size > total_bytes) {
          size = total_bytes - (container_id * container_size);
        }
        scr_hash_util_set_bytecount(details, SCR_SUMMARY_6_KEY_SIZE, size);

        /* move on to the next file segment */
        remaining   -= container_length;
        file_offset += container_length;
        file_segment++;
      }
    } else {
      /* TODO: error */
      rc = SCR_FAILURE;
    }
  }

  /* determine whether all processes successfully computed their containers */
  if (! scr_alltrue((rc == SCR_SUCCESS))) {
    return SCR_FAILURE;
  }
  return SCR_SUCCESS;
}

/* given file map and dataset id, identify files, containers, and
 * directories needed for flush and return in file list hash */
static int scr_flush_identify(const scr_filemap* map, int id, scr_hash* file_list)
{
  /* check that we have all of our files */
  int have_files = 1;
  if (scr_cache_check_files(map, id) != SCR_SUCCESS) {
    scr_err("Missing one or more files for dataset %d @ %s:%d",
      id, __FILE__, __LINE__
    );
    have_files = 0;
  }
  if (! scr_alltrue(have_files)) {
    if (scr_my_rank_world == 0) {
      scr_err("One or more processes are missing files for dataset %d @ %s:%d",
        id, __FILE__, __LINE__
      );
    }
    return SCR_FAILURE;
  }

  /* build the list of files to flush, which includes meta data for each one */
  if (scr_flush_identify_files(map, id, file_list) != SCR_SUCCESS) {
    scr_abort(-1, "Failed to get list of files for dataset %d @ %s:%d",
      id, __FILE__, __LINE__
    );
  }

  /* build the list of directories to create */
  if (scr_flush_identify_dirs(file_list) != SCR_SUCCESS) {
    scr_abort(-1, "Failed to get list of directories for dataset %d @ %s:%d",
      id, __FILE__, __LINE__
    );
  }

  /* identify containers for our files */
  if (scr_use_containers) {
    if (scr_flush_identify_containers(file_list) != SCR_SUCCESS) {
      scr_abort(-1, "Failed to identify containers for dataset %d @ %s:%d",
        id, __FILE__, __LINE__
      );
    }
  }

  return SCR_SUCCESS;
}

/* create all directories needed for file list */
static int scr_flush_create_dirs(scr_hash* file_list)
{
  /* have rank 0 create the dataset directory */
  if (scr_my_rank_world == 0) {
    /* get top level path */
    char* flushdir;
    if (scr_hash_util_get_str(file_list, SCR_KEY_PATH, &flushdir) != SCR_SUCCESS) {
      scr_abort(-1, "Failed to get flush directory @ %s:%d",
        __FILE__, __LINE__
      );
    }

    /* extract subdir name */
    char path[SCR_MAX_FILENAME];
    char subdir[SCR_MAX_FILENAME];
    scr_path_split(flushdir, path, subdir);

    /* get the dataset for this list of files */
    scr_dataset* dataset = scr_hash_get(file_list, SCR_KEY_DATASET);

    /* get the id of the dataset */
    int id;
    if (scr_dataset_get_id(dataset, &id) != SCR_SUCCESS) {
      scr_abort(-1, "Failed to get dataset id @ %s:%d",
        __FILE__, __LINE__
      );
    }

    /* add the directory to our index file, and record the flush timestamp */
    scr_hash* index_hash = scr_hash_new();
    scr_index_read(scr_prefix, index_hash);
    scr_index_set_dataset(index_hash, id, subdir, dataset, 0);
    scr_index_add_dir(index_hash, id, subdir);
    scr_index_mark_flushed(index_hash, id, subdir);
    scr_index_write(scr_prefix, index_hash);
    scr_hash_delete(&index_hash);

    /* create the directory */
    if (scr_mkdir(flushdir, S_IRWXU) == SCR_SUCCESS) {
      /* created the directory successfully */
      scr_dbg(1, "Flushing to %s", flushdir);
    } else {
      /* failed to create the directory */
      scr_abort(-1, "Failed to make dataset directory mkdir(%s) @ %s:%d",
        flushdir, __FILE__, __LINE__
      );
    }

    /* create the .scr subdirectory */
    char dir_scr[SCR_MAX_FILENAME];
    scr_path_build(dir_scr, sizeof(dir_scr), flushdir, ".scr");
    if (scr_mkdir(dir_scr, S_IRWXU) != SCR_SUCCESS) {
      /* failed to create the directory */
      scr_abort(-1, "Failed to make .scr subdirectory directory mkdir(%s) @ %s:%d",
        dir_scr, __FILE__, __LINE__
      );
    }
  }

  /* wait for rank 0 */
  MPI_Barrier(scr_comm_world);

  /* TODO: add flow control here */
  /* create other directories in file list */
  int success = 1;
  scr_hash_elem* elem = NULL;
  scr_hash* dirs = scr_hash_get(file_list, SCR_KEY_DIRECTORY);
  for (elem = scr_hash_elem_first(dirs);
       elem != NULL;
       elem = scr_hash_elem_next(elem))
  {
    /* create directory */
    char* dir = scr_hash_elem_key(elem);
    if (scr_mkdir(dir, S_IRWXU) != SCR_SUCCESS) {
      success = 0;
    }
  }

  /* TODO: PRESERVE need to track directory names in summary file so we can delete them later */

  /* determine whether all leaders successfully created their directories */
  if (! scr_alltrue((success == 1))) {
    return SCR_FAILURE;
  }
  return SCR_SUCCESS;
}

/* create container files
 * could do different things here depending on the file system, for example:
 *   Lustre - have first process writing to container create it
 *   GPFS   - gather container names to rank 0 and have it create them all */
static int scr_flush_create_containers(const scr_hash* file_list)
{
  int success = SCR_SUCCESS;

  /* here, we look at each segment a process writes,
   * and the process which writes data to offset 0 is responsible for creating the container */

  /* get the hash of containers */
  scr_hash* containers = scr_hash_get(file_list, SCR_SUMMARY_6_KEY_CONTAINER);

  /* iterate over each of our files */
  scr_hash_elem* file_elem;
  scr_hash* files = scr_hash_get(file_list, SCR_KEY_FILE);
  for (file_elem = scr_hash_elem_first(files);
       file_elem != NULL;
       file_elem = scr_hash_elem_next(file_elem))
  {
    /* get the hash for this file */
    scr_hash* hash = scr_hash_elem_hash(file_elem);

    /* iterate over the segments for this file */
    scr_hash_elem* segment_elem;
    scr_hash* segments = scr_hash_get(hash, SCR_SUMMARY_6_KEY_SEGMENT);
    for (segment_elem = scr_hash_elem_first(segments);
         segment_elem != NULL;
         segment_elem = scr_hash_elem_next(segment_elem))
    {
      /* get the hash for this segment */
      scr_hash* segment = scr_hash_elem_hash(segment_elem);

      /* lookup the container details for this segment */
      char* name;
      unsigned long size, offset, length;
      if (scr_container_get_name_size_offset_length(segment, containers, &name, &size, &offset, &length) == SCR_SUCCESS) {
        /* if we write something to offset 0 of this container,
         * we are responsible for creating the file */
        if (offset == 0 && length > 0) {
          /* open the file with create and truncate options, then just immediately close it */
          int fd = scr_open(name, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
          if (fd < 0) {
            /* the create failed */
            scr_err("Opening file for writing: scr_open(%s) errno=%d %m @ %s:%d",
              name, errno, __FILE__, __LINE__
            );
            success = SCR_FAILURE;
          } else {
            /* the create succeeded, now just close the file */
            scr_close(name, fd);
          }
        }
      } else {
        /* failed to read container details from segment hash, consider this an error */
        success = SCR_FAILURE;
      }
    }
  }

  /* determine whether all processes successfully created their containers */
  if (scr_alltrue((success == SCR_SUCCESS))) {
    return SCR_SUCCESS;
  }
  return SCR_FAILURE;
}

/* TODO: attach this info to file map */
/* this is a hacky way to record data in flush file from complete checkpoint */
int scr_flush_verify(const scr_filemap* map, int id, char* dir, size_t dir_size)
{
  scr_hash* file_list = scr_hash_new();

  /* build the list of files to flush, which includes meta data for each one */
  if (scr_flush_identify(map, id, file_list) != SCR_SUCCESS) {
    scr_abort(-1, "Failed to identify data for flush of dataset %d @ %s:%d",
      id, __FILE__, __LINE__
    );
  }

  /* look up path of flush directory */
  char* flush_dir;
  if (scr_hash_util_get_str(file_list, SCR_KEY_PATH, &flush_dir) != SCR_SUCCESS) {
    scr_abort(-1, "Failed to get flush directory of dataset %d @ %s:%d",
      id, __FILE__, __LINE__
    );
  }

  /* get subdirectory name for flush */
  char prefix[SCR_MAX_FILENAME];
  char subdir[SCR_MAX_FILENAME];
  scr_path_split(flush_dir, prefix, subdir);

  /* copy subdir to users buffer */
  size_t subdir_len = strlen(subdir) + 1;
  if (subdir_len <= dir_size) {
    strcpy(dir, subdir);
  } else {
    scr_abort(-1, "Failed to copy subdirectory name for dataset %d @ %s:%d",
      id, __FILE__, __LINE__
    );
  }

  scr_hash_delete(&file_list);

  return SCR_SUCCESS;
}

/* given a filemap and a dataset id, prepare and return a list of files to be flushed,
 * also create corresponding directories and container files */
int scr_flush_prepare(const scr_filemap* map, int id, scr_hash* file_list)
{
  /* build the list of files to flush, which includes meta data for each one */
  if (scr_flush_identify(map, id, file_list) != SCR_SUCCESS) {
    scr_abort(-1, "Failed to identify data for flush of dataset %d @ %s:%d",
      id, __FILE__, __LINE__
    );
  }

  /* create directories for flush */
  if (scr_flush_create_dirs(file_list) != SCR_SUCCESS) {
    /* TODO: delete the directories that we just created above? */
    if (scr_my_rank_world == 0) {
      scr_err("Failed to create flush directories for dataset %d @ %s:%d",
        id, __FILE__, __LINE__
      );
    }
    return SCR_FAILURE;
  }

  /* create container files */
  if (scr_use_containers) {
    if (scr_flush_create_containers(file_list) != SCR_SUCCESS) {
      /* TODO: delete the directories that we just created above?
       * and the partial set of files we just created here? */
      scr_err("Failed to create container files for dataset %d @ %s:%d",
        id, __FILE__, __LINE__
      );
      return SCR_FAILURE;
    }
  }

  return SCR_SUCCESS;
}

/*
=========================================
Complete the flush by writing the summary file and updating the
index file
=========================================
*/

/* write summary file for flush */
static int scr_flush_summary(const char* summary_dir, const scr_dataset* dataset, const scr_hash* file_list, scr_hash* data)
{
  int flushed = SCR_SUCCESS;

  /* TODO: need to determine whether everyone flushed successfully */

  /* TODO: current method is a flat tree with rank 0 as the root,
   * need a more scalable algorithm */

  /* flow control the send among processes, and gather meta data to rank 0 */
  if (scr_my_rank_world == 0) {
    /* now, have a sliding window of w processes send simultaneously */
    int w = scr_flush_width;
    if (w > (scr_ranks_world - 1)) {
      w = scr_ranks_world - 1;
    }

    /* allocate MPI_Request arrays and an array of ints */
    int*         ranks    = (int*)         malloc(w * sizeof(int));
    int*         flags    = (int*)         malloc(w * sizeof(int));
    MPI_Request* req_recv = (MPI_Request*) malloc(w * sizeof(MPI_Request));
    MPI_Request* req_send = (MPI_Request*) malloc(w * sizeof(MPI_Request));
    MPI_Status status;

    int i = 1;
    int outstanding = 0;
    int index = 0;
    while (i < scr_ranks_world || outstanding > 0) {
      /* issue up to w outstanding sends and receives */
      while (i < scr_ranks_world && outstanding < w) {
        /* record which rank we assign to this slot */
        ranks[index] = i;

        /* post a receive for the response message we'll get back when rank i is done */
        MPI_Irecv(&flags[index], 1, MPI_INT, i, 0, scr_comm_world, &req_recv[index]);

        /* post a send to tell rank i to start */
        MPI_Isend(&flushed, 1, MPI_INT, i, 0, scr_comm_world, &req_send[index]);

        /* update the number of outstanding requests */
        i++;
        outstanding++;
        index++;
      }

      /* wait to hear back from any rank */
      MPI_Waitany(w, req_recv, &index, &status);

      /* someone responded, the send to this rank should also be done, so complete it */
      MPI_Wait(&req_send[index], &status);

      /* receive the meta data from this rank */
      scr_hash* incoming_hash = scr_hash_new();
      scr_hash_recv(incoming_hash, ranks[index], scr_comm_world);
      scr_hash_merge(data, incoming_hash);
      scr_hash_delete(&incoming_hash);

      /* one less request outstanding now */
      outstanding--;
    }

    /* free the MPI_Request arrays */
    scr_free(&req_send);
    scr_free(&req_recv);
    scr_free(&flags);
    scr_free(&ranks);
  } else {
    /* receive signal to start */
    int start;
    MPI_Status status;
    MPI_Recv(&start, 1, MPI_INT, 0, 0, scr_comm_world, &status);

    /* flush each of my files and fill in meta data structures */
    if (start != SCR_SUCCESS) {
      flushed = SCR_FAILURE;
    }

    /* send message to rank 0 to report that we're done */
    MPI_Send(&flushed, 1, MPI_INT, 0, 0, scr_comm_world);

    /* would be better to do this as a reduction-type of operation */
    scr_hash_send(data, 0, scr_comm_world);
  }

  /* write out the summary file */
  if (scr_my_rank_world == 0) {
    /* determine whether flush was complete */
    int complete = 1;
    if (flushed != SCR_SUCCESS) {
      complete = 0;
    }

    /* write summary file */
    if (scr_summary_write(summary_dir, dataset, complete, data) != SCR_SUCCESS) {
      flushed = SCR_FAILURE;
    }
  }

  /* determine whether everyone wrote their files ok */
  if (scr_alltrue((flushed == SCR_SUCCESS))) {
    return SCR_SUCCESS;
  }
  return SCR_FAILURE;
}

/* given a dataset id that has been flushed, the list provided by scr_flush_prepare,
 * and data to include in the summary file, complete the flush by writing the summary file */
int scr_flush_complete(int id, scr_hash* file_list, scr_hash* data)
{
  int flushed = SCR_SUCCESS;

  /* get the dataset of this flush */
  scr_dataset* dataset = scr_hash_get(file_list, SCR_KEY_DATASET);

  /* get the directory for the summary file */
  char* summary_dir;
  if (scr_hash_util_get_str(file_list, SCR_KEY_PATH, &summary_dir) != SCR_SUCCESS) {
    return SCR_FAILURE;
  }

  /* write summary file */
  if (scr_flush_summary(summary_dir, dataset, file_list, data) != SCR_SUCCESS) {
    flushed = SCR_FAILURE;
  }

  /* update index file */
  if (scr_my_rank_world == 0) {
    /* assume the flush failed */
    int complete = 0;
    if (flushed == SCR_SUCCESS) {
      /* remember that the flush was successful */
      complete = 1;

      /* read the index file */
      scr_hash* index_hash = scr_hash_new();
      scr_index_read(scr_prefix, index_hash);

      /* get name of subdirectory holding dataset */
      char subdir[SCR_MAX_FILENAME];
      if (scr_find_subdir(scr_prefix, summary_dir, subdir, sizeof(subdir)) == SCR_SUCCESS) {
        /* create new current to point to new directory */
        scr_index_set_current(index_hash, subdir);
      }

      /* record the dataset in the index file */
      int id;
      if (scr_dataset_get_id(dataset, &id) == SCR_SUCCESS) {
        scr_index_set_dataset(index_hash, id, subdir, dataset, complete);
      } else {
        scr_abort(-1, "Failed to read dataset id @ %s:%d",
          __FILE__, __LINE__
        );
      }

      /* write the index file and delete the hash */
      scr_index_write(scr_prefix, index_hash);
      scr_hash_delete(&index_hash);
    }
  }

  /* have rank 0 broadcast whether the entire flush succeeded,
   * including summary file and index update */
  MPI_Bcast(&flushed, 1, MPI_INT, 0, scr_comm_world);

  /* mark this dataset as flushed to the parallel file system */
  if (flushed == SCR_SUCCESS) {
    scr_flush_file_location_set(id, SCR_FLUSH_KEY_LOCATION_PFS);
  }

  return flushed;
}
