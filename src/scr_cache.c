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
Dataset cache functions
=========================================
*/

/* allocates and returns a string representing the dataset directory
 * given a path and dataset id */
static char* scr_cache_dir_build(const char* path, int id)
{
  scr_path* dir = scr_path_from_str(path);
  scr_path_append_strf(dir, "dataset.%d", id);
  scr_path_reduce(dir);
  char* str = scr_path_strdup(dir);
  scr_path_delete(&dir);
  return str;
}

/* returns name of the dataset directory for a given redundancy descriptor
 * and dataset id */
int scr_cache_dir_get(const scr_reddesc* red, int id, char* dir)
{
  /* fatal error if c or c->directory is not set */
  if (red == NULL || red->directory == NULL) {
    scr_abort(-1, "NULL redundancy descriptor or NULL dataset directory @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* build the dataset directory name */
  char* tmp = scr_cache_dir_build(red->directory, id);
  strcpy(dir, tmp);
  scr_free(&tmp);

  return SCR_SUCCESS;
}

/* create a dataset directory given a redundancy descriptor and dataset id,
 * waits for all tasks on the same node before returning */
int scr_cache_dir_create(const scr_reddesc* red, int id)
{
  int rc = SCR_SUCCESS;

  /* get store descriptor for this redudancy descriptor */
  scr_storedesc* store = scr_reddesc_get_store(red);
  if (store != NULL) {
    /* get the name of the dataset directory for the given id */
    char dir[SCR_MAX_FILENAME];
    scr_cache_dir_get(red, id, dir);

    /* create directory on store */
    if (scr_storedesc_dir_create(store, dir) != SCR_SUCCESS)
    {
      /* check that we created the directory successfully,
       * fatal error if not */
      scr_abort(-1, "Failed to create dataset directory, aborting @ %s:%d",
        __FILE__, __LINE__
      );
    }
  } else {
    scr_abort(-1, "Invalid store descriptor @ %s:%d",
      __FILE__, __LINE__
    );
  }

  return rc;
}

/* remove all files associated with specified dataset */
int scr_cache_delete(scr_filemap* map, int id)
{
  /* print a debug messages */
  if (scr_my_rank_world == 0) {
    scr_dbg(1, "Deleting dataset %d from cache", id);
  }

  /* for each file of each rank we have for this dataset, delete the file */
  scr_hash_elem* rank_elem;
  for (rank_elem = scr_filemap_first_rank_by_dataset(map, id);
       rank_elem != NULL;
       rank_elem = scr_hash_elem_next(rank_elem))
  {
    /* get the rank id */
    int rank = scr_hash_elem_key_int(rank_elem);

    scr_hash_elem* file_elem;
    for (file_elem = scr_filemap_first_file(map, id, rank);
         file_elem != NULL;
         file_elem = scr_hash_elem_next(file_elem))
    {
      /* get the filename */
      char* file = scr_hash_elem_key(file_elem); 

      /* check file's crc value (monitor that cache hardware isn't corrupting
       * files on us) */
      if (scr_crc_on_delete) {
        /* TODO: if corruption, need to log */
        if (scr_compute_crc(map, id, rank, file) != SCR_SUCCESS) {
          scr_err("Failed to verify CRC32 before deleting file %s, bad drive? @ %s:%d",
            file, __FILE__, __LINE__
          );
        }
      }

      /* delete the file */
      scr_file_unlink(file);
    }
  }

  /* TODO: due to bug in scr_cache_rebuild, we need to pull the dataset directory
   * from somewhere other than the redundancy descriptor, which may not be defined */

  /* remove the cache directory for this dataset */
  char* base = scr_reddesc_base_from_filemap(map, id, scr_my_rank_world);
  char* dir  = scr_reddesc_dir_from_filemap(map, id, scr_my_rank_world);
  int store_index = scr_storedescs_index_from_name(base);
  if (store_index >= 0 && store_index < scr_nstoredescs && dir != NULL) {
    /* build name of dataset directory */
    char* dataset_dir = scr_cache_dir_build(dir, id);

    /* remove the dataset directory from cache */
    scr_storedesc* store = &scr_storedescs[store_index];
    if (scr_storedesc_dir_delete(store, dataset_dir) != SCR_SUCCESS) {
      scr_err("Failed to remove dataset directory: %s @ %s:%d",
        dataset_dir, __FILE__, __LINE__
      );
    }

    /* free off dataset directory string */
    scr_free(&dataset_dir);
  } else {
    /* TODO: abort! */
  }
  scr_free(&dir);
  scr_free(&base);

  /* delete any entry in the flush file for this dataset */
  scr_flush_file_dataset_remove(id);

  /* TODO: remove data from transfer file for this dataset */

  /* remove this dataset from the filemap, and write new filemap to disk */
  scr_filemap_remove_dataset(map, id);
  scr_filemap_write(scr_map_file, map);

  return SCR_SUCCESS;
}

/* each process passes in an ordered list of dataset ids along with a current
 * index, this function identifies the next smallest id across all processes
 * and returns this id in current, it also updates index on processes as
 * appropriate */
int scr_next_dataset(int ndsets, const int* dsets, int* index, int* current)
{
  int dset_index = *index;

  /* get the next dataset we have in our list */
  int id = -1;
  if (dset_index < ndsets) {
    id = dsets[dset_index];
  }

  /* find the maximum dataset id across all ranks */
  int current_id;
  MPI_Allreduce(&id, &current_id, 1, MPI_INT, MPI_MAX, scr_comm_world);

  /* if any process has any dataset, identify the smallest */
  if (current_id != -1) {
    /* now find the minimum dataset id (that's not -1) */
    if (id == -1) {
      /* if we don't have a dataset, set our id to the max to avoid
       * picking -1 as the minimum */
      id = current_id;
    }
    MPI_Allreduce(&id, &current_id, 1, MPI_INT, MPI_MIN, scr_comm_world);

    /* if the current id matches our id, increment our index for the next
     * iteration */
    if (current_id == id) {
      dset_index++;
    }
  }

  /* update caller's current index value and the dataset id we settled on */
  *index   = dset_index;
  *current = current_id;

  return SCR_SUCCESS;
}

/* given a filemap, a dataset, and a rank, unlink those files and remove
 * them from the map */
int scr_unlink_rank(scr_filemap* map, int id, int rank)
{
  /* delete each file and remove its metadata file */
  scr_hash_elem* file_elem;
  for (file_elem = scr_filemap_first_file(map, id, rank);
       file_elem != NULL;
       file_elem = scr_hash_elem_next(file_elem))
  {
    char* file = scr_hash_elem_key(file_elem);
    if (file != NULL) {
      scr_dbg(2, "Delete file Dataset %d, Rank %d, File %s", id, rank, file);

      /* delete the file */
      scr_file_unlink(file);

      /* remove the file from the map */
      scr_filemap_remove_file(map, id, rank, file);
    }
  }

  /* unset the expected number of files for this rank */
  scr_filemap_unset_expected_files(map, id, rank);

  /* write the new filemap to disk */
  scr_filemap_write(scr_map_file, map);

  return SCR_SUCCESS;
}

/* remove all files recorded in filemap and the filemap itself */
int scr_cache_purge(scr_filemap* map)
{
  /* TODO: put dataset selection logic into a function */

  /* get the list of datasets we have in our cache */
  int ndsets;
  int* dsets;
  scr_filemap_list_datasets(map, &ndsets, &dsets);

  /* TODO: also attempt to recover datasets which we were in the
   * middle of flushing */
  int current_id;
  int dset_index = 0;
  do {
    /* get the smallest index across all processes (returned in current_id),
     * this also updates our dset_index value if appropriate */
    scr_next_dataset(ndsets, dsets, &dset_index, &current_id);
    
    /* if we found a dataset, delete it */
    if (current_id != -1) {
      /* remove this dataset from all tasks */
      scr_cache_delete(map, current_id);
    }
  } while (current_id != -1);

  /* now delete the filemap itself */
  char* file = scr_path_strdup(scr_map_file);
  scr_file_unlink(file);
  scr_free(&file);

  /* TODO: want to clear the map object here? */

  /* TODO: want to delete the master map file? */

  /* free our list of dataset ids */
  scr_free(&dsets);

  return 1;
}

/* opens the filemap, inspects that all listed files are readable and complete,
 * unlinks any that are not */
int scr_cache_clean(scr_filemap* map)
{
  /* create a map to remember which files to keep */
  scr_filemap* keep_map = scr_filemap_new();

  /* scan each file for each rank of each checkpoint */
  scr_hash_elem* dset_elem;
  for (dset_elem = scr_filemap_first_dataset(map);
       dset_elem != NULL;
       dset_elem = scr_hash_elem_next(dset_elem))
  {
    /* get the dataset id */
    int dset = scr_hash_elem_key_int(dset_elem);

    scr_hash_elem* rank_elem;
    for (rank_elem = scr_filemap_first_rank_by_dataset(map, dset);
         rank_elem != NULL;
         rank_elem = scr_hash_elem_next(rank_elem))
    {
      /* get the rank id */
      int rank = scr_hash_elem_key_int(rank_elem);

      /* if we're missing any file for this rank in this checkpoint,
       * we'll delete them all */
      int missing_file = 0;

      /* first time through the file list, check that we have each file */
      scr_hash_elem* file_elem = NULL;
      for (file_elem = scr_filemap_first_file(map, dset, rank);
           file_elem != NULL;
           file_elem = scr_hash_elem_next(file_elem))
      {
        /* get filename */
        char* file = scr_hash_elem_key(file_elem);

        /* check whether we have it */
        if (! scr_bool_have_file(map, dset, rank, file, scr_ranks_world)) {
            missing_file = 1;
            scr_dbg(2, "File is unreadable or incomplete: Dataset %d, Rank %d, File: %s",
              dset, rank, file
            );
        }
      }

      /* add redundancy descriptor to keep map, if one is set */
      scr_hash* desc = scr_hash_new();
      if (scr_filemap_get_desc(map, dset, rank, desc) == SCR_SUCCESS) {
        scr_filemap_set_desc(keep_map, dset, rank, desc);
      }
      scr_hash_delete(&desc);

      /* add dataset descriptor to keep map, if one is set */
      scr_hash* dataset = scr_hash_new();
      if (scr_filemap_get_dataset(map, dset, rank, dataset) == SCR_SUCCESS) {
        scr_filemap_set_dataset(keep_map, dset, rank, dataset);
      }
      scr_hash_delete(&dataset);

      /* check whether we have all the files we think we should */
      int expected_files = scr_filemap_get_expected_files(map, dset, rank);
      int num_files = scr_filemap_num_files(map, dset, rank);
      if (num_files != expected_files) {
        missing_file = 1;
      }

      /* if we have all the files, set the expected file number in the
       * keep_map */
      if (! missing_file) {
        scr_filemap_set_expected_files(keep_map, dset, rank, expected_files);
      }

      /* second time through, either add all files to keep_map or delete
       * them all */
      for (file_elem = scr_filemap_first_file(map, dset, rank);
           file_elem != NULL;
           file_elem = scr_hash_elem_next(file_elem))
      {
        /* get the filename */
        char* file = scr_hash_elem_key(file_elem);

        /* if we failed to read any file, delete them all 
         * otherwise add them all to the keep_map */
        if (missing_file) {
          /* inform user on what we're doing */
          scr_dbg(2, "Deleting file: Dataset %d, Rank %d, File: %s",
            dset, rank, file
          );

          /* delete the file */
          scr_file_unlink(file);
        } else {
          /* keep this file */
          scr_filemap_add_file(keep_map, dset, rank, file);

          /* copy the meta data */
          scr_meta* meta = scr_meta_new();
          if (scr_filemap_get_meta(map, dset, rank, file, meta) == SCR_SUCCESS) {
            scr_filemap_set_meta(keep_map, dset, rank, file, meta);
          }
          scr_meta_delete(&meta);
        }
      }
    }
  }

  /* clear our current map, merge the keep_map into it,
   * and write the map to disk */
  scr_filemap_clear(map);
  scr_filemap_merge(map, keep_map);
  scr_filemap_write(scr_map_file, map);

  /* free the keep_map object */
  scr_filemap_delete(&keep_map);

  return SCR_SUCCESS;
}

/* returns true iff each file in the filemap can be read */
int scr_cache_check_files(const scr_filemap* map, int id)
{
  /* for each file of each rank for specified dataset id */
  int failed_read = 0;
  scr_hash_elem* rank_elem;
  for (rank_elem = scr_filemap_first_rank_by_dataset(map, id);
       rank_elem != NULL;
       rank_elem = scr_hash_elem_next(rank_elem))
  {
    /* get the rank id */
    int rank = scr_hash_elem_key_int(rank_elem);

    scr_hash_elem* file_elem;
    for (file_elem = scr_filemap_first_file(map, id, rank);
         file_elem != NULL;
         file_elem = scr_hash_elem_next(file_elem))
    {
      /* get the filename */
      char* file = scr_hash_elem_key(file_elem);

      /* check that we can read the file */
      if (scr_file_is_readable(file) != SCR_SUCCESS) {
        failed_read = 1;
      }

      /* get meta data for this file */
      scr_meta* meta = scr_meta_new();
      if (scr_filemap_get_meta(map, id, rank, file, meta) != SCR_SUCCESS) {
        failed_read = 1;
      } else {
        /* check that the file is complete */
        if (scr_meta_is_complete(meta) != SCR_SUCCESS) {
          failed_read = 1;
        }
      }
      scr_meta_delete(&meta);
    }
  }

  /* if we failed to read a file, assume the set is incomplete */
  if (failed_read) {
    /* TODO: want to unlink all files in this case? */
    return SCR_FAILURE;
  }
  return SCR_SUCCESS;
}

/* checks whether specifed file exists, is readable, and is complete */
int scr_bool_have_file(const scr_filemap* map, int dset, int rank, const char* file, int ranks)
{
  /* if no filename is given return false */
  if (file == NULL || strcmp(file,"") == 0) {
    scr_dbg(2, "File name is null or the empty string @ %s:%d",
      __FILE__, __LINE__
    );
    return 0;
  }

  /* check that we can read the file */
  if (scr_file_is_readable(file) != SCR_SUCCESS) {
    scr_dbg(2, "Do not have read access to file: %s @ %s:%d",
      file, __FILE__, __LINE__
    );
    return 0;
  }

  /* allocate object to read meta data into */
  scr_meta* meta = scr_meta_new();

  /* check that we can read meta file for the file */
  if (scr_filemap_get_meta(map, dset, rank, file, meta) != SCR_SUCCESS) {
    scr_dbg(2, "Failed to read meta data for file: %s @ %s:%d",
      file, __FILE__, __LINE__
    );
    scr_meta_delete(&meta);
    return 0;
  }

  /* check that the file is complete */
  if (scr_meta_is_complete(meta) != SCR_SUCCESS) {
    scr_dbg(2, "File is marked as incomplete: %s @ %s:%d",
      file, __FILE__, __LINE__
    );
    scr_meta_delete(&meta);
    return 0;
  }

  /* TODODSET: enable check for correct dataset / checkpoint id */

#if 0
  /* check that the file really belongs to the checkpoint id we think it does */
  int meta_dset = -1;
  if (scr_meta_get_dataset(meta, &meta_dset) != SCR_SUCCESS) {
    scr_dbg(2, "Failed to read dataset field in meta data: %s @ %s:%d",
            file, __FILE__, __LINE__
    );
    scr_meta_delete(&meta);
    return 0;
  }
  if (dset != meta_dset) {
    scr_dbg(2, "File's dataset ID (%d) does not match id in meta data file (%d) for %s @ %s:%d",
            dset, meta_dset, file, __FILE__, __LINE__
    );
    scr_meta_delete(&meta);
    return 0;
  }
#endif

#if 0
  /* check that the file really belongs to the rank we think it does */
  int meta_rank = -1;
  if (scr_meta_get_rank(meta, &meta_rank) != SCR_SUCCESS) {
    scr_dbg(2, "Failed to read rank field in meta data: %s @ %s:%d",
            file, __FILE__, __LINE__
    );
    scr_meta_delete(&meta);
    return 0;
  }
  if (rank != meta_rank) {
    scr_dbg(2, "File's rank (%d) does not match rank in meta data (%d) for %s @ %s:%d",
            rank, meta_rank, file, __FILE__, __LINE__
    );
    scr_meta_delete(&meta);
    return 0;
  }
#endif

#if 0
  /* check that the file really belongs to the rank we think it does */
  int meta_ranks = -1;
  if (scr_meta_get_ranks(meta, &meta_ranks) != SCR_SUCCESS) {
    scr_dbg(2, "Failed to read ranks field in meta data: %s @ %s:%d",
            file, __FILE__, __LINE__
    );
    scr_meta_delete(&meta);
    return 0;
  }
  if (ranks != meta_ranks) {
    scr_dbg(2, "File's ranks (%d) does not match ranks in meta data file (%d) for %s @ %s:%d",
            ranks, meta_ranks, file, __FILE__, __LINE__
    );
    scr_meta_delete(&meta);
    return 0;
  }
#endif

  /* check that the file size matches */
  unsigned long size = scr_file_size(file);
  unsigned long meta_size = 0;
  if (scr_meta_get_filesize(meta, &meta_size) != SCR_SUCCESS) {
    scr_dbg(2, "Failed to read filesize field in meta data: %s @ %s:%d",
      file, __FILE__, __LINE__
    );
    scr_meta_delete(&meta);
    return 0;
  }
  if (size != meta_size) {
    scr_dbg(2, "Filesize is incorrect, currently %lu, expected %lu for %s @ %s:%d",
      size, meta_size, file, __FILE__, __LINE__
    );
    scr_meta_delete(&meta);
    return 0;
  }

  /* TODO: check that crc32 match if set (this would be expensive) */

  /* free meta data object */
  scr_meta_delete(&meta);

  /* if we made it here, assume the file is good */
  return 1;
}

/* check whether we have all files for a given rank of a given dataset */
int scr_bool_have_files(const scr_filemap* map, int id, int rank)
{
  /* check whether we have any files for the specified rank */
  if (! scr_filemap_have_rank_by_dataset(map, id, rank)) {
    return 0;
  }

  /* check whether we have all of the files we should */
  int expected_files = scr_filemap_get_expected_files(map, id, rank);
  int num_files = scr_filemap_num_files(map, id, rank);
  if (num_files != expected_files) {
    return 0;
  }

  /* check the integrity of each of the files */
  int missing_a_file = 0;
  scr_hash_elem* file_elem;
  for (file_elem = scr_filemap_first_file(map, id, rank);
       file_elem != NULL;
       file_elem = scr_hash_elem_next(file_elem))
  {
    char* file = scr_hash_elem_key(file_elem);
    if (! scr_bool_have_file(map, id, rank, file, scr_ranks_world)) {
      missing_a_file = 1;
    }
  }
  if (missing_a_file) {
    return 0;
  }

  /* if we make it here, we have all of our files */
  return 1;
}

/* compute and store crc32 value for specified file in given dataset and rank,
 * check against current value if one is set */
int scr_compute_crc(scr_filemap* map, int id, int rank, const char* file)
{
  /* compute crc for the file */
  uLong crc_file;
  if (scr_crc32(file, &crc_file) != SCR_SUCCESS) {
    scr_err("Failed to compute crc for file %s @ %s:%d",
      file, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* allocate a new meta data object */
  scr_meta* meta = scr_meta_new();
  if (meta == NULL) {
    scr_abort(-1, "Failed to allocate meta data object @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* read meta data from filemap */
  if (scr_filemap_get_meta(map, id, rank, file, meta) != SCR_SUCCESS) {
    return SCR_FAILURE;
  }

  int rc = SCR_SUCCESS;

  /* read crc value from meta data */
  uLong crc_meta;
  if (scr_meta_get_crc32(meta, &crc_meta) == SCR_SUCCESS) {
    /* check that the values are the same */
    if (crc_file != crc_meta) {
      rc = SCR_FAILURE;
    }
  } else {
    /* record crc in filemap */
    scr_meta_set_crc32(meta, crc_file);
    scr_filemap_set_meta(map, id, rank, file, meta);
  }

  /* free our meta data object */
  scr_meta_delete(&meta);

  return rc;
}
