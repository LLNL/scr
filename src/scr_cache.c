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
#include "scr_cache_index.h"
#include "scr_storedesc.h"

#include "spath.h"
#include "kvtree.h"

/*
=========================================
Dataset cache functions
=========================================
*/

static char* scr_cache_dir_from_str(const char* dir, const char* storage_view, int id)
{
  /* build the dataset directory name */
  spath* path = spath_from_str(dir);
  if(! strcmp(storage_view, "GLOBAL")) {
    spath_append_strf(path, "node.%d", scr_my_hostid);
  }
  spath_append_strf(path, "scr.dataset.%d", id);
  spath_reduce(path);
  char* str = spath_strdup(path);
  spath_delete(&path);
  return str;
}

static char* scr_cache_dir_hidden_from_str(const char* dir, const char* storage_view, int id)
{
  /* build the dataset directory name */
  spath* path = spath_from_str(dir);
  if(! strcmp(storage_view, "GLOBAL")) {
    spath_append_strf(path, "node.%d", scr_my_hostid);
  }
  spath_append_strf(path, "scr.dataset.%d", id);
  spath_append_str(path, ".scr");
  spath_reduce(path);
  char* str = spath_strdup(path);
  spath_delete(&path);
  return str;
}

/* returns name of the dataset directory for a given redundancy descriptor
 * and dataset id, caller must free returned string */
char* scr_cache_dir_get(const scr_reddesc* red, int id)
{
  /* fatal error if c or c->directory is not set */
  if (red == NULL || red->directory == NULL) {
    scr_abort(-1, "NULL redundancy descriptor or NULL dataset directory @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* get the store descripter */
  scr_storedesc* store = scr_reddesc_get_store(red);
  if (store == NULL){
    scr_abort(-1, "attempting to build cache dir for null store @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* build the dataset directory name */
  char* str = scr_cache_dir_from_str(red->directory, store->view, id);
  return str;
}

/* returns name of hidden .scr subdirectory within the dataset directory
 * for a given redundancy descriptor and dataset id, caller must free
 * returned string */
char* scr_cache_dir_hidden_get(const scr_reddesc* red, int id)
{
  /* fatal error if c or c->directory is not set */
  if (red == NULL || red->directory == NULL) {
    scr_abort(-1, "NULL redundancy descriptor or NULL dataset directory @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* get the store descripter */
  scr_storedesc* store = scr_reddesc_get_store(red);
  if (store == NULL) {
    scr_abort(-1, "attempting to build cache hidden dir for null store @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* build the hidden directory name */
  char* str = scr_cache_dir_hidden_from_str(red->directory, store->view, id);
  return str;
}

/* create a dataset directory given a redundancy descriptor and dataset id,
 * waits for all tasks on the same node before returning */
int scr_cache_dir_create(const scr_reddesc* red, int id)
{
  int rc = SCR_SUCCESS;

  /* get store descriptor for this redudancy descriptor */
  scr_storedesc* store = scr_reddesc_get_store(red);
  if (store != NULL) {
    /* create directory on store */
    char* dir = scr_cache_dir_get(red, id);
    if (scr_storedesc_dir_create(store, dir) != SCR_SUCCESS) {
      /* check that we created the directory successfully,
       * fatal error if not */
      scr_abort(-1, "Failed to create dataset directory %s, aborting @ %s:%d",
        dir, __FILE__, __LINE__
      );
    }
    scr_free(&dir);

    /* create hidden .scr subdir within dataset directory */
    char* dir_scr = scr_cache_dir_hidden_get(red, id);
    if (scr_storedesc_dir_create(store, dir_scr) != SCR_SUCCESS) {
      scr_abort(-1, "Failed to create dataset directory %s, aborting @ %s:%d",
        dir_scr, __FILE__, __LINE__
      );
    }
    scr_free(&dir_scr);
  } else {
    scr_abort(-1, "Invalid store descriptor @ %s:%d",
      __FILE__, __LINE__
    );
  }

  return rc;
}

/* create and return spath object for map file for calling rank,
 * returns NULL on failure */
static spath* scr_cache_get_map_path(const scr_cache_index* cindex, int id)
{
  /* get directory for dataset */
  char* dir;
  if (scr_cache_index_get_dir(cindex, id, &dir)) {
    return NULL;
  }

  /* build path to map file for this process */
  spath* path = spath_from_str(dir);
  spath_append_str(path, ".scr");
  spath_append_strf(path, "filemap_%d", scr_my_rank_world);
  return path;
}

const char* scr_cache_get_map_file(const scr_cache_index* cindex, int id)
{
  /* get directory for dataset */
  spath* path = scr_cache_get_map_path(cindex, id);
  if (path == NULL) {
    return NULL;
  }

  /* get file name as string */
  const char* file = spath_strdup(path);

  /* free the path to the map file */
  spath_delete(&path);

  /* return path to caller */
  return file;
}

/* read file map for dataset from cache directory */
int scr_cache_get_map(const scr_cache_index* cindex, int id, scr_filemap* map)
{
  /* get directory for dataset */
  spath* path = scr_cache_get_map_path(cindex, id);
  if (path == NULL) {
    return SCR_FAILURE;
  }

  /* read map file */
  int rc = SCR_SUCCESS;
  if (scr_filemap_read(path, map) != SCR_SUCCESS) {
    rc = SCR_FAILURE;
  }

  /* free the path to the map file */
  spath_delete(&path);

  return rc;
}

/* write file map for dataset to cache directory */
int scr_cache_set_map(const scr_cache_index* cindex, int id, const scr_filemap* map)
{
  /* get directory for dataset */
  spath* path = scr_cache_get_map_path(cindex, id);
  if (path == NULL) {
    return SCR_FAILURE;
  }

  /* write map file */
  int rc = SCR_SUCCESS;
  if (scr_filemap_write(path, map) != SCR_SUCCESS) {
    rc = SCR_FAILURE;
  }

  /* free the path to the map file */
  spath_delete(&path);

  return rc;
}

/* delete file map file for dataset from cache directory */
int scr_cache_unset_map(const scr_cache_index* cindex, int id)
{
  /* get directory for dataset */
  spath* path = scr_cache_get_map_path(cindex, id);
  if (path == NULL) {
    return SCR_FAILURE;
  }

  /* delete the file */
  const char* file = spath_strdup(path);
  scr_file_unlink(file);
  scr_free(&file);

  /* free the path to the map file */
  spath_delete(&path);

  return SCR_SUCCESS;
}

/* remove all files associated with specified dataset */
int scr_cache_delete(scr_cache_index* cindex, int id)
{
  /* get cache directory for this dataset */
  char* dir = NULL;
  if (scr_cache_index_get_dir(cindex, id, &dir) == SCR_FAILURE) {
    /* assume dataset is not in cache if we fail to find its directory */
    return SCR_SUCCESS;
  }

  /* print a debug messages */
  if (scr_my_rank_world == 0) {
    scr_dataset* dataset = scr_dataset_new();
    scr_cache_index_get_dataset(cindex, id, dataset);
    char* dset_name;
    scr_dataset_get_name(dataset, &dset_name);
    scr_dbg(1, "Deleting dataset %d `%s' from cache", id, dset_name);
    scr_dataset_delete(&dataset);
  }

  /* build list to hidden directory */
  spath* path_scr = spath_from_str(dir);
  spath_append_str(path_scr, ".scr");
  char* dir_scr = spath_strdup(path_scr);
  spath_delete(&path_scr);

  /* remove redundancy files */
  scr_reddesc_unapply(cindex, id, dir_scr);
  
  /* if this dataset was a bypass, no need remove files since
   * those are on the file system (not cache), we will still
   * delete associated directories from cache and the filemap */
  int bypass = 0;
  scr_cache_index_get_bypass(cindex, id, &bypass);

  /* get list of files for this dataset */
  scr_filemap* map = scr_filemap_new();
  scr_cache_get_map(cindex, id, map);
  
  /* for each file we have for this dataset, delete the file */
  kvtree_elem* file_elem;
  for (file_elem = scr_filemap_first_file(map);
       file_elem != NULL;
       file_elem = kvtree_elem_next(file_elem))
  {
    /* get the filename */
    char* file = kvtree_elem_key(file_elem); 
  
    /* verify that file mtime and ctime have not changed since scr_complete_output,
     * which could idenitfy a bug in the user's code */
    struct stat statbuf;
    int stat_rc = stat(file, &statbuf);
    if (stat_rc == 0) {
      scr_meta* meta = scr_meta_new();
      scr_filemap_get_meta(map, file, meta);

      int file_changed = 0;

      /* check that file contents have not been modified */
      if (scr_meta_check_mtime(meta, &statbuf) != SCR_SUCCESS) {
        file_changed = 1;
        scr_warn("Detected mtime change in file `%s' since it was completed @ %s:%d",
          file, __FILE__, __LINE__
        );
      }

      /* check that permission bits, uid, and gid have not changed */
      if (scr_meta_check_metadata(meta, &statbuf) != SCR_SUCCESS) {
        file_changed = 1;
        scr_warn("Detected change in mode bits, uid, or gid on file `%s' since it was completed @ %s:%d",
          file, __FILE__, __LINE__
        );
      }

      if (file_changed) {
        scr_warn("Detected change in file `%s' since it was completed @ %s:%d",
          file, __FILE__, __LINE__
        );
      }

      scr_meta_delete(&meta);
    }
  
    /* check file's crc value (monitor that cache hardware isn't corrupting
     * files on us) */
    if (scr_crc_on_delete) {
      /* TODO: if corruption, need to log */
      if (scr_compute_crc(map, file) != SCR_SUCCESS) {
        scr_err("Failed to verify CRC32 before deleting file %s, bad drive? @ %s:%d",
          file, __FILE__, __LINE__
        );
      }
    }

    /* if we're not using bypass, delete data files from cache */
    if (! bypass) {
      /* delete the file */
      scr_file_unlink(file);
    }
  }
  
  /* delete map object */
  scr_filemap_delete(&map);

  /* delete the map file */
  scr_cache_unset_map(cindex, id);

  /* TODO: due to bug in scr_cache_rebuild, we need to pull the dataset directory
   * from somewhere other than the redundancy descriptor, which may not be defined */

  /* remove the cache directory for this dataset */
  int store_index = scr_storedescs_index_from_child_path(dir);
  int have_dir = (store_index >= 0 && store_index < scr_nstoredescs && dir != NULL);
  if (scr_alltrue(have_dir, scr_comm_world)) {
    /* get store descriptor */
    scr_storedesc* store = &scr_storedescs[store_index];

    /* remove hidden .scr subdirectory from cache */
    if (scr_storedesc_dir_delete(store, dir_scr) != SCR_SUCCESS) {
      scr_err("Failed to remove dataset directory: %s @ %s:%d",
        dir_scr, __FILE__, __LINE__
      );
    }
    
    /* remove the dataset directory from cache */
    if (scr_storedesc_dir_delete(store, dir) != SCR_SUCCESS) {
      scr_err("Failed to remove dataset directory: %s @ %s:%d",
        dir, __FILE__, __LINE__
      );
    }
  } else {
    /* TODO: We end up here if at least one process does not have its
     * reddeesc for this dataset.  We could try to have each process delete
     * directories directly, or we could use DTCMP to assign a new leader
     * for each directory to clean up, but we can't call
     * scr_storedesc_dir_delete() since the barrier in that function
     * could lead to deadlock.  For now, skip the cleanup, and just leave
     * the directories in place.  We should run ok, but we may leave
     * some cruft behind. */
  }

  /* delete any entry in the flush file for this dataset */
  scr_flush_file_dataset_remove(id);

  /* TODO: remove data from transfer file for this dataset */

  /* remove this dataset from the index and write updated index to disk */
  scr_cache_index_remove_dataset(cindex, id);
  scr_cache_index_write(scr_cindex_file, cindex);

  /* free path to hidden directory */
  scr_free(&dir_scr);

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

/* remove all files recorded in filemap and the filemap itself */
int scr_cache_purge(scr_cache_index* cindex)
{
  /* TODO: put dataset selection logic into a function */

  /* get the list of datasets we have in our cache */
  int ndsets;
  int* dsets;
  scr_cache_index_list_datasets(cindex, &ndsets, &dsets);

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
      scr_cache_delete(cindex, current_id);
    }
  } while (current_id != -1);

  /* free our list of dataset ids */
  scr_free(&dsets);

  /* delete the cache index file itself */
  char* file = spath_strdup(scr_cindex_file);
  scr_file_unlink(file);
  scr_free(&file);

  /* clear the cache index object */
  scr_cache_index_clear(cindex);

  return 1;
}

/* delete dataset with matching name from cache, if one exists */
int scr_cache_delete_by_name(scr_cache_index* cindex, const char* name)
{
  /* TODO: put dataset selection logic into a function */
  /* TODO: need to worry about different procs having different ids for a given name? */

  /* get the list of datasets we have in our cache */
  int ndsets;
  int* dsets;
  scr_cache_index_list_datasets(cindex, &ndsets, &dsets);

  /* TODO: also attempt to recover datasets which we were in the
   * middle of flushing */
  int current_id;
  int dset_index = 0;
  do {
    /* we'll set this to the dataset id if we find one that matches the target name */
    int delete_id = -1;

    /* get the smallest index across all processes (returned in current_id),
     * this also updates our dset_index value if appropriate */
    scr_next_dataset(ndsets, dsets, &dset_index, &current_id);
    
    /* if we found a dataset id, check its name against target name */
    if (current_id != -1) {
      /* get dataset for this id */
      scr_dataset* dataset = scr_dataset_new();
      scr_cache_index_get_dataset(cindex, current_id, dataset);

      /* check the name of this dataset to the given name */
      char* dset_name;
      if (scr_dataset_get_name(dataset, &dset_name) == SCR_SUCCESS) {
        if (strcmp(name, dset_name) == 0) {
          /* found a match, record the id */
          delete_id = current_id;
        }
      }

      /* release the dataset and lookup the id of the next oldest dataset */
      scr_dataset_delete(&dataset);
    }

    /* if we found a matching dataset, delete it */
    if (delete_id != -1) {
      /* remove this dataset from all tasks */
      scr_cache_delete(cindex, delete_id);
    }
  } while (current_id != -1);

  /* free our list of dataset ids */
  scr_free(&dsets);

  return SCR_SUCCESS;
}

#if 0
/* opens the filemap, inspects that all listed files are readable and complete,
 * unlinks any that are not */
int scr_cache_clean(scr_filemap* map)
{
  /* create a map to remember which files to keep */
  scr_filemap* keep_map = scr_filemap_new();

  /* scan each file for each rank of each checkpoint */
  kvtree_elem* dset_elem;
  for (dset_elem = scr_filemap_first_dataset(map);
       dset_elem != NULL;
       dset_elem = kvtree_elem_next(dset_elem))
  {
    /* get the dataset id */
    int dset = kvtree_elem_key_int(dset_elem);

    kvtree_elem* rank_elem;
    for (rank_elem = scr_filemap_first_rank_by_dataset(map, dset);
         rank_elem != NULL;
         rank_elem = kvtree_elem_next(rank_elem))
    {
      /* get the rank id */
      int rank = kvtree_elem_key_int(rank_elem);

      /* if we're missing any file for this rank in this checkpoint,
       * we'll delete them all */
      int missing_file = 0;

      /* first time through the file list, check that we have each file */
      kvtree_elem* file_elem = NULL;
      for (file_elem = scr_filemap_first_file(map, dset, rank);
           file_elem != NULL;
           file_elem = kvtree_elem_next(file_elem))
      {
        /* get filename */
        char* file = kvtree_elem_key(file_elem);

        /* check whether we have it */
        if (! scr_bool_have_file(map, dset, rank, file, scr_ranks_world)) {
            missing_file = 1;
            scr_dbg(2, "File is unreadable or incomplete: Dataset %d, Rank %d, File: %s",
              dset, rank, file
            );
        }
      }

      /* add redundancy descriptor to keep map, if one is set */
      kvtree* desc = kvtree_new();
      if (scr_filemap_get_desc(map, desc) == SCR_SUCCESS) {
        scr_filemap_set_desc(keep_map, desc);
      }
      kvtree_delete(&desc);

      /* add dataset descriptor to keep map, if one is set */
      kvtree* dataset = kvtree_new();
      if (scr_filemap_get_dataset(map, dataset) == SCR_SUCCESS) {
        scr_filemap_set_dataset(keep_map, dataset);
      }
      kvtree_delete(&dataset);

      /* second time through, either add all files to keep_map or delete
       * them all */
      for (file_elem = scr_filemap_first_file(map, dset, rank);
           file_elem != NULL;
           file_elem = kvtree_elem_next(file_elem))
      {
        /* get the filename */
        char* file = kvtree_elem_key(file_elem);

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
          if (scr_filemap_get_meta(map, file, meta) == SCR_SUCCESS) {
            scr_filemap_set_meta(keep_map, file, meta);
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
#endif

/* returns true iff each file in the filemap can be read */
int scr_cache_check_files(const scr_cache_index* cindex, int id)
{
  int failed_read = 0;

  /* get map of files for this dataset */
  scr_filemap* map = scr_filemap_new();
  scr_cache_get_map(cindex, id, map);

  /* loop over each file in the map */
  kvtree_elem* file_elem;
  for (file_elem = scr_filemap_first_file(map);
       file_elem != NULL;
       file_elem = kvtree_elem_next(file_elem))
  {
    /* get the filename */
    char* file = kvtree_elem_key(file_elem);

    /* check that we can read the file */
    if (scr_file_is_readable(file) != SCR_SUCCESS) {
      failed_read = 1;
    }

    /* get meta data for this file */
    scr_meta* meta = scr_meta_new();
    if (scr_filemap_get_meta(map, file, meta) != SCR_SUCCESS) {
      failed_read = 1;
    } else {
      /* check that the file is complete */
      if (scr_meta_is_complete(meta) != SCR_SUCCESS) {
        failed_read = 1;
      }
    }
    scr_meta_delete(&meta);
  }

  /* free the map */
  scr_filemap_delete(&map);

  /* if we failed to read a file, assume the set is incomplete */
  if (failed_read) {
    /* TODO: want to unlink all files in this case? */
    return SCR_FAILURE;
  }
  return SCR_SUCCESS;
}

/* checks whether specifed file exists, is readable, and is complete */
int scr_bool_have_file(const scr_filemap* map, const char* file)
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
  if (scr_filemap_get_meta(map, file, meta) != SCR_SUCCESS) {
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

/* compute and store crc32 value for specified file in given dataset and rank,
 * check against current value if one is set */
int scr_compute_crc(scr_filemap* map, const char* file)
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
  if (scr_filemap_get_meta(map, file, meta) != SCR_SUCCESS) {
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
    scr_filemap_set_meta(map, file, meta);
  }

  /* free our meta data object */
  scr_meta_delete(&meta);

  return rc;
}

/* return store descriptor associated with dataset, returns NULL if not found */
scr_storedesc* scr_cache_get_storedesc(const scr_cache_index* cindex, int id)
{
  /* get directory associated with this dataset */
  char* dir;
  if (scr_cache_index_get_dir(cindex, id, &dir) != SCR_SUCCESS) {
    return NULL;
  }

  /* lookup store descriptor index based on path */
  int store_index = scr_storedescs_index_from_child_path(dir);
  if (store_index < 0 || store_index >= scr_nstoredescs) {
    return NULL;
  }

  /* return address of store descriptor */
  scr_storedesc* storedesc = &scr_storedescs[store_index];
  return storedesc;
}
