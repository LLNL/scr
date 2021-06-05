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
#include "dtcmp.h"
#include "scr_flush_nompi.h"

/*
=========================================
Prepare for flush by building list of files, creating directories,
and creating container files (if any)
=========================================
*/

/* given file list from flush_prepare,
 * allocate and fill in lists of source and destination file paths,
 * caller should free arrays with call to list_free */
int scr_flush_list_alloc(
  const kvtree* file_list,
  int* out_num_files,
  char*** out_src_filelist,
  char*** out_dst_filelist)
{
  /* assume we will succeed in this flush */
  int rc = SCR_SUCCESS;

  /* initialize output params */
  *out_num_files = 0;
  *out_src_filelist = NULL;
  *out_dst_filelist = NULL;

  /* get pointer to file list */
  kvtree* files = kvtree_get(file_list, SCR_KEY_FILE);

  /* allocate space to hold list of file names */
  int numfiles = kvtree_size(files);
  const char** src_filelist = (const char**) SCR_MALLOC(numfiles * sizeof(const char*));
  const char** dst_filelist = (const char**) SCR_MALLOC(numfiles * sizeof(const char*));

  /* record source and destination paths for each file */
  int i = 0;
  kvtree_elem* elem = NULL;
  for (elem = kvtree_elem_first(files);
       elem != NULL;
       elem = kvtree_elem_next(elem))
  {
    /* get the filename */
    char* file = kvtree_elem_key(elem);

    /* get the hash for this element */
    kvtree* hash = kvtree_elem_hash(elem);

    /* get meta data for this file */
    scr_meta* meta = kvtree_get(hash, SCR_KEY_META);

    /* get directory to flush file to */
    char* origpath;
    if (scr_meta_get_origpath(meta, &origpath) == SCR_SUCCESS) {
      char* origname;
      if (scr_meta_get_origname(meta, &origname) == SCR_SUCCESS) {
        /* build full path for destination file */
        spath* dest_path = spath_from_str(origpath);
        spath_append_str(dest_path, origname);
        char* destfile = spath_strdup(dest_path);

        /* add file to our list */
        src_filelist[i] = strdup(file);
        dst_filelist[i] = strdup(destfile);
        i++;

        spath_delete(&dest_path);
        scr_free(&destfile);
      } else {
        scr_abort(-1, "Failed to read directory to flush file to @ %s:%d",
          __FILE__, __LINE__
        );
      }
    } else {
      scr_abort(-1, "Failed to read directory to flush file to @ %s:%d",
        __FILE__, __LINE__
      );
    }
  }

  /* set output params */
  *out_num_files = numfiles;
  *out_src_filelist = (char**) src_filelist;
  *out_dst_filelist = (char**) dst_filelist;

  return rc;
}

/* free list allocated in list_alloc */
int scr_flush_list_free(
  int num_files,
  char*** ptr_src_filelist,
  char*** ptr_dst_filelist)
{
  char** src_filelist = *ptr_src_filelist;
  char** dst_filelist = *ptr_dst_filelist;

  /* free our file list */
  int i;
  for (i = 0; i < num_files; i++) {
    scr_free(&src_filelist[i]);
    scr_free(&dst_filelist[i]);
  }
  scr_free(ptr_src_filelist);
  scr_free(ptr_dst_filelist);

  return SCR_SUCCESS;
}

/* create directories from basepath down to each file as needed */
int scr_flush_create_dirs(
  const char* basepath,       /* top-level directory, assumed to exist */
  int count,                  /* number of files */
  const char** dest_filelist, /* list of files */
  MPI_Comm comm)              /* communicator of participating processes */
{
  /* TODO: need to list dirs in order from parent to child */

  /* allocate buffers to hold the directory needed for each file */
  int* leader           = (int*)         SCR_MALLOC(sizeof(int)         * count);
  const char** dirs     = (const char**) SCR_MALLOC(sizeof(const char*) * count);
  uint64_t* group_id    = (uint64_t*)    SCR_MALLOC(sizeof(uint64_t)    * count);
  uint64_t* group_ranks = (uint64_t*)    SCR_MALLOC(sizeof(uint64_t)    * count);
  uint64_t* group_rank  = (uint64_t*)    SCR_MALLOC(sizeof(uint64_t)    * count);

  /* lookup directory from meta data for each file */
  int i;
  for (i = 0; i < count; i++) {
    /* extract directory from filename */
    const char* filename = dest_filelist[i];
    spath* path = spath_from_str(filename);
    spath_dirname(path);
    dirs[i] = spath_strdup(path);
    spath_delete(&path);

    /* we'll use DTCMP to select one leader for each directory later */
    leader[i] = 0;
  }

  /* with DTCMP we identify a single process to create each directory */

  /* identify the set of unique directories */
  uint64_t groups;
  DTCMP_Rankv_strings(
    count, dirs, &groups, group_id, group_ranks, group_rank,
    DTCMP_FLAG_NONE, comm
  );

  /* select leader for each directory */
  for (i = 0; i < count; i++) {
    if (group_rank[i] == 0) {
      leader[i] = 1;
    }
  }

  /* get file mode for directory permissions */
  mode_t mode_dir = scr_getmode(1, 1, 1);

  /* TODO: add flow control here */

  /* create other directories in file list */
  int success = 1;
  for (i = 0; i < count; i++) {
    /* get directory name */
    const char* dir = dirs[i];

    /* if we're the leader, create directory */
    if (leader[i]) {
      if (scr_mkdir(dir, mode_dir) != SCR_SUCCESS) {
        success = 0;
      }
    }

    /* free the dirname we strdup'd */
    scr_free(&dir);
  }

  /* free buffers */
  scr_free(&group_id);
  scr_free(&group_ranks);
  scr_free(&group_rank);
  scr_free(&dirs);
  scr_free(&leader);

  /* determine whether all leaders successfully created their directories */
  if (! scr_alltrue(success == 1, comm)) {
    return SCR_FAILURE;
  }
  return SCR_SUCCESS;
}

/* given a dataset, return a newly allocated string specifying the
 * metadata directory for that dataset, must be freed by caller */
char* scr_flush_dataset_metadir(const scr_dataset* dataset)
{
  /* get the name of the dataset */
  int id;
  if (scr_dataset_get_id(dataset, &id) != SCR_SUCCESS) {
    scr_abort(-1, "Failed to get dataset id @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* define metadata directory for dataset */
  spath* path = spath_from_str(scr_prefix_scr);
  spath_append_strf(path, "scr.dataset.%d", id);
  char* dir = spath_strdup(path);
  spath_delete(&path);

  return dir;
}

/* given a filemap and a dataset id, prepare and return a list of
 * files to be flushed */
int scr_flush_prepare(const scr_cache_index* cindex, int id, kvtree* file_list)
{
  /* assume we'll succeed */
  int rc = SCR_SUCCESS;

  /* check that we have all of our files */
  int have_files = 1;
  if (scr_cache_check_files(cindex, id) != SCR_SUCCESS) {
    scr_err("Missing one or more files for dataset %d @ %s:%d",
      id, __FILE__, __LINE__
    );
    have_files = 0;
  }

  if (! scr_alltrue(have_files, scr_comm_world)) {
    if (scr_my_rank_world == 0) {
      scr_err("One or more processes are missing files for dataset %d @ %s:%d",
        id, __FILE__, __LINE__
      );
    }
    return SCR_FAILURE;
  }

  /* lookup dataset from filemap and store in file list */
  scr_dataset* dataset = kvtree_new();
  scr_cache_index_get_dataset(cindex, id, dataset);
  kvtree_set(file_list, SCR_KEY_DATASET, dataset);

  /* get filemap from cache */
  scr_filemap* map = scr_filemap_new();
  scr_cache_get_map(cindex, id, map);

  /* identify which files we need to flush as part of the specified
   * dataset id */
  kvtree_elem* elem = NULL;
  for (elem = scr_filemap_first_file(map);
       elem != NULL;
       elem = kvtree_elem_next(elem))
  {
    /* get the filename */
    char* file = kvtree_elem_key(elem);

    /* read meta data for file and attach it to file list */
    scr_meta* meta = scr_meta_new();
    if (scr_filemap_get_meta(map, file, meta) == SCR_SUCCESS) {
      /* if we need to flush this file, add it to the list and attach
       * its meta data */
      kvtree* file_hash = kvtree_set_kv(file_list, SCR_KEY_FILE, file);
      kvtree_set(file_hash, SCR_KEY_META, meta);
      meta = NULL;
    } else {
      /* TODO: print error */
      rc = SCR_FAILURE;
    }

    /* if we didn't attach the meta data, we need to delete it */
    if (meta != NULL) {
      scr_meta_delete(&meta);
    }
  }

  /* free map object */
  scr_filemap_delete(&map);

  if (! scr_alltrue(rc == SCR_SUCCESS, scr_comm_world)) {
    if (scr_my_rank_world == 0) {
      scr_err("Failed to create list of files and metadata for dataset %d @ %s:%d",
        id, __FILE__, __LINE__
      );
    }
    rc = SCR_FAILURE;
  }

  return rc;
}

/* write summary file for flush */
static int scr_flush_summary(
  const scr_dataset* dataset,
  const kvtree* file_list,
  int complete)
{
  int rc = SCR_SUCCESS;

  /* define path to metadata directory */
  char* dataset_path_str = scr_flush_dataset_metadir(dataset);
  spath* dataset_path = spath_from_str(dataset_path_str);
  spath_reduce(dataset_path);
  scr_free(&dataset_path_str);

  /* rank 0 creates summary file and writes dataset info */
  if (scr_my_rank_world == 0) {
    /* build file name to summary file */
    spath* summary_path = spath_dup(dataset_path);
    spath_append_str(summary_path, "summary.scr");
    char* summary_file = spath_strdup(summary_path);

    rc = scr_flush_summary_file(dataset, complete, summary_file);

    /* free the path and string of the summary file */
    scr_free(&summary_file);
    spath_delete(&summary_path);
  }

  /* free path and file name */
  spath_delete(&dataset_path);

  /* determine whether everyone wrote their files ok */
  if (scr_alltrue((rc == SCR_SUCCESS), scr_comm_world)) {
    return SCR_SUCCESS;
  }
  return SCR_FAILURE;
}

/* create entry in scr index file to indicate that a new dataset
 * has been started to be copied to the prefix directory, but mark
 * it as incomplete */
int scr_flush_init_index(scr_dataset* dataset)
{
  int rc = SCR_SUCCESS;

  /* update index file */
  if (scr_my_rank_world == 0) {
    /* read the index file */
    kvtree* index_hash = kvtree_new();
    scr_index_read(scr_prefix_path, index_hash);

    /* get id of dataset */
    int id;
    if (scr_dataset_get_id(dataset, &id) != SCR_SUCCESS) {
      scr_abort(-1, "Failed to read dataset id @ %s:%d",
        __FILE__, __LINE__
      );
    }

    /* get name of dataset */
    char* name;
    if (scr_dataset_get_name(dataset, &name) != SCR_SUCCESS) {
      scr_abort(-1, "Failed to read dataset name @ %s:%d",
        __FILE__, __LINE__
      );
    }

    /* clear any existing entry for this dataset */
    scr_index_remove(index_hash, name);

    /* update complete flag in index file */
    int complete = 0;
    scr_index_set_dataset(index_hash, id, name, dataset, complete);

    /* write the index file and delete the hash */
    scr_index_write(scr_prefix_path, index_hash);
    kvtree_delete(&index_hash);
  }

  /* have rank 0 broadcast whether the update succeeded */
  MPI_Bcast(&rc, 1, MPI_INT, 0, scr_comm_world);

  return rc;
}

/* given a dataset id that has been flushed and the list provided by scr_flush_prepare,
 * complete the flush by writing the summary file */
int scr_flush_complete(const scr_cache_index* cindex, int id, kvtree* file_list)
{
  int flushed = SCR_SUCCESS;

  /* to get this far, the dataset must be complete */
  int complete = 1;

  /* get the dataset of this flush */
  scr_dataset* dataset = kvtree_get(file_list, SCR_KEY_DATASET);

  /* write summary file */
  if (scr_flush_summary(dataset, file_list, complete) != SCR_SUCCESS) {
    flushed = SCR_FAILURE;
  }

  /* update index file */
  if (scr_my_rank_world == 0) {
    if (flushed == SCR_SUCCESS) {
      /* read the index file */
      kvtree* index_hash = kvtree_new();
      scr_index_read(scr_prefix_path, index_hash);

      /* get name of dataset */
      char* name;
      if (scr_dataset_get_name(dataset, &name) != SCR_SUCCESS) {
        scr_abort(-1, "Failed to read dataset name @ %s:%d",
          __FILE__, __LINE__
        );
      }

      /* clear any existing entry for this dataset */
      scr_index_remove(index_hash, name);

      /* update complete flag in index file */
      scr_index_set_dataset(index_hash, id, name, dataset, complete);

      /* record flushed tag */
      scr_index_mark_flushed(index_hash, id, name);

      /* remove any failed marker, since we may have flushed over
       * a previously failed dataset */
      scr_index_clear_failed(index_hash, id, name);

      /* if this is a checkpoint, update current to point to new dataset,
       * this must come after index_set_dataset above because set_current
       * checks that named dataset is a checkpoint */
      if (scr_dataset_is_ckpt(dataset)) {
        scr_index_set_current(index_hash, name);
      }

      /* write the index file and delete the hash */
      scr_index_write(scr_prefix_path, index_hash);
      kvtree_delete(&index_hash);
    }
  }

  /* have rank 0 broadcast whether the entire flush succeeded,
   * including summary file and index update */
  MPI_Bcast(&flushed, 1, MPI_INT, 0, scr_comm_world);

  /* mark this dataset as flushed to the parallel file system */
  if (flushed == SCR_SUCCESS) {
    scr_flush_file_location_set(id, SCR_FLUSH_KEY_LOCATION_PFS);

    /* if we just flushed a checkpoint,
     * delete others to maintain a sliding window */
    if (scr_prefix_size > 0) {
      int is_ckpt = scr_dataset_is_ckpt(dataset);
      if (is_ckpt) {
        scr_prefix_delete_sliding(id, scr_prefix_size);
      }
    }

    /* TODODSET: if this dataset is not a checkpoint, delete it from cache now */
#if 0
    if (! scr_dataset_is_ckpt(dataset)) {
      scr_cache_delete(map, id);
    }
#endif
  }

  return flushed;
}
