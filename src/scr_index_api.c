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

/* Implements an interface to read and write index files. */

#include "scr_globals.h"
#include "scr.h"
#include "scr_io.h"
#include "scr_err.h"
#include "scr_util.h"
#include "spath.h"
#include "kvtree.h"
#include "kvtree_util.h"
#include "scr_index_api.h"
#include "scr_dataset.h"

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/file.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>

/* strdup */
#include <string.h>

/* strftime */
#include <time.h>

/* basename/dirname */
#include <unistd.h>
#include <libgen.h>

#define SCR_INDEX_FILENAME "index.scr"

/* read the index file from given directory and merge its contents into the given hash */
int scr_index_read(const spath* dir, kvtree* index)
{
  int rc = SCR_FAILURE;

  /* build the file name for the index file */
  spath* path_index = spath_dup(dir);
  spath_append_str(path_index, ".scr");
  spath_append_str(path_index, SCR_INDEX_FILENAME);
  char* index_file = spath_strdup(path_index);

  /* if we can access it, read the index file */
  if (scr_file_exists(index_file) == SCR_SUCCESS) {
    int kvtree_rc = kvtree_read_file(index_file, index);
    rc = (kvtree_rc == KVTREE_SUCCESS) ? SCR_SUCCESS : SCR_FAILURE;
  }

  /* free path and string */
  scr_free(&index_file);
  spath_delete(&path_index);

  return rc;
}

/* overwrite the contents of the index file in given directory with given hash */
int scr_index_write(const spath* dir, kvtree* index)
{
  /* build the file name for the index file */
  spath* path_index = spath_dup(dir);
  spath_append_str(path_index, ".scr");
  spath_append_str(path_index, SCR_INDEX_FILENAME);

  /* set the index file version key if it's not set already */
  kvtree* version = kvtree_get(index, SCR_INDEX_KEY_VERSION);
  if (version == NULL) {
    kvtree_util_set_int(index, SCR_INDEX_KEY_VERSION, SCR_INDEX_FILE_VERSION_1);
  }

  /* write out the file */
  int kvtree_rc = kvtree_write_path(path_index, index);
  int rc = (kvtree_rc == KVTREE_SUCCESS) ? SCR_SUCCESS : SCR_FAILURE;

  /* free path */
  spath_delete(&path_index);

  return rc;
}

/* this adds an entry to the index that maps a name to a dataset id */
static int scr_index_set_directory(kvtree* hash, const char* name, int id)
{
  /* add entry to directory index */
  kvtree* dir  = kvtree_set_kv(hash, SCR_INDEX_1_KEY_NAME, name);
  kvtree* dset = kvtree_set_kv_int(dir, SCR_INDEX_1_KEY_DATASET, id);
  if (dset == NULL) {
    return SCR_FAILURE;
  }
  return SCR_SUCCESS;
}

/* add given dataset id and name to given hash */
int scr_index_add_name(kvtree* index, int id, const char* name)
{
  /* set the dataset id */
  kvtree* dset_hash = kvtree_set_kv_int(index, SCR_INDEX_1_KEY_DATASET, id);

  /* unset then set name so we overwrite it if it's already set */
  kvtree_unset_kv(dset_hash, SCR_INDEX_1_KEY_NAME, name);
  kvtree_set_kv(dset_hash, SCR_INDEX_1_KEY_NAME, name);

  /* add entry to directory index (maps name to dataset id) */
  scr_index_set_directory(index, name, id);

  return SCR_SUCCESS;
}

/* remove given dataset name from hash */
int scr_index_remove(kvtree* index, const char* name)
{
  /* lookup the dataset id based on the dataset name */
  int id;
  if (scr_index_get_id_by_name(index, name, &id) == SCR_SUCCESS) {
    /* delete dataset name from the name-to-dataset-id index */
    kvtree_unset_kv(index, SCR_INDEX_1_KEY_NAME, name);

    /* get the hash for this dataset id */
    kvtree* dset = kvtree_get_kv_int(index, SCR_INDEX_1_KEY_DATASET, id);

    /* delete this dataset name from the hash for this dataset id */
    kvtree_unset_kv(dset, SCR_INDEX_1_KEY_NAME, name);

    /* if that was the only entry for this dataset id,
     * also delete the dataset id field */
    if (kvtree_size(dset) == 0) {
      kvtree_unset_kv_int(index, SCR_INDEX_1_KEY_DATASET, id);
    }

    /* if this is the current dataset, update current */
    char* current = NULL;
    if (scr_index_get_current(index, &current) == SCR_SUCCESS) {
      if (strcmp(current, name) == 0) {
        /* name is current, remove it */
        scr_index_unset_current(index);

        /* replace current with next most recent */
        int newid;
        char newname[SCR_MAX_FILENAME];
        scr_index_get_most_recent_complete(index, id, &newid, newname);
        if (newid != -1) {
          /* found a new dataset name, update current */
          scr_index_set_current(index, newname);
        }
      }
    }

    /* write out the new index file */
    return SCR_SUCCESS;
  } else {
    /* drop index from current if it matches */
    kvtree_unset_kv(index, SCR_INDEX_1_KEY_CURRENT, name);

    /* couldn't find the named dataset, print an error */
    scr_err("Named dataset was not found in index file: %s @ %s:%d",
      name, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }
}

/* set dataset name as current to restart from */
int scr_index_set_current(kvtree* index, const char* name)
{
  /* lookup the dataset id based on the dataset name */
  int id;
  if (scr_index_get_id_by_name(index, name, &id) != SCR_SUCCESS) {
    /* failed to find dataset by this name */
    return SCR_FAILURE;
  }

  /* check that dataset is a checkpoint */
  kvtree* dset_hash = kvtree_get_kv_int(index, SCR_INDEX_1_KEY_DATASET, id);
  kvtree* name_hash = kvtree_get_kv(dset_hash, SCR_INDEX_1_KEY_NAME, name);
  kvtree* dataset = kvtree_get(name_hash, SCR_INDEX_1_KEY_DATASET);
  if (! scr_dataset_is_ckpt(dataset)) {
    return SCR_FAILURE;
  }

  /* set the current dataset */
  kvtree_util_set_str(index, SCR_INDEX_1_KEY_CURRENT, name);

  return SCR_SUCCESS;
}

/* get dataset name as current to restart from */
int scr_index_get_current(kvtree* index, char** name)
{
  /* get the current dataset */
  int kvtree_rc = kvtree_util_get_str(index, SCR_INDEX_1_KEY_CURRENT, name);
  int rc = (kvtree_rc == KVTREE_SUCCESS) ? SCR_SUCCESS : SCR_FAILURE;
  return rc;
}

/* unset dataset name as current to restart from */
int scr_index_unset_current(kvtree* index)
{
  /* unset the current dataset */
  int kvtree_rc = kvtree_unset(index, SCR_INDEX_1_KEY_CURRENT);
  int rc = (kvtree_rc == KVTREE_SUCCESS) ? SCR_SUCCESS : SCR_FAILURE;
  return rc;
}

/* write completeness code (0 or 1) for given dataset id and name in given hash */
int scr_index_set_complete(kvtree* index, int id, const char* name, int complete)
{
  /* mark the dataset as complete or incomplete */
  kvtree* dset_hash = kvtree_set_kv_int(index, SCR_INDEX_1_KEY_DATASET, id);
  kvtree* dir_hash  = kvtree_set_kv(dset_hash, SCR_INDEX_1_KEY_NAME, name);
  kvtree_util_set_int(dir_hash, SCR_INDEX_1_KEY_COMPLETE, complete);

  /* add entry to directory index (maps name to dataset id) */
  scr_index_set_directory(index, name, id);

  return SCR_SUCCESS;
}

/* write completeness code (0 or 1) for given dataset id and name in given hash */
int scr_index_set_dataset(kvtree* index, int id, const char* name, const scr_dataset* dataset, int complete)
{
  /* copy contents of dataset hash */
  kvtree* dataset_copy = kvtree_new();
  kvtree_merge(dataset_copy, dataset);

  /* get pointer to name hash */
  kvtree* dset_hash = kvtree_set_kv_int(index, SCR_INDEX_1_KEY_DATASET, id);
  kvtree* dir_hash  = kvtree_set_kv(dset_hash, SCR_INDEX_1_KEY_NAME, name);

  /* record dataset hash in index */
  kvtree_set(dir_hash, SCR_INDEX_1_KEY_DATASET, dataset_copy);

  /* mark the dataset as complete or incomplete */
  kvtree_util_set_int(dir_hash, SCR_INDEX_1_KEY_COMPLETE, complete);

  /* add entry to directory index (maps name to dataset id) */
  scr_index_set_directory(index, name, id);

  return SCR_SUCCESS;
}

/* record fetch event for given dataset id and name in given hash */
int scr_index_mark_fetched(kvtree* index, int id, const char* name)
{
  /* format timestamp */
  time_t now = time(NULL);
  char timestamp[30];
  strftime(timestamp, sizeof(timestamp), "%Y-%m-%dT%H:%M:%S", localtime(&now));

  /* NOTE: we use set_kv instead of util_set_str so that multiple fetch
   * timestamps can be recorded */
  /* mark the dataset as fetched at current timestamp */
  kvtree* dset_hash = kvtree_set_kv_int(index, SCR_INDEX_1_KEY_DATASET, id);
  kvtree* dir_hash  = kvtree_set_kv(dset_hash, SCR_INDEX_1_KEY_NAME, name);
  kvtree_set_kv(dir_hash, SCR_INDEX_1_KEY_FETCHED, timestamp);

  /* add entry to directory index (maps name to dataset id) */
  scr_index_set_directory(index, name, id);

  return SCR_SUCCESS;
}

/* record failed fetch event for given dataset id and name in given hash */
int scr_index_mark_failed(kvtree* index, int id, const char* name)
{
  /* format timestamp */
  time_t now = time(NULL);
  char timestamp[30];
  strftime(timestamp, sizeof(timestamp), "%Y-%m-%dT%H:%M:%S", localtime(&now));

  /* mark the dataset as failed at current timestamp */
  kvtree* dset_hash = kvtree_set_kv_int(index, SCR_INDEX_1_KEY_DATASET, id);
  kvtree* dir_hash  = kvtree_set_kv(dset_hash, SCR_INDEX_1_KEY_NAME, name);
  kvtree_util_set_str(dir_hash, SCR_INDEX_1_KEY_FAILED, timestamp);

  /* add entry to directory index (maps name to dataset id) */
  scr_index_set_directory(index, name, id);

  return SCR_SUCCESS;
}

/* record flush event for given dataset id and name in given hash */
int scr_index_mark_flushed(kvtree* index, int id, const char* name)
{
  /* format timestamp */
  time_t now = time(NULL);
  char timestamp[30];
  strftime(timestamp, sizeof(timestamp), "%Y-%m-%dT%H:%M:%S", localtime(&now));

  /* mark the dataset as flushed at current timestamp */
  kvtree* dset_hash = kvtree_set_kv_int(index, SCR_INDEX_1_KEY_DATASET, id);
  kvtree* dir_hash  = kvtree_set_kv(dset_hash, SCR_INDEX_1_KEY_NAME, name);
  kvtree_util_set_str(dir_hash, SCR_INDEX_1_KEY_FLUSHED, timestamp);

  /* add entry to directory index (maps name to dataset id) */
  scr_index_set_directory(index, name, id);

  return SCR_SUCCESS;
}

/* get completeness code for given dataset id and name in given hash,
 * sets complete=0 and returns SCR_FAILURE if key is not set */
int scr_index_get_complete(kvtree* index, int id, const char* name, int* complete)
{
  int rc = SCR_FAILURE;
  *complete = 0;

  /* get the value of the COMPLETE key */
  int complete_tmp;
  kvtree* dset_hash = kvtree_get_kv_int(index, SCR_INDEX_1_KEY_DATASET, id);
  kvtree* dir_hash  = kvtree_get_kv(dset_hash, SCR_INDEX_1_KEY_NAME, name);
  if (kvtree_util_get_int(dir_hash, SCR_INDEX_1_KEY_COMPLETE, &complete_tmp) == KVTREE_SUCCESS) {
    *complete = complete_tmp;
    rc = SCR_SUCCESS;
  }

  return rc;
}

/* lookup the dataset id corresponding to the given dataset name in given hash
 * (assumes a name maps to a single dataset id) */
int scr_index_get_id_by_name(const kvtree* index, const char* name, int* id)
{
  /* assume that we won't find this dataset */
  *id = -1;

  /* attempt to lookup the dataset id */
  kvtree* dir_hash = kvtree_get_kv(index, SCR_INDEX_1_KEY_NAME, name);
  int id_tmp;
  if (kvtree_util_get_int(dir_hash, SCR_INDEX_1_KEY_DATASET, &id_tmp) == KVTREE_SUCCESS) {
    /* found it, convert the string to an int and return */
    *id = id_tmp;
    return SCR_SUCCESS;
  }

  /* couldn't find it, return failure */
  return SCR_FAILURE;
}

/* lookup the most recent complete dataset id and name whose id is less than earlier_than
 * setting earlier_than = -1 disables this filter */
int scr_index_get_most_recent_complete(const kvtree* index, int earlier_than, int* id, char* name)
{
  /* assume that we won't find a valid dataset */
  *id = -1;

  /* search for the checkpoint with the maximum dataset id which is
   * complete and less than earlier_than if earlier_than is set */
  int max_id = -1;
  kvtree* dsets = kvtree_get(index, SCR_INDEX_1_KEY_DATASET);
  kvtree_elem* dset = NULL;
  for (dset = kvtree_elem_first(dsets);
       dset != NULL;
       dset = kvtree_elem_next(dset))
  {
    /* get the id for this dataset */
    char* key = kvtree_elem_key(dset);
    if (key != NULL) {
      /* if this dataset id is less than our limit and it's more than
       * our current max, check whether it's complete */
      int current_id = atoi(key);
      if ((earlier_than == -1 || current_id <= earlier_than) && current_id > max_id) {
        /* alright, this dataset id is within range to be the most recent,
         * now scan the various names we have for this dataset looking for a complete */
        kvtree* dset_hash = kvtree_elem_hash(dset);
        kvtree* names = kvtree_get(dset_hash, SCR_INDEX_1_KEY_NAME);
        kvtree_elem* elem = NULL;
        for (elem = kvtree_elem_first(names);
             elem != NULL;
             elem = kvtree_elem_next(elem))
        {
          char* name_key = kvtree_elem_key(elem);
          kvtree* name_hash = kvtree_elem_hash(elem);

          int found_one = 1;

          /* look for the complete string */
          int complete;
          if (kvtree_util_get_int(name_hash, SCR_INDEX_1_KEY_COMPLETE, &complete) == KVTREE_SUCCESS) {
            if (complete != 1) {
              found_one = 0;
            }
          } else {
            found_one = 0;
          }

          /* check that there is no failed string */
          kvtree* failed = kvtree_get(name_hash, SCR_INDEX_1_KEY_FAILED);
          if (failed != NULL) {
            found_one = 0;
          }

          /* TODO: also avoid dataset if we've tried to read it too many times */

          /* check that dataset is really a checkpoint */
          kvtree* dataset_hash = kvtree_get(name_hash, SCR_INDEX_1_KEY_DATASET);
          if (! scr_dataset_is_ckpt(dataset_hash)) {
            /* data set is not a checkpoint */
            found_one = 0;
          }

          /* if we found one, copy the dataset id and name, and update our max */
          if (found_one) {
            *id = current_id;
            strcpy(name, name_key);

            /* update our max */
            max_id = current_id;
            break;
          }
        }
      }
    }

  }

  return SCR_FAILURE;
}
