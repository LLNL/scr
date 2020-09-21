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

/* Example contents of an index file:
 * This contains:
 *   - a VERSION number indicating the format of the file
 *   - an optional CURRENT that lists the current checkpoint
 *     a job should restart from
 *   - a map from NAME to DSET id, to lookup a dataset id given a name
 *   - a record for each dataset accessed by daatset id
 *     - contains FLUSHED, FAILED, FETCHED timestamps
 *     - contains COMPLETE marker indicating whether checkpoint is valid
 *     - then a full scr_dataset entry (see scr_dataset.h)
 *
 *  CURRENT
 *    ckpt.6
 *  VERSION
 *    1
 *  NAME
 *    ckpt.6
 *      DSET
 *        6
 *    ckpt.3
 *      DSET
 *        3
 *  DSET
 *    6
 *      FLUSHED
 *        2020-05-09T11:59:50
 *      COMPLETE
 *        1
 *      DSET
 *        ID
 *          6
 *        NAME
 *          ckpt.6
 *        FLAG_CKPT
 *          1
 *        FLAG_OUTPUT
 *          0
 *        CREATED
 *          1589050790160919
 *        USER
 *          user1
 *        JOBNAME
 *          testing_job
 *        JOBID
 *          5116040
 *        CKPT
 *          6
 *        FILES
 *          4
 *        SIZE
 *          2097186
 *        COMPLETE
 *          1
 *    3
 *      FLUSHED
 *        2020-05-09T11:59:50
 *      COMPLETE
 *        1
 *      DSET
 *        ID
 *          3
 *        NAME
 *          ckpt.3
 *        FLAG_CKPT
 *          1
 *        FLAG_OUTPUT
 *          0
 *        CREATED
 *          1589050789896295
 *        USER
 *          user1
 *        JOBNAME
 *          testing_job
 *        JOBID
 *          5116040
 *        CKPT
 *          3
 *        FILES
 *          4
 *        SIZE
 *          2097186
 *        COMPLETE
 *          1
 */

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
    kvtree* tmp = kvtree_new();
    int kvtree_rc = kvtree_read_file(index_file, tmp);
    rc = (kvtree_rc == KVTREE_SUCCESS) ? SCR_SUCCESS : SCR_FAILURE;

    /* version check on file */
    if (rc == SCR_SUCCESS) {
      /* read version value from file */
      int version;
      if (kvtree_util_get_int(tmp, SCR_INDEX_KEY_VERSION, &version) == KVTREE_SUCCESS) {
        /* got a version number, check that it's what we expect */
        if (version == SCR_INDEX_FILE_VERSION_2) {
          /* got the correct version, copy file contents into caller's kvtree */
          kvtree_merge(index, tmp);
        } else {
          /* failed to find the version number in the file */
          scr_err("Found file format version %d but expected %d in index file: %s @ %s:%d",
            version, SCR_INDEX_FILE_VERSION_2, index_file, __FILE__, __LINE__
          );
          rc = SCR_FAILURE;
        }
      } else {
        /* failed to find any version number in the file */
        scr_err("Failed to find file format version in index file: %s @ %s:%d",
          index_file, __FILE__, __LINE__
        );
        rc = SCR_FAILURE;
      }

      /* free our temporary tree */
      kvtree_delete(&tmp);
    }
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
    kvtree_util_set_int(index, SCR_INDEX_KEY_VERSION, SCR_INDEX_FILE_VERSION_2);
  }

  /* write out the file */
  int kvtree_rc = kvtree_write_path(path_index, index);
  int rc = (kvtree_rc == KVTREE_SUCCESS) ? SCR_SUCCESS : SCR_FAILURE;

  /* free path */
  spath_delete(&path_index);

  return rc;
}

/* read index file and return max dataset and checkpoint ids,
 *  * returns SCR_SUCCESS if file read successfully */
int scr_index_get_max_ids(const spath* dir, int* dset_id, int* ckpt_id, int* ckpt_dset_id)
{
  int rc = SCR_FAILURE;

  *dset_id      = 0;
  *ckpt_id      = 0;
  *ckpt_dset_id = 0;

  kvtree* index = kvtree_new();
  if (scr_index_read(dir, index) == SCR_SUCCESS) {
    /* search for the checkpoint with the maximum dataset id which is
     * complete and less than earlier_than if earlier_than is set */
    kvtree* dsets = kvtree_get(index, SCR_INDEX_1_KEY_DATASET);
    kvtree_elem* dset = NULL;
    for (dset = kvtree_elem_first(dsets);
         dset != NULL;
         dset = kvtree_elem_next(dset))
    {
      /* get the id for this dataset */
      char* key = kvtree_elem_key(dset);
      int current_id = atoi(key);
      if (current_id > *dset_id) {
        *dset_id = current_id;
      }

      /* check that dataset is really a checkpoint */
      kvtree* dset_hash = kvtree_elem_hash(dset);
      kvtree* dataset = kvtree_get(dset_hash, SCR_INDEX_1_KEY_DATASET);
      if (scr_dataset_is_ckpt(dataset)) {
        int current_ckpt_id;
        if (scr_dataset_get_ckpt(dataset, &current_ckpt_id) == SCR_SUCCESS) {
          if (current_ckpt_id > *ckpt_id) {
            *ckpt_id      = current_ckpt_id;
            *ckpt_dset_id = current_id;
          }
        }
      }
    }

    rc = SCR_SUCCESS;
  }

  kvtree_delete(&index);

  return rc;
}

/* this adds an entry to the index that maps a name to a dataset id */
static int scr_index_set_directory(kvtree* hash, const char* name, int id)
{
  /* add entry to directory index */
  kvtree* dir = kvtree_set_kv(hash, SCR_INDEX_1_KEY_NAME, name);
  kvtree_util_set_int(dir, SCR_INDEX_1_KEY_DATASET, id);
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

    /* delete the dataset id field */
    kvtree_unset_kv_int(index, SCR_INDEX_1_KEY_DATASET, id);

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

    return SCR_SUCCESS;
  } else {
    /* drop index from current if it matches */
    kvtree_unset_kv(index, SCR_INDEX_1_KEY_CURRENT, name);

    /* couldn't find the named dataset, return an error */
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
  kvtree* dataset = kvtree_get(dset_hash, SCR_INDEX_1_KEY_DATASET);
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
  kvtree_util_set_int(dset_hash, SCR_INDEX_1_KEY_COMPLETE, complete);

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

  /* get pointer to dataset hash */
  kvtree* dset_hash = kvtree_set_kv_int(index, SCR_INDEX_1_KEY_DATASET, id);

  /* record dataset hash in index */
  kvtree_set(dset_hash, SCR_INDEX_1_KEY_DATASET, dataset_copy);

  /* mark the dataset as complete or incomplete */
  kvtree_util_set_int(dset_hash, SCR_INDEX_1_KEY_COMPLETE, complete);

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
  kvtree_set_kv(dset_hash, SCR_INDEX_1_KEY_FETCHED, timestamp);

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
  kvtree_util_set_str(dset_hash, SCR_INDEX_1_KEY_FAILED, timestamp);

  /* add entry to directory index (maps name to dataset id) */
  scr_index_set_directory(index, name, id);

  return SCR_SUCCESS;
}

/* clear any failed fetch event for given dataset id and name in given hash */
int scr_index_clear_failed(kvtree* index, int id, const char* name)
{
  /* mark the dataset as failed at current timestamp */
  kvtree* dset_hash = kvtree_set_kv_int(index, SCR_INDEX_1_KEY_DATASET, id);
  kvtree_unset(dset_hash, SCR_INDEX_1_KEY_FAILED);

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
  kvtree_util_set_str(dset_hash, SCR_INDEX_1_KEY_FLUSHED, timestamp);

  /* add entry to directory index (maps name to dataset id) */
  scr_index_set_directory(index, name, id);

  return SCR_SUCCESS;
}

/* copy dataset into given dataset object,
 * returns SCR_FAILURE if not found */
int scr_index_get_dataset(kvtree* index, int id, const char* name, scr_dataset* dataset)
{
  int rc = SCR_FAILURE;

  /* get pointer to dataset hash */
  kvtree* dset_hash = kvtree_get_kv_int(index, SCR_INDEX_1_KEY_DATASET, id);

  /* lookup dataset hash in index */
  kvtree* dset = kvtree_get(dset_hash, SCR_INDEX_1_KEY_DATASET);
  if (dset != NULL) {
    /* found it, copy contents of dataset hash */
    kvtree_merge(dataset, dset);
    rc = SCR_SUCCESS;
  }

  return rc;
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
  if (kvtree_util_get_int(dset_hash, SCR_INDEX_1_KEY_COMPLETE, &complete_tmp) == KVTREE_SUCCESS) {
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
    if (key == NULL) {
      continue;
    }
    int current_id = atoi(key);

    /* if we have a limit and the current id is beyond that limit,
     * skip it */
    if (earlier_than != -1 && current_id >= earlier_than) {
      continue;
    }

    /* if the current id is earlier than the lastest we've found so far,
     * skip it */
    if (current_id < max_id) {
      continue;
    }

    kvtree* dset_hash = kvtree_elem_hash(dset);

    /* alright, this dataset id is within range to be the most recent,
     * now scan the various names we have for this dataset looking for a complete */
    int found_one = 1;

    /* this dataset id is less than our limit and it's more than
     * our current max, check whether it's complete */
    int complete;
    if (kvtree_util_get_int(dset_hash, SCR_INDEX_1_KEY_COMPLETE, &complete) == KVTREE_SUCCESS) {
      if (complete != 1) {
        /* COMPLETE marker explicitly shows it's bad */
        found_one = 0;
      }
    } else {
      /* no COMPLETE marker at all, assume it'd bad */
      found_one = 0;
    }

    /* check that there is no failed string */
    kvtree* failed = kvtree_get(dset_hash, SCR_INDEX_1_KEY_FAILED);
    if (failed != NULL) {
      /* some previous fetch of this checkpoint failed,
       * don't try it again */
      found_one = 0;
    }

    /* TODO: also avoid dataset if we've tried to read it too many times */

    /* check that dataset is really a checkpoint */
    kvtree* dataset_hash = kvtree_get(dset_hash, SCR_INDEX_1_KEY_DATASET);
    if (! scr_dataset_is_ckpt(dataset_hash)) {
      /* data set is not a checkpoint */
      found_one = 0;
    }

    /* get the name of the dataset */
    char* current_name;
    scr_dataset_get_name(dataset_hash, &current_name);

    /* if we found one, copy the dataset id and name, and update our max */
    if (found_one) {
      *id = current_id;
      strcpy(name, current_name);

      /* update our max */
      max_id = current_id;
    }
  }

  return SCR_FAILURE;
}

/* lookup the dataset having the lowest id, return its id and name,
 * sets id to -1 to indicate no dataset is left */
int scr_index_get_oldest(const kvtree* index, int* id, char* name)
{
  /* assume that we won't find a valid dataset */
  *id = -1;

  /* search for the checkpoint with the minimum dataset id */
  int min_id = -1;
  kvtree* dsets = kvtree_get(index, SCR_INDEX_1_KEY_DATASET);
  kvtree_elem* dset = NULL;
  for (dset = kvtree_elem_first(dsets);
       dset != NULL;
       dset = kvtree_elem_next(dset))
  {
    /* get the id for this dataset */
    char* key = kvtree_elem_key(dset);
    if (key == NULL) {
      continue;
    }
    int current_id = atoi(key);

    /* if the current id is more recent than the oldest we've found so far,
     * skip it */
    if (min_id != -1 && current_id > min_id) {
      continue;
    }

    /* get dataset info */
    kvtree* dset_hash = kvtree_elem_hash(dset);
    kvtree* dataset_hash = kvtree_get(dset_hash, SCR_INDEX_1_KEY_DATASET);

    /* get the name of the dataset */
    char* current_name;
    scr_dataset_get_name(dataset_hash, &current_name);

    /* if we found one, copy the dataset id and name, and update our max */
    *id = current_id;
    strcpy(name, current_name);

    /* update our min */
    min_id = current_id;
  }

  return SCR_FAILURE;
}

int scr_index_remove_later(kvtree* index, int target_id)
{
  int rc = SCR_SUCCESS;

  /* search for the checkpoint with the maximum dataset id which is
   * complete and less than earlier_than if earlier_than is set */
  kvtree* dsets = kvtree_get(index, SCR_INDEX_1_KEY_DATASET);

  /* get list of dataset ids in index */
  int count = 0;
  int* ids = NULL;
  kvtree_list_int(dsets, &count, &ids);

  int i;
  for (i = 0; i < count; i++) {
    /* get id for this dataset */
    int id = ids[i];
    if (id <= target_id) {
      /* this dataset is earlier, throw it back */
      continue;
    }

    /* got a dataset that is later than the target,
     * get the dataset hash for this dataset */
    kvtree* dset_hash = kvtree_get_kv_int(index, SCR_INDEX_1_KEY_DATASET, id);
    kvtree* dataset_hash = kvtree_get(dset_hash, SCR_INDEX_1_KEY_DATASET);

#if 0
    /* don't delete output datasets */
    int is_output = scr_dataset_is_output(dataset_hash);
    if (is_output) {
      continue;
    }
#endif

    /* get the name of the dataset */
    char* name;
    scr_dataset_get_name(dataset_hash, &name);

    /* delete it from the index */
    scr_index_remove(index, name);
  }

  /* free list of dataset ids */
  scr_free(&ids);

  return rc;
}
