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

#include "scr.h"
#include "scr_io.h"
#include "scr_err.h"
#include "scr_hash.h"
#include "scr_index_api.h"

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
#define SCR_INDEX_FILE_VERSION_1 (1)

/* this index is used to lookup a checkpoint id given a hash and checkpoint directory name */
static int scr_index_set_directory(scr_hash* hash, const char* name, int checkpoint_id)
{
  /* add entry to directory index */
  scr_hash* dir  = scr_hash_set_kv(hash, SCR_INDEX_KEY_DIR, name);
  scr_hash* ckpt = scr_hash_set_kv_int(dir, SCR_INDEX_KEY_CKPT, checkpoint_id);
  if (ckpt == NULL) {
    return SCR_FAILURE;
  }
  return SCR_SUCCESS;
}

/* read the index file from given directory and merge its contents into the given hash */
int scr_index_read(const char* dir, scr_hash* index)
{
  /* build the file name for the index file */
  char index_file[SCR_MAX_FILENAME];
  if (scr_build_path(index_file, sizeof(index_file), dir, SCR_INDEX_FILENAME) != SCR_SUCCESS) {
    scr_err("Failed to build filename to read index file in dir %s @ %s:%d",
            dir, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* if we can access it, read the index file */
  if (scr_file_exists(index_file) == SCR_SUCCESS) {
    int rc = scr_hash_read(index_file, index);
    return rc;
  } else {
    return SCR_FAILURE;
  }
}

/* overwrite the contents of the index file in given directory with given hash */
int scr_index_write(const char* dir, scr_hash* index)
{
  /* build the file name for the index file */
  char index_file[SCR_MAX_FILENAME];
  if (scr_build_path(index_file, sizeof(index_file), dir, SCR_INDEX_FILENAME) != SCR_SUCCESS) {
    scr_err("Failed to build filename to write index file in dir %s @ %s:%d",
            dir, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* set the index file version key if it's not set already */
  scr_hash* version = scr_hash_get(index, SCR_INDEX_KEY_VERSION);
  if (version == NULL) {
    scr_hash_set_kv_int(index, SCR_INDEX_KEY_VERSION, SCR_INDEX_FILE_VERSION_1);
  }

  /* write out the file */
  int rc = scr_hash_write(index_file, index);
  return rc;
}

/* add given checkpoint id and directory name to given hash */
int scr_index_add_checkpoint_dir(scr_hash* index, int checkpoint_id, const char* name)
{
  /* set the checkpoint directory */
  scr_hash* ckpt1 = scr_hash_set_kv_int(index, SCR_INDEX_KEY_CKPT, checkpoint_id);
  scr_hash* dir1  = scr_hash_set_kv(ckpt1, SCR_INDEX_KEY_DIR, name);

  /* add entry to directory index */
  scr_index_set_directory(index, name, checkpoint_id);

  return SCR_SUCCESS;
}

/* write completeness code (0 or 1) for given checkpoint id and directory in given hash */
int scr_index_set_complete_key(scr_hash* index, int checkpoint_id, const char* name, int complete)
{
  /* mark the checkpoint as complete or incomplete */
  scr_hash* ckpt1 = scr_hash_set_kv_int(index, SCR_INDEX_KEY_CKPT, checkpoint_id);
  scr_hash* dir1  = scr_hash_set_kv(ckpt1, SCR_INDEX_KEY_DIR, name);
  scr_hash_set_kv_int(dir1, SCR_INDEX_KEY_COMPLETE, complete);

  /* add entry to directory index */
  scr_index_set_directory(index, name, checkpoint_id);

  return SCR_SUCCESS;
}

/* get completeness code for given checkpoint id and directory in given hash,
 * sets complete=0 and returns SCR_FAILURE if key is not set */
int scr_index_get_complete_key(scr_hash* index, int checkpoint_id, const char* name, int* complete)
{
  int rc = SCR_FAILURE;
  *complete = 0;

  /* get the value of the COMPLETE key */
  scr_hash* ckpt = scr_hash_get_kv_int(index, SCR_INDEX_KEY_CKPT, checkpoint_id);
  scr_hash* dir  = scr_hash_get_kv(ckpt, SCR_INDEX_KEY_DIR, name);
  scr_hash* comp = scr_hash_get(dir, SCR_INDEX_KEY_COMPLETE);
  int size = scr_hash_size(comp);
  if (size == 1) {
    char* complete_str = scr_hash_elem_get_first_val(dir, SCR_INDEX_KEY_COMPLETE);
    if (complete_str != NULL) {
      *complete = atoi(complete_str);
      rc = SCR_SUCCESS;
    }
  }

  return rc;
}

/* record fetch event for given checkpoint id and directory in given hash */
int scr_index_mark_fetched(scr_hash* index, int checkpoint_id, const char* name)
{
  /* format timestamp */
  time_t now = time(NULL);
  char timestamp[30];
  strftime(timestamp, sizeof(timestamp), "%Y-%m-%dT%H:%M:%S", localtime(&now));

  /* mark the checkpoint as fetched at current timestamp */
  scr_hash* ckpt1 = scr_hash_set_kv_int(index, SCR_INDEX_KEY_CKPT, checkpoint_id);
  scr_hash* dir1  = scr_hash_set_kv(ckpt1, SCR_INDEX_KEY_DIR, name);
  scr_hash_set_kv(dir1, SCR_INDEX_KEY_FETCHED, timestamp);

  /* add entry to directory index */
  scr_index_set_directory(index, name, checkpoint_id);

  return SCR_SUCCESS;
}

/* record failed fetch event for given checkpoint id and directory in given hash */
int scr_index_mark_failed(scr_hash* index, int checkpoint_id, const char* name)
{
  /* format timestamp */
  time_t now = time(NULL);
  char timestamp[30];
  strftime(timestamp, sizeof(timestamp), "%Y-%m-%dT%H:%M:%S", localtime(&now));

  /* mark the checkpoint as failed at current timestamp */
  scr_hash* ckpt1 = scr_hash_set_kv_int(index, SCR_INDEX_KEY_CKPT, checkpoint_id);
  scr_hash* dir1  = scr_hash_set_kv(ckpt1, SCR_INDEX_KEY_DIR, name);
  scr_hash_set_kv(dir1, SCR_INDEX_KEY_FAILED, timestamp);

  /* add entry to directory index */
  scr_index_set_directory(index, name, checkpoint_id);

  return SCR_SUCCESS;
}

/* record flush event for given checkpoint id and directory in given hash */
int scr_index_mark_flushed(scr_hash* index, int checkpoint_id, const char* name)
{
  /* format timestamp */
  time_t now = time(NULL);
  char timestamp[30];
  strftime(timestamp, sizeof(timestamp), "%Y-%m-%dT%H:%M:%S", localtime(&now));

  /* mark the checkpoint as flushed at current timestamp */
  scr_hash* ckpt1 = scr_hash_set_kv_int(index, SCR_INDEX_KEY_CKPT, checkpoint_id);
  scr_hash* dir1  = scr_hash_set_kv(ckpt1, SCR_INDEX_KEY_DIR, name);
  scr_hash_set_kv(dir1, SCR_INDEX_KEY_FLUSHED, timestamp);

  /* add entry to directory index */
  scr_index_set_directory(index, name, checkpoint_id);

  return SCR_SUCCESS;
}

/* lookup the checkpoint id corresponding to the given checkpoint directory name in given hash
 * (assumes a directory maps to a single checkpoint id) */
int scr_index_get_checkpoint_id_by_dir(const scr_hash* index, const char* name, int* checkpoint_id)
{
  /* assume that we won't find this checkpoint */
  *checkpoint_id = -1;

  /* attempt to lookup the checkpoint id */
  scr_hash* dir = scr_hash_get_kv(index, SCR_INDEX_KEY_DIR, name);
  char* checkpoint_id_str = scr_hash_elem_get_first_val(dir, SCR_INDEX_KEY_CKPT);
  if (checkpoint_id_str != NULL) {
    /* found it, convert the string to an int and return */
    *checkpoint_id = atoi(checkpoint_id_str);
    return SCR_SUCCESS;
  }

  /* couldn't find it, return failure */
  return SCR_FAILURE;
}

/* lookup the most recent complete checkpoint id and directory whose id is less than earlier_than
 * setting earlier_than = -1 disables this filter */
int scr_index_most_recent_complete(const scr_hash* index, int earlier_than, int* checkpoint_id, char* name)
{
  /* assume that we won't find this checkpoint */
  *checkpoint_id = -1;

  /* search for the maximum checkpoint id which is complete and less than earlier_than
   * if earlier_than is set */
  int max_id = -1;
  scr_hash* ckpts = scr_hash_get(index, SCR_INDEX_KEY_CKPT);
  scr_hash_elem* ckpt = NULL;
  for (ckpt = scr_hash_elem_first(ckpts);
       ckpt != NULL;
       ckpt = scr_hash_elem_next(ckpt))
  {
    /* get the id for this checkpoint */
    char* key = scr_hash_elem_key(ckpt);
    if (key != NULL) {
      int id = atoi(key);
      /* if this checkpoint id is less than our limit and it's more than
       * our current max, check whether it's complete */
      if ((earlier_than == -1 || id <= earlier_than) && id > max_id) {
        /* alright, this checkpoint id is within range to be the most recent,
         * now scan the various dirs we have for this checkpoint looking for a complete */
        scr_hash* ckpt_hash = scr_hash_elem_hash(ckpt);
        scr_hash* dirs = scr_hash_get(ckpt_hash, SCR_INDEX_KEY_DIR);
        scr_hash_elem* dir = NULL;
        for (dir = scr_hash_elem_first(dirs);
             dir != NULL;
             dir = scr_hash_elem_next(dir))
        {
          char* dir_key = scr_hash_elem_key(dir);
          scr_hash* dir_hash = scr_hash_elem_hash(dir);

          int found_one = 1;

          /* look for the complete string */
          char* complete_str = scr_hash_elem_get_first_val(dir_hash, SCR_INDEX_KEY_COMPLETE);
          if (complete_str != NULL) {
            int complete = atoi(complete_str);
            if (complete != 1) {
              found_one = 0;
            }
          } else {
            found_one = 0;
          }

          /* check that there is no failed string */
          scr_hash* failed = scr_hash_get(dir_hash, SCR_INDEX_KEY_FAILED);
          if (failed != NULL) {
            found_one = 0;
          }

          /* TODO: also avoid checkpoint if we've tried to read it too many times */

          /* if we found one, copy the checkpoint id and directory name, and update our max */
          if (found_one) {
            *checkpoint_id = id;
            strcpy(name, dir_key);

            /* update our max */
            max_id = id;
            break;
          }
        }
      }
    }

  }

  return SCR_FAILURE;
}
