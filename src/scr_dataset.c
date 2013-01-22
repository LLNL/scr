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

/* Implements an interface to read/write SCR meta data files. */

#include "scr_err.h"
#include "scr_io.h"
#include "scr_dataset.h"
#include "scr_hash.h"

#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <strings.h>

#if 0
/* variable length args */
#include <stdarg.h>
#include <errno.h>

/* basename/dirname */
#include <unistd.h>
#include <libgen.h>

/* compute crc32 */
#include <zlib.h>
#endif

/*
=========================================
Allocate, delete, and copy functions
=========================================
*/

/* allocate a new dataset object */
scr_dataset* scr_dataset_new()
{
  scr_dataset* dataset = scr_hash_new();
  if (dataset == NULL) {
    scr_err("Failed to allocate dataset object @ %s:%d", __FILE__, __LINE__);
  }
  return dataset;
}

/* free memory assigned to dataset object */
int scr_dataset_delete(scr_dataset** ptr_dataset)
{
  int rc = scr_hash_delete(ptr_dataset);
  return rc;
}

/*
=========================================
Set field values
=========================================
*/

/* sets the id in dataset to be the value specified */
int scr_dataset_set_id(scr_dataset* dataset, int id)
{
  return scr_hash_util_set_int(dataset, SCR_DATASET_KEY_ID, id);
}

/* sets the username of the dataset */
int scr_dataset_set_username(scr_dataset* dataset, const char* name)
{
  return scr_hash_util_set_str(dataset, SCR_DATASET_KEY_USER, name);
}

/* sets the simulation name of the dataset */
int scr_dataset_set_jobname(scr_dataset* dataset, const char* name)
{
  return scr_hash_util_set_str(dataset, SCR_DATASET_KEY_JOBNAME, name);
}

/* sets the name of the dataset */
int scr_dataset_set_name(scr_dataset* dataset, const char* name)
{
  return scr_hash_util_set_str(dataset, SCR_DATASET_KEY_NAME, name);
}

/* sets the size of the dataset (in bytes) */
int scr_dataset_set_size(scr_dataset* dataset, unsigned long size)
{
  return scr_hash_util_set_bytecount(dataset, SCR_DATASET_KEY_SIZE, size);
}

/* sets the number of (logical) files in the dataset */
int scr_dataset_set_files(scr_dataset* dataset, int files)
{
  return scr_hash_util_set_int(dataset, SCR_DATASET_KEY_FILES, files);
}

/* sets the created timestamp for the dataset */
int scr_dataset_set_created(scr_dataset* dataset, int64_t usecs)
{
  return scr_hash_util_set_int64(dataset, SCR_DATASET_KEY_CREATED, usecs);
}

/* sets the jobid in which the dataset is created */
int scr_dataset_set_jobid(scr_dataset* dataset, const char* jobid)
{
  return scr_hash_util_set_str(dataset, SCR_DATASET_KEY_JOBID, jobid);
}

/* sets the cluster name on which the dataset was created */
int scr_dataset_set_cluster(scr_dataset* dataset, const char* name)
{
  return scr_hash_util_set_str(dataset, SCR_DATASET_KEY_CLUSTER, name);
}

/* sets the checkpoint id in dataset to be the value specified */
int scr_dataset_set_ckpt(scr_dataset* dataset, int id)
{
  return scr_hash_util_set_int(dataset, SCR_DATASET_KEY_CKPT, id);
}

/* sets the complete flag for the dataset to be the value specified */
int scr_dataset_set_complete(scr_dataset* dataset, int complete)
{
  return scr_hash_util_set_int(dataset, SCR_DATASET_KEY_COMPLETE, complete);
}

/*
=========================================
Get field values
=========================================
*/

/* gets id recorded in dataset, returns SCR_SUCCESS if successful */
int scr_dataset_get_id(const scr_dataset* dataset, int* id)
{
  return scr_hash_util_get_int(dataset, SCR_DATASET_KEY_ID, id);
}

/* gets username of dataset, returns SCR_SUCCESS if successful */
int scr_dataset_get_username(const scr_dataset* dataset, char** name)
{
  return scr_hash_util_get_str(dataset, SCR_DATASET_KEY_USER, name);
}

/* gets simulation name of dataset, returns SCR_SUCCESS if successful */
int scr_dataset_get_jobname(const scr_dataset* dataset, char** name)
{
  return scr_hash_util_get_str(dataset, SCR_DATASET_KEY_JOBNAME, name);
}

/* gets name of dataset, returns SCR_SUCCESS if successful */
int scr_dataset_get_name(const scr_dataset* dataset, char** name)
{
  return scr_hash_util_get_str(dataset, SCR_DATASET_KEY_NAME, name);
}

/* gets size of dataset (in bytes), returns SCR_SUCCESS if successful */
int scr_dataset_get_size(const scr_dataset* dataset, unsigned long* size)
{
  return scr_hash_util_get_bytecount(dataset, SCR_DATASET_KEY_SIZE, size);
}

/* gets number of (logical) files in dataset, returns SCR_SUCCESS if successful */
int scr_dataset_get_files(const scr_dataset* dataset, int* files)
{
  return scr_hash_util_get_int(dataset, SCR_DATASET_KEY_FILES, files);
}

/* gets created timestamp of dataset, returns SCR_SUCCESS if successful */
int scr_dataset_get_created(const scr_dataset* dataset, int64_t* usecs)
{
  return scr_hash_util_get_int64(dataset, SCR_DATASET_KEY_CREATED, usecs);
}

/* gets the jobid in which the dataset was created, returns SCR_SUCCESS if successful */
int scr_dataset_get_jobid(const scr_dataset* dataset, char** jobid)
{
  return scr_hash_util_get_str(dataset, SCR_DATASET_KEY_JOBID, jobid);
}

/* gets the cluster name on which the dataset was created, returns SCR_SUCCESS if successful */
int scr_dataset_get_cluster(const scr_dataset* dataset, char** name)
{
  return scr_hash_util_get_str(dataset, SCR_DATASET_KEY_CLUSTER, name);
}

/* gets checkpoint id recorded in dataset, returns SCR_SUCCESS if successful */
int scr_dataset_get_ckpt(const scr_dataset* dataset, int* id)
{
  return scr_hash_util_get_int(dataset, SCR_DATASET_KEY_CKPT, id);
}

/* gets complete flag recorded in dataset, returns SCR_SUCCESS if successful */
int scr_dataset_get_complete(const scr_dataset* dataset, int* complete)
{
  return scr_hash_util_get_int(dataset, SCR_DATASET_KEY_COMPLETE, complete);
}

/*
=========================================
Check field values
=========================================
*/
