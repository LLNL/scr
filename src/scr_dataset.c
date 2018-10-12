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

#include "scr_globals.h"

#include "scr_err.h"
#include "scr_io.h"
#include "scr_dataset.h"

#include "kvtree.h"
#include "kvtree_util.h"

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

#define SCR_DATASET_KEY_ID       ("ID")
#define SCR_DATASET_KEY_USER     ("USER")
#define SCR_DATASET_KEY_JOBNAME  ("JOBNAME")
#define SCR_DATASET_KEY_NAME     ("NAME")
#define SCR_DATASET_KEY_SIZE     ("SIZE")
#define SCR_DATASET_KEY_FILES    ("FILES")
#define SCR_DATASET_KEY_CREATED  ("CREATED")
#define SCR_DATASET_KEY_JOBID    ("JOBID")
#define SCR_DATASET_KEY_CLUSTER  ("CLUSTER")
#define SCR_DATASET_KEY_CKPT     ("CKPT")
#define SCR_DATASET_KEY_COMPLETE ("COMPLETE")
#define SCR_DATASET_KEY_FLAG_CKPT   ("FLAG_CKPT")
#define SCR_DATASET_KEY_FLAG_OUTPUT ("FLAG_OUTPUT")

static int convert_kvtree_rc(int kvtree_rc)
{
  int rc = (kvtree_rc == KVTREE_SUCCESS) ? SCR_SUCCESS : SCR_FAILURE;
  return rc;
}

/*
=========================================
Allocate, delete, and copy functions
=========================================
*/

/* allocate a new dataset object */
scr_dataset* scr_dataset_new()
{
  scr_dataset* dataset = kvtree_new();
  if (dataset == NULL) {
    scr_err("Failed to allocate dataset object @ %s:%d", __FILE__, __LINE__);
  }
  return dataset;
}

/* free memory assigned to dataset object */
int scr_dataset_delete(scr_dataset** ptr_dataset)
{
  int kvtree_rc = kvtree_delete(ptr_dataset);
  return convert_kvtree_rc(kvtree_rc);
}

/*
=========================================
Set field values
=========================================
*/

/* sets the id in dataset to be the value specified */
int scr_dataset_set_id(scr_dataset* dataset, int id)
{
  int kvtree_rc = kvtree_util_set_int(dataset, SCR_DATASET_KEY_ID, id);
  return convert_kvtree_rc(kvtree_rc);
}

/* sets the username of the dataset */
int scr_dataset_set_username(scr_dataset* dataset, const char* name)
{
  int kvtree_rc = kvtree_util_set_str(dataset, SCR_DATASET_KEY_USER, name);
  return convert_kvtree_rc(kvtree_rc);
}

/* sets the simulation name of the dataset */
int scr_dataset_set_jobname(scr_dataset* dataset, const char* name)
{
  int kvtree_rc = kvtree_util_set_str(dataset, SCR_DATASET_KEY_JOBNAME, name);
  return convert_kvtree_rc(kvtree_rc);
}

/* sets the name of the dataset */
int scr_dataset_set_name(scr_dataset* dataset, const char* name)
{
  int kvtree_rc = kvtree_util_set_str(dataset, SCR_DATASET_KEY_NAME, name);
  return convert_kvtree_rc(kvtree_rc);
}

/* sets the size of the dataset (in bytes) */
int scr_dataset_set_size(scr_dataset* dataset, unsigned long size)
{
  int kvtree_rc = kvtree_util_set_bytecount(dataset, SCR_DATASET_KEY_SIZE, size);
  return convert_kvtree_rc(kvtree_rc);
}

/* sets the number of (logical) files in the dataset */
int scr_dataset_set_files(scr_dataset* dataset, int files)
{
  int kvtree_rc = kvtree_util_set_int(dataset, SCR_DATASET_KEY_FILES, files);
  return convert_kvtree_rc(kvtree_rc);
}

/* sets the created timestamp for the dataset */
int scr_dataset_set_created(scr_dataset* dataset, int64_t usecs)
{
  int kvtree_rc = kvtree_util_set_int64(dataset, SCR_DATASET_KEY_CREATED, usecs);
  return convert_kvtree_rc(kvtree_rc);
}

/* sets the jobid in which the dataset is created */
int scr_dataset_set_jobid(scr_dataset* dataset, const char* jobid)
{
  int kvtree_rc = kvtree_util_set_str(dataset, SCR_DATASET_KEY_JOBID, jobid);
  return convert_kvtree_rc(kvtree_rc);
}

/* sets the cluster name on which the dataset was created */
int scr_dataset_set_cluster(scr_dataset* dataset, const char* name)
{
  int kvtree_rc = kvtree_util_set_str(dataset, SCR_DATASET_KEY_CLUSTER, name);
  return convert_kvtree_rc(kvtree_rc);
}

/* sets the checkpoint id in dataset to be the value specified */
int scr_dataset_set_ckpt(scr_dataset* dataset, int id)
{
  int kvtree_rc = kvtree_util_set_int(dataset, SCR_DATASET_KEY_CKPT, id);
  return convert_kvtree_rc(kvtree_rc);
}

/* sets the complete flag for the dataset to be the value specified */
int scr_dataset_set_complete(scr_dataset* dataset, int complete)
{
  int kvtree_rc = kvtree_util_set_int(dataset, SCR_DATASET_KEY_COMPLETE, complete);
  return convert_kvtree_rc(kvtree_rc);
}

/*
=========================================
Get field values
=========================================
*/

/* gets id recorded in dataset, returns SCR_SUCCESS if successful */
int scr_dataset_get_id(const scr_dataset* dataset, int* id)
{
  int kvtree_rc = kvtree_util_get_int(dataset, SCR_DATASET_KEY_ID, id);
  return convert_kvtree_rc(kvtree_rc);
}

/* gets username of dataset, returns SCR_SUCCESS if successful */
int scr_dataset_get_username(const scr_dataset* dataset, char** name)
{
  int kvtree_rc = kvtree_util_get_str(dataset, SCR_DATASET_KEY_USER, name);
  return convert_kvtree_rc(kvtree_rc);
}

/* gets simulation name of dataset, returns SCR_SUCCESS if successful */
int scr_dataset_get_jobname(const scr_dataset* dataset, char** name)
{
  int kvtree_rc = kvtree_util_get_str(dataset, SCR_DATASET_KEY_JOBNAME, name);
  return convert_kvtree_rc(kvtree_rc);
}

/* gets name of dataset, returns SCR_SUCCESS if successful */
int scr_dataset_get_name(const scr_dataset* dataset, char** name)
{
  int kvtree_rc = kvtree_util_get_str(dataset, SCR_DATASET_KEY_NAME, name);
  return convert_kvtree_rc(kvtree_rc);
}

/* gets size of dataset (in bytes), returns SCR_SUCCESS if successful */
int scr_dataset_get_size(const scr_dataset* dataset, unsigned long* size)
{
  int kvtree_rc = kvtree_util_get_bytecount(dataset, SCR_DATASET_KEY_SIZE, size);
  return convert_kvtree_rc(kvtree_rc);
}

/* gets number of (logical) files in dataset, returns SCR_SUCCESS if successful */
int scr_dataset_get_files(const scr_dataset* dataset, int* files)
{
  int kvtree_rc = kvtree_util_get_int(dataset, SCR_DATASET_KEY_FILES, files);
  return convert_kvtree_rc(kvtree_rc);
}

/* gets created timestamp of dataset, returns SCR_SUCCESS if successful */
int scr_dataset_get_created(const scr_dataset* dataset, int64_t* usecs)
{
  int kvtree_rc = kvtree_util_get_int64(dataset, SCR_DATASET_KEY_CREATED, usecs);
  return convert_kvtree_rc(kvtree_rc);
}

/* gets the jobid in which the dataset was created, returns SCR_SUCCESS if successful */
int scr_dataset_get_jobid(const scr_dataset* dataset, char** jobid)
{
  int kvtree_rc = kvtree_util_get_str(dataset, SCR_DATASET_KEY_JOBID, jobid);
  return convert_kvtree_rc(kvtree_rc);
}

/* gets the cluster name on which the dataset was created, returns SCR_SUCCESS if successful */
int scr_dataset_get_cluster(const scr_dataset* dataset, char** name)
{
  int kvtree_rc = kvtree_util_get_str(dataset, SCR_DATASET_KEY_CLUSTER, name);
  return convert_kvtree_rc(kvtree_rc);
}

/* gets checkpoint id recorded in dataset, returns SCR_SUCCESS if successful */
int scr_dataset_get_ckpt(const scr_dataset* dataset, int* id)
{
  int kvtree_rc = kvtree_util_get_int(dataset, SCR_DATASET_KEY_CKPT, id);
  return convert_kvtree_rc(kvtree_rc);
}

/* gets complete flag recorded in dataset, returns SCR_SUCCESS if successful */
int scr_dataset_get_complete(const scr_dataset* dataset, int* complete)
{
  int kvtree_rc = kvtree_util_get_int(dataset, SCR_DATASET_KEY_COMPLETE, complete);
  return convert_kvtree_rc(kvtree_rc);
}

/*
=========================================
Check field values
=========================================
*/

/* set flags associated with dataset */
int scr_dataset_set_flags(scr_dataset* dataset, int flags)
{
  int is_ckpt   = (flags & SCR_FLAG_CHECKPOINT) ? 1 : 0;
  int is_output = (flags & SCR_FLAG_OUTPUT)     ? 1 : 0;
  kvtree_util_set_int(dataset, SCR_DATASET_KEY_FLAG_CKPT, is_ckpt);
  int kvtree_rc = kvtree_util_set_int(dataset, SCR_DATASET_KEY_FLAG_OUTPUT, is_output);
  return convert_kvtree_rc(kvtree_rc);
}

/* sets flag to 1 if checkpoint flag is set on this dataset, returns SCR_SUCCESS if successful */
int scr_dataset_get_flag_ckpt(const scr_dataset* dataset, int* flag)
{
  int kvtree_rc = kvtree_util_get_int(dataset, SCR_DATASET_KEY_FLAG_CKPT, flag);
  return convert_kvtree_rc(kvtree_rc);
}

/* sets flag to 1 if output flag is set on this dataset, returns SCR_SUCCESS if successful */
int scr_dataset_get_flag_output(const scr_dataset* dataset, int* flag)
{
  int kvtree_rc = kvtree_util_get_int(dataset, SCR_DATASET_KEY_FLAG_OUTPUT, flag);
  return convert_kvtree_rc(kvtree_rc);
}

/* returns 1 if dataset is a checkpoint, 0 otherwise */
int scr_dataset_is_ckpt(const scr_dataset* dataset)
{
  int flag;
  if (scr_dataset_get_flag_ckpt(dataset, &flag) == SCR_SUCCESS) {
    return flag;
  }
  return 0;
}

/* returns 1 if dataset is output, 0 otherwise */
int scr_dataset_is_output(const scr_dataset* dataset)
{
  int flag;
  if (scr_dataset_get_flag_output(dataset, &flag) == SCR_SUCCESS) {
    return flag;
  }
  return 0;
}
