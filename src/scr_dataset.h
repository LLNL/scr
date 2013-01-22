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

#ifndef SCR_DATASET_H
#define SCR_DATASET_H

#include <sys/types.h>

/* needed for SCR_MAX_FILENAME */
#include "scr.h"
#include "scr_hash.h"
#include "scr_hash_util.h"

/* compute crc32, needed for uLong */
#include <zlib.h>

typedef scr_hash scr_dataset;

/*
=========================================
Allocate, delete, and copy functions
=========================================
*/

/* allocate a new dataset object */
scr_dataset* scr_dataset_new();

/* free memory assigned to dataset object */
int scr_dataset_delete(scr_dataset** ptr_dataset);

/*
=========================================
Set field values
=========================================
*/

/* sets the id in dataset to be the value specified */
int scr_dataset_set_id(scr_dataset* dataset, int id);

/* sets the username of the dataset */
int scr_dataset_set_username(scr_dataset* dataset, const char* user);

/* sets the simulation name of the dataset */
int scr_dataset_set_jobname(scr_dataset* dataset, const char* name);

/* sets the name of the dataset */
int scr_dataset_set_name(scr_dataset* dataset, const char* name);

/* sets the size of the dataset (in bytes) */
int scr_dataset_set_size(scr_dataset* dataset, unsigned long size);

/* sets the number of (logical) files in the dataset */
int scr_dataset_set_files(scr_dataset* dataset, int files);

/* sets the created timestamp for the dataset */
int scr_dataset_set_created(scr_dataset* dataset, int64_t created);

/* sets the jobid in which the dataset is created */
int scr_dataset_set_jobid(scr_dataset* dataset, const char* jobid);

/* sets the cluster name on which the dataset is created */
int scr_dataset_set_cluster(scr_dataset* dataset, const char* name);

/* sets the checkpoint id in dataset to be the value specified */
int scr_dataset_set_ckpt(scr_dataset* dataset, int id);

/* sets the complete flag for the dataset to be the value specified */
int scr_dataset_set_complete(scr_dataset* dataset, int complete);

/*
=========================================
Get field values
=========================================
*/

/* gets id recorded in dataset, returns SCR_SUCCESS if successful */
int scr_dataset_get_id(const scr_dataset* dataset, int* id);

/* gets username of dataset, returns SCR_SUCCESS if successful */
int scr_dataset_get_username(const scr_dataset* dataset, char** name);

/* gets simulation name of dataset, returns SCR_SUCCESS if successful */
int scr_dataset_get_jobname(const scr_dataset* dataset, char** name);

/* gets name of dataset, returns SCR_SUCCESS if successful */
int scr_dataset_get_name(const scr_dataset* dataset, char** name);

/* gets size of dataset (in bytes), returns SCR_SUCCESS if successful */
int scr_dataset_get_size(const scr_dataset* dataset, unsigned long* size);

/* gets number of (logical) files in dataset, returns SCR_SUCCESS if successful */
int scr_dataset_get_files(const scr_dataset* dataset, int* files);

/* gets created timestamp of dataset, returns SCR_SUCCESS if successful */
int scr_dataset_get_created(const scr_dataset* dataset, int64_t* created);

/* gets the jobid in which the dataset was created, returns SCR_SUCCESS if successful */
int scr_dataset_get_jobid(const scr_dataset* dataset, char** jobid);

/* gets the cluster name on which the dataset was created, returns SCR_SUCCESS if successful */
int scr_dataset_get_cluster(const scr_dataset* dataset, char** name);

/* gets checkpoint id recorded in dataset, returns SCR_SUCCESS if successful */
int scr_dataset_get_ckpt(const scr_dataset* dataset, int* id);

/* gets complete flag recorded in dataset, returns SCR_SUCCESS if successful */
int scr_dataset_get_complete(const scr_dataset* dataset, int* complete);

/*
=========================================
Check field values
=========================================
*/

#endif
