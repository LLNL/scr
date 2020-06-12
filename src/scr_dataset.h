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

/* All rights reserved. This program and the accompanying materials
 * are made available under the terms of the BSD-3 license which accompanies this
 * distribution in LICENSE.TXT
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the BSD-3  License in
 * LICENSE.TXT for more details.
 *
 * GOVERNMENT LICENSE RIGHTS-OPEN SOURCE SOFTWARE
 * The Government's rights to use, modify, reproduce, release, perform,
 * display, or disclose this software are subject to the terms of the BSD-3
 * License as provided in Contract No. B609815.
 * Any reproduction of computer software, computer software documentation, or
 * portions thereof marked with this legend must also reproduce the markings.
 *
 * Author: Christopher Holguin <christopher.a.holguin@intel.com>
 *
 * (C) Copyright 2015-2016 Intel Corporation.
 */

#ifndef SCR_DATASET_H
#define SCR_DATASET_H

#include <sys/types.h>

#include "scr.h"
#include "kvtree.h"

/* compute crc32, needed for uLong */
#include <zlib.h>

typedef kvtree scr_dataset;

/*
=========================================
Allocate, delete, and copy functions
=========================================
*/

/* allocate a new dataset object */
scr_dataset* scr_dataset_new(void);

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

/* set flags on dataset */
int scr_dataset_set_flags(scr_dataset* dataset, int flags);

/* returns 1 if dataset is an output set, 0 otherwise */
int scr_dataset_is_ckpt(const scr_dataset* dataset);

/* returns 1 if dataset is a checkpoint, 0 otherwise */
int scr_dataset_is_output(const scr_dataset* dataset);

#endif
