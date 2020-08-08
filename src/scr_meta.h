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

#ifndef SCR_META_H
#define SCR_META_H

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "scr.h"
#include "kvtree.h"
#include "kvtree_util.h"

/* compute crc32, needed for uLong */
#include <zlib.h>

typedef kvtree scr_meta;

/*
=========================================
Allocate, delete, and copy functions
=========================================
*/

/* allocate a new meta data object */
scr_meta* scr_meta_new(void);

/* free memory assigned to meta data object */
int scr_meta_delete(scr_meta** ptr_meta);

/* clear m1 and copy contents of m2 into m1 */
int scr_meta_copy(scr_meta* m1, const scr_meta* m2);

/*
=========================================
Set field values
=========================================
*/

/* sets the checkpoint id in meta data to be the value specified */
int scr_meta_set_checkpoint(scr_meta* meta, int ckpt);

/* sets the rank in meta data to be the value specified */
int scr_meta_set_rank(scr_meta* meta, int rank);

/* sets the rank in meta data to be the value specified */
int scr_meta_set_ranks(scr_meta* meta, int ranks);

/* sets the original filename value in meta data */
int scr_meta_set_orig(scr_meta* meta, const char* file);

/* sets the absolute path to the original file */
int scr_meta_set_origpath(scr_meta* meta, const char* path);

/* sets the name of the original file */
int scr_meta_set_origname(scr_meta* meta, const char* path);

/* sets the filesize to be the value specified */
int scr_meta_set_filesize(scr_meta* meta, unsigned long filesize);

/* set the completeness field on meta */
int scr_meta_set_complete(scr_meta* meta, int complete);

/* capture stat metadata (uid, gid, mode, atime, ctime, mtime) */
int scr_meta_set_stat(scr_meta* meta, struct stat* statbuf);

/* set the crc32 field on meta */
int scr_meta_set_crc32(scr_meta* meta, uLong crc);

/*
=========================================
Get field values
=========================================
*/

/* gets checkpoint id recorded in meta data, returns SCR_SUCCESS if successful */
int scr_meta_get_checkpoint(const scr_meta* meta, int* checkpoint_id);

/* gets rank value recorded in meta data, returns SCR_SUCCESS if successful */
int scr_meta_get_rank(const scr_meta* meta, int* rank);

/* gets ranks value recorded in meta data, returns SCR_SUCCESS if successful */
int scr_meta_get_ranks(const scr_meta* meta, int* ranks);

/* gets original filename recorded in meta data, returns SCR_SUCCESS if successful */
int scr_meta_get_orig(const scr_meta* meta, char** filename);

/* gets full path to original filename recorded in meta data, returns SCR_SUCCESS if successful */
int scr_meta_get_origpath(const scr_meta* meta, char** path);

/* gets name of the original filename recorded in meta data, returns SCR_SUCCESS if successful */
int scr_meta_get_origname(const scr_meta* meta, char** name);

/* gets filesize recorded in meta data, returns SCR_SUCCESS if successful */
int scr_meta_get_filesize(const scr_meta* meta, unsigned long* filesize);

/* get the completeness field in meta data, returns SCR_SUCCESS if successful */
int scr_meta_get_complete(const scr_meta* meta, int* complete);

/* get the crc32 field in meta data, returns SCR_SUCCESS if a field is set */
int scr_meta_get_crc32(const scr_meta* meta, uLong* crc);

/*
=========================================
Check field values
=========================================
*/

/* return SCR_SUCCESS if meta data is marked as complete */
int scr_meta_is_complete(const scr_meta* meta);

/* return SCR_SUCCESS if rank is set in meta data, and if it matches the specified value */
int scr_meta_check_rank(const scr_meta* meta, int rank);

/* return SCR_SUCCESS if ranks is set in meta data, and if it matches the specified value */
int scr_meta_check_ranks(const scr_meta* meta, int ranks);

/* return SCR_SUCCESS if checkpoint_id is set in meta data, and if it matches the specified value */
int scr_meta_check_checkpoint(const scr_meta* meta, int checkpoint_id);

/* returns SCR_SUCCESS if filesize is set in meta data, and if it matches specified value */
int scr_meta_check_filesize(const scr_meta* meta, unsigned long filesize);

/* returns SCR_SUCCESS if mtime is set and if it matches values in statbuf */
int scr_meta_check_mtime(const scr_meta* meta, struct stat* statbuf);

/* returns SCR_SUCCESS if ctime is set and if it matches values in statbuf */
int scr_meta_check_ctime(const scr_meta* meta, struct stat* statbuf);

/* returns SCR_SUCCESS if mode bits, uid, and gid match values in statbuf */
int scr_meta_check_metadata(const scr_meta* meta, struct stat* statbuf);

/* apply stat metadata recorded in meta to given file path */
int scr_meta_apply_stat(const scr_meta* meta, const char* file);

#endif
