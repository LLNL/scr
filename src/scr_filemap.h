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

#ifndef SCR_FILEMAP_H
#define SCR_FILEMAP_H

#include "scr.h"
#include "scr_meta.h"
#include "scr_dataset.h"

#include "spath.h"
#include "kvtree.h"

/* a special type of hash */
typedef kvtree scr_filemap;

/*
=========================================
Filemap set/get/unset data functions
=========================================
*/

/* adds a new filename to the filemap */
int scr_filemap_add_file(scr_filemap* map, const char* file);

/* removes a filename from the filemap */
int scr_filemap_remove_file(scr_filemap* map, const char* file);

/* sets the dataset for the files */
int scr_filemap_set_dataset(scr_filemap* map, kvtree* hash);

/* copies the dataset into hash */
int scr_filemap_get_dataset(const scr_filemap* map, kvtree* hash);

/* unset the dataset */
int scr_filemap_unset_dataset(scr_filemap* map);

/* sets metadata for file */
int scr_filemap_set_meta(scr_filemap* map, const char* file, const scr_meta* meta);

/* gets metadata for file */
int scr_filemap_get_meta(const scr_filemap* map, const char* file, scr_meta* meta);

/* unsets metadata for file */
int scr_filemap_unset_meta(scr_filemap* map, const char* file);

/*
=========================================
Filemap clear and copy functions
=========================================
*/

/* clear the filemap completely */
int scr_filemap_clear(scr_filemap* map);

/* adds all files from map2 to map1 */
int scr_filemap_merge(scr_filemap* map1, scr_filemap* map2);

/*
=========================================
Filemap list functions
=========================================
*/

/* given a filemap, return the number of files and a list of the filenames */
/* TODO: must free files list when done with it */
int scr_filemap_list_files(const scr_filemap* map, int* numfiles, char*** files);

/*
=========================================
Filemap iterator functions
=========================================
*/

/* given a filemap, return a hash elem pointer to the first file */
kvtree_elem* scr_filemap_first_file(const scr_filemap* map);

/*
=========================================
Filemap query count functions
=========================================
*/

/* return the number of files in the filemap */
int scr_filemap_num_files(const scr_filemap* map);

/*
=========================================
Filemap read/write/free functions
=========================================
*/

/* reads specified file and fills in filemap structure */
int scr_filemap_read(const spath* file, scr_filemap* map);

/* writes given filemap to specified file */
int scr_filemap_write(const spath* file, const scr_filemap* map);

/* create a new filemap structure */
scr_filemap* scr_filemap_new(void);

/* free memory resources assocaited with filemap */
int scr_filemap_delete(scr_filemap** ptr_map);

#endif
