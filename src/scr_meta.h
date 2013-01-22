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

#ifndef SCR_META_H
#define SCR_META_H

#include <sys/types.h>

/* needed for SCR_MAX_FILENAME */
#include "scr.h"
#include "scr_hash.h"
#include "scr_hash_util.h"

/* compute crc32, needed for uLong */
#include <zlib.h>

#define SCR_META_FILE_USER (SCR_META_KEY_TYPE_USER)
#define SCR_META_FILE_XOR  (SCR_META_KEY_TYPE_XOR)

typedef scr_hash scr_meta;

/*
=========================================
Allocate, delete, and copy functions
=========================================
*/

/* allocate a new meta data object */
scr_meta* scr_meta_new();

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

/* sets the filename value in meta data, strips any leading directory */
int scr_meta_set_filename(scr_meta* meta, const char* file);

/* sets the filesize to be the value specified */
int scr_meta_set_filesize(scr_meta* meta, unsigned long filesize);

/* sets the filename value in meta data, strips any leading directory */
int scr_meta_set_filetype(scr_meta* meta, const char* filetype);

/* set the completeness field on meta */
int scr_meta_set_complete(scr_meta* meta, int complete);

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

/* gets filename recorded in meta data, returns SCR_SUCCESS if successful */
int scr_meta_get_filename(const scr_meta* meta, char** filename);

/* gets filesize recorded in meta data, returns SCR_SUCCESS if successful */
int scr_meta_get_filesize(const scr_meta* meta, unsigned long* filesize);

/* gets filetype recorded in meta data, returns SCR_SUCCESS if successful */
int scr_meta_get_filetype(const scr_meta* meta, char** filetype);

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

/* return SCR_SUCCESS if filename is set in meta data, and if it matches the specified value */
int scr_meta_check_filename(const scr_meta* meta, const char* filename);

/* return SCR_SUCCESS if filetype is set in meta data, and if it matches the specified value */
int scr_meta_check_filetype(const scr_meta* meta, const char* filetype);

/* returns SCR_SUCCESS if filesize is set in meta data, and if it matches specified value */
int scr_meta_check_filesize(const scr_meta* meta, unsigned long filesize);

#endif
