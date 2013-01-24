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
#include "scr_util.h"
#include "scr_io.h"
#include "scr_path.h"
#include "scr_meta.h"
#include "scr_hash.h"

#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <strings.h>

/* variable length args */
#include <stdarg.h>
#include <errno.h>

/* basename/dirname */
#include <unistd.h>
#include <libgen.h>

/* compute crc32 */
#include <zlib.h>

/*
=========================================
Allocate, delete, and copy functions
=========================================
*/

/* allocate a new meta data object */
scr_meta* scr_meta_new()
{
  scr_meta* meta = scr_hash_new();
  if (meta == NULL) {
    scr_err("Failed to allocate meta data object @ %s:%d", __FILE__, __LINE__);
  }
  return meta;
}

/* free memory assigned to meta data object */
int scr_meta_delete(scr_meta** ptr_meta)
{
  int rc = scr_hash_delete(ptr_meta);
  return rc;
}

/* clear m1 and copy contents of m2 into m1 */
int scr_meta_copy(scr_meta* m1, const scr_meta* m2)
{
  scr_hash_unset_all(m1);
  int rc = scr_hash_merge(m1, m2);
  return rc;
}

/*
=========================================
Set field values
=========================================
*/

/* sets the checkpoint id in meta data to be the value specified */
int scr_meta_set_checkpoint(scr_meta* meta, int ckpt)
{
  scr_hash_unset(meta, SCR_META_KEY_CKPT);
  scr_hash_set_kv_int(meta, SCR_META_KEY_CKPT, ckpt);
  return SCR_SUCCESS;
}

/* sets the rank in meta data to be the value specified */
int scr_meta_set_rank(scr_meta* meta, int rank)
{
  scr_hash_unset(meta, SCR_META_KEY_RANK);
  scr_hash_set_kv_int(meta, SCR_META_KEY_RANK, rank);
  return SCR_SUCCESS;
}

/* sets the rank in meta data to be the value specified */
int scr_meta_set_ranks(scr_meta* meta, int ranks)
{
  scr_hash_unset(meta, SCR_META_KEY_RANKS);
  scr_hash_set_kv_int(meta, SCR_META_KEY_RANKS, ranks);
  return SCR_SUCCESS;
}

/* sets the original filename value in meta data */
int scr_meta_set_orig(scr_meta* meta, const char* file)
{
  scr_hash_unset(meta, SCR_META_KEY_ORIG);
  scr_hash_set_kv(meta, SCR_META_KEY_ORIG, file);
  return SCR_SUCCESS;
}

/* sets the full path to the original filename value in meta data */
int scr_meta_set_origpath(scr_meta* meta, const char* file)
{
  scr_hash_unset(meta, SCR_META_KEY_PATH);
  scr_hash_set_kv(meta, SCR_META_KEY_PATH, file);
  return SCR_SUCCESS;
}

/* sets the full directory to the original filename value in meta data */
int scr_meta_set_origname(scr_meta* meta, const char* file)
{
  scr_hash_unset(meta, SCR_META_KEY_NAME);
  scr_hash_set_kv(meta, SCR_META_KEY_NAME, file);
  return SCR_SUCCESS;
}

/* sets the filename value in meta data, strips any leading directory */
int scr_meta_set_filename(scr_meta* meta, const char* file)
{
  /* extract file name */
  scr_path* path_file = scr_path_from_str(file);
  scr_path_basename(path_file);
  char* name = scr_path_strdup(path_file);

  scr_hash_unset(meta, SCR_META_KEY_FILE);
  scr_hash_set_kv(meta, SCR_META_KEY_FILE, name);

  /* free the path and string */
  scr_free(&name);
  scr_path_delete(&path_file);

  return SCR_SUCCESS;
}

/* sets the filesize to be the value specified */
int scr_meta_set_filesize(scr_meta* meta, unsigned long filesize)
{
  int rc = scr_hash_util_set_bytecount(meta, SCR_META_KEY_SIZE, filesize);
  return rc;
}

/* sets the filename value in meta data, strips any leading directory */
int scr_meta_set_filetype(scr_meta* meta, const char* filetype)
{
  scr_hash_unset(meta, SCR_META_KEY_TYPE);
  scr_hash_set_kv(meta, SCR_META_KEY_TYPE, filetype);
  return SCR_SUCCESS;
}

/* sets complete value in meta data, overwrites any existing value with new value */
int scr_meta_set_complete(scr_meta* meta, int complete)
{
  scr_hash_unset(meta, SCR_META_KEY_COMPLETE);
  scr_hash_set_kv_int(meta, SCR_META_KEY_COMPLETE, complete);
  return SCR_SUCCESS;
}

/* sets crc value in meta data, overwrites any existing value with new value */
int scr_meta_set_crc32(scr_meta* meta, uLong crc)
{
  int rc = scr_hash_util_set_crc32(meta, SCR_META_KEY_CRC, crc);
  return rc;
}

/*
=========================================
Get field values
=========================================
*/

/* gets checkpoint id recorded in meta data, returns SCR_SUCCESS if successful */
int scr_meta_get_checkpoint(const scr_meta* meta, int* ckpt)
{
  int rc = scr_hash_util_get_int(meta, SCR_META_KEY_CKPT, ckpt);
  return rc;
}

/* gets rank value recorded in meta data, returns SCR_SUCCESS if successful */
int scr_meta_get_rank(const scr_meta* meta, int* rank)
{
  int rc = scr_hash_util_get_int(meta, SCR_META_KEY_RANK, rank);
  return rc;
}

/* gets ranks value recorded in meta data, returns SCR_SUCCESS if successful */
int scr_meta_get_ranks(const scr_meta* meta, int* ranks)
{
  int rc = scr_hash_util_get_int(meta, SCR_META_KEY_RANKS, ranks);
  return rc;
}

/* gets original filename recorded in meta data, returns SCR_SUCCESS if successful */
int scr_meta_get_orig(const scr_meta* meta, char** filename)
{
  int rc = scr_hash_util_get_str(meta, SCR_META_KEY_ORIG, filename);
  return rc;
}

/* gets full path to the original filename recorded in meta data, returns SCR_SUCCESS if successful */
int scr_meta_get_origpath(const scr_meta* meta, char** filename)
{
  int rc = scr_hash_util_get_str(meta, SCR_META_KEY_PATH, filename);
  return rc;
}

/* gets the name of the original filename recorded in meta data, returns SCR_SUCCESS if successful */
int scr_meta_get_origname(const scr_meta* meta, char** filename)
{
  int rc = scr_hash_util_get_str(meta, SCR_META_KEY_NAME, filename);
  return rc;
}

/* gets filename recorded in meta data, returns SCR_SUCCESS if successful */
int scr_meta_get_filename(const scr_meta* meta, char** filename)
{
  int rc = scr_hash_util_get_str(meta, SCR_META_KEY_FILE, filename);
  return rc;
}

/* gets filesize recorded in meta data, returns SCR_SUCCESS if successful */
int scr_meta_get_filesize(const scr_meta* meta, unsigned long* filesize)
{
  int rc = scr_hash_util_get_bytecount(meta, SCR_META_KEY_SIZE, filesize);
  return rc;
}

/* gets filetype recorded in meta data, returns SCR_SUCCESS if successful */
int scr_meta_get_filetype(const scr_meta* meta, char** filetype)
{
  int rc = scr_hash_util_get_str(meta, SCR_META_KEY_TYPE, filetype);
  return rc;
}

/* get the completeness field in meta data, returns SCR_SUCCESS if successful */
int scr_meta_get_complete(const scr_meta* meta, int* complete)
{
  int rc = scr_hash_util_get_int(meta, SCR_META_KEY_COMPLETE, complete);
  return rc;
}

/* get the crc32 field in meta data, returns SCR_SUCCESS if a field is set */
int scr_meta_get_crc32(const scr_meta* meta, uLong* crc)
{
  int rc = scr_hash_util_get_crc32(meta, SCR_META_KEY_CRC, crc);
  return rc;
}

/*
=========================================
Check field values
=========================================
*/

/* return SCR_SUCCESS if meta data is marked as complete */
int scr_meta_is_complete(const scr_meta* meta)
{
  int complete = 0;
  if (scr_hash_util_get_int(meta, SCR_META_KEY_COMPLETE, &complete) == SCR_SUCCESS) {
    if (complete == 1) {
      return SCR_SUCCESS;
    }
  }
  return SCR_FAILURE;
}

/* return SCR_SUCCESS if rank is set in meta data, and if it matches the specified value */
int scr_meta_check_rank(const scr_meta* meta, int rank)
{
  int rank_meta;
  if (scr_hash_util_get_int(meta, SCR_META_KEY_RANK, &rank_meta) == SCR_SUCCESS) {
    if (rank == rank_meta) {
      return SCR_SUCCESS;
    }
  }
  return SCR_FAILURE;
}

/* return SCR_SUCCESS if ranks is set in meta data, and if it matches the specified value */
int scr_meta_check_ranks(const scr_meta* meta, int ranks)
{
  int ranks_meta;
  if (scr_hash_util_get_int(meta, SCR_META_KEY_RANKS, &ranks_meta) == SCR_SUCCESS) {
    if (ranks == ranks_meta) {
      return SCR_SUCCESS;
    }
  }
  return SCR_FAILURE;
}

/* return SCR_SUCCESS if checkpoint_id is set in meta data, and if it matches the specified value */
int scr_meta_check_checkpoint(const scr_meta* meta, int ckpt)
{
  int ckpt_meta;
  if (scr_hash_util_get_int(meta, SCR_META_KEY_CKPT, &ckpt_meta) == SCR_SUCCESS) {
    if (ckpt == ckpt_meta) {
      return SCR_SUCCESS;
    }
  }
  return SCR_FAILURE;
}

/* return SCR_SUCCESS if filename is set in meta data, and if it matches the specified value */
int scr_meta_check_filename(const scr_meta* meta, const char* filename)
{
  char* filename_meta = scr_hash_elem_get_first_val(meta, SCR_META_KEY_FILE);
  if (filename_meta != NULL) {
    if (strcmp(filename, filename_meta) == 0) {
      return SCR_SUCCESS;
    }
  } 
  return SCR_FAILURE;
}

/* return SCR_SUCCESS if filetype is set in meta data, and if it matches the specified value */
int scr_meta_check_filetype(const scr_meta* meta, const char* filetype)
{
  char* filetype_meta = scr_hash_elem_get_first_val(meta, SCR_META_KEY_TYPE);
  if (filetype_meta != NULL) {
    if (strcmp(filetype, filetype_meta) == 0) {
      return SCR_SUCCESS;
    }
  } 
  return SCR_FAILURE;
}

/* returns SCR_SUCCESS if filesize is set in meta data, and if it matches specified value */
int scr_meta_check_filesize(const scr_meta* meta, unsigned long filesize)
{
  unsigned long filesize_meta = 0;
  if (scr_hash_util_get_bytecount(meta, SCR_META_KEY_SIZE, &filesize_meta) == SCR_SUCCESS) {
    if (filesize == filesize_meta) {
      return SCR_SUCCESS;
    }
  }
  return SCR_FAILURE;
}
