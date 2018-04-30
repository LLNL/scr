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
#include "scr_util.h"
#include "scr_io.h"
#include "scr_meta.h"

#include "spath.h"
#include "kvtree.h"

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
  scr_meta* meta = kvtree_new();
  if (meta == NULL) {
    scr_err("Failed to allocate meta data object @ %s:%d", __FILE__, __LINE__);
  }
  return meta;
}

/* free memory assigned to meta data object */
int scr_meta_delete(scr_meta** ptr_meta)
{
  int rc = kvtree_delete(ptr_meta);
  return rc;
}

/* clear m1 and copy contents of m2 into m1 */
int scr_meta_copy(scr_meta* m1, const scr_meta* m2)
{
  kvtree_unset_all(m1);
  int rc = kvtree_merge(m1, m2);
  return (rc == KVTREE_SUCCESS) ? SCR_SUCCESS : SCR_FAILURE;
}

/*
=========================================
Set field values
=========================================
*/

/* sets the checkpoint id in meta data to be the value specified */
int scr_meta_set_checkpoint(scr_meta* meta, int ckpt)
{
  kvtree_unset(meta, SCR_META_KEY_CKPT);
  kvtree_set_kv_int(meta, SCR_META_KEY_CKPT, ckpt);
  return SCR_SUCCESS;
}

/* sets the rank in meta data to be the value specified */
int scr_meta_set_rank(scr_meta* meta, int rank)
{
  kvtree_unset(meta, SCR_META_KEY_RANK);
  kvtree_set_kv_int(meta, SCR_META_KEY_RANK, rank);
  return SCR_SUCCESS;
}

/* sets the rank in meta data to be the value specified */
int scr_meta_set_ranks(scr_meta* meta, int ranks)
{
  kvtree_unset(meta, SCR_META_KEY_RANKS);
  kvtree_set_kv_int(meta, SCR_META_KEY_RANKS, ranks);
  return SCR_SUCCESS;
}

/* sets the original filename value in meta data */
int scr_meta_set_orig(scr_meta* meta, const char* file)
{
  kvtree_unset(meta, SCR_META_KEY_ORIG);
  kvtree_set_kv(meta, SCR_META_KEY_ORIG, file);
  return SCR_SUCCESS;
}

/* sets the full path to the original filename value in meta data */
int scr_meta_set_origpath(scr_meta* meta, const char* file)
{
  kvtree_unset(meta, SCR_META_KEY_PATH);
  kvtree_set_kv(meta, SCR_META_KEY_PATH, file);
  return SCR_SUCCESS;
}

/* sets the full directory to the original filename value in meta data */
int scr_meta_set_origname(scr_meta* meta, const char* file)
{
  kvtree_unset(meta, SCR_META_KEY_NAME);
  kvtree_set_kv(meta, SCR_META_KEY_NAME, file);
  return SCR_SUCCESS;
}

/* sets the filesize to be the value specified */
int scr_meta_set_filesize(scr_meta* meta, unsigned long filesize)
{
  int rc = kvtree_util_set_bytecount(meta, SCR_META_KEY_SIZE, filesize);
  return (rc == KVTREE_SUCCESS) ? SCR_SUCCESS : SCR_FAILURE;
}

/* sets the filename value in meta data, strips any leading directory */
int scr_meta_set_filetype(scr_meta* meta, const char* filetype)
{
  kvtree_unset(meta, SCR_META_KEY_TYPE);
  kvtree_set_kv(meta, SCR_META_KEY_TYPE, filetype);
  return SCR_SUCCESS;
}

/* sets complete value in meta data, overwrites any existing value with new value */
int scr_meta_set_complete(scr_meta* meta, int complete)
{
  kvtree_unset(meta, SCR_META_KEY_COMPLETE);
  kvtree_set_kv_int(meta, SCR_META_KEY_COMPLETE, complete);
  return SCR_SUCCESS;
}

/* sets crc value in meta data, overwrites any existing value with new value */
int scr_meta_set_crc32(scr_meta* meta, uLong crc)
{
  int rc = kvtree_util_set_crc32(meta, SCR_META_KEY_CRC, crc);
  return (rc == KVTREE_SUCCESS) ? SCR_SUCCESS : SCR_FAILURE;
}

/*
=========================================
Get field values
=========================================
*/

/* gets checkpoint id recorded in meta data, returns SCR_SUCCESS if successful */
int scr_meta_get_checkpoint(const scr_meta* meta, int* ckpt)
{
  int rc = kvtree_util_get_int(meta, SCR_META_KEY_CKPT, ckpt);
  return (rc == KVTREE_SUCCESS) ? SCR_SUCCESS : SCR_FAILURE;
}

/* gets rank value recorded in meta data, returns SCR_SUCCESS if successful */
int scr_meta_get_rank(const scr_meta* meta, int* rank)
{
  int rc = kvtree_util_get_int(meta, SCR_META_KEY_RANK, rank);
  return (rc == KVTREE_SUCCESS) ? SCR_SUCCESS : SCR_FAILURE;
}

/* gets ranks value recorded in meta data, returns SCR_SUCCESS if successful */
int scr_meta_get_ranks(const scr_meta* meta, int* ranks)
{
  int rc = kvtree_util_get_int(meta, SCR_META_KEY_RANKS, ranks);
  return (rc == KVTREE_SUCCESS) ? SCR_SUCCESS : SCR_FAILURE;
}

/* gets original filename recorded in meta data, returns SCR_SUCCESS if successful */
int scr_meta_get_orig(const scr_meta* meta, char** filename)
{
  int rc = kvtree_util_get_str(meta, SCR_META_KEY_ORIG, filename);
  return (rc == KVTREE_SUCCESS) ? SCR_SUCCESS : SCR_FAILURE;
}

/* gets full path to the original filename recorded in meta data, returns SCR_SUCCESS if successful */
int scr_meta_get_origpath(const scr_meta* meta, char** filename)
{
  int rc = kvtree_util_get_str(meta, SCR_META_KEY_PATH, filename);
  return (rc == KVTREE_SUCCESS) ? SCR_SUCCESS : SCR_FAILURE;
}

/* gets the name of the original filename recorded in meta data, returns SCR_SUCCESS if successful */
int scr_meta_get_origname(const scr_meta* meta, char** filename)
{
  int rc = kvtree_util_get_str(meta, SCR_META_KEY_NAME, filename);
  return (rc == KVTREE_SUCCESS) ? SCR_SUCCESS : SCR_FAILURE;
}

/* gets filesize recorded in meta data, returns SCR_SUCCESS if successful */
int scr_meta_get_filesize(const scr_meta* meta, unsigned long* filesize)
{
  int rc = kvtree_util_get_bytecount(meta, SCR_META_KEY_SIZE, filesize);
  return (rc == KVTREE_SUCCESS) ? SCR_SUCCESS : SCR_FAILURE;
}

/* gets filetype recorded in meta data, returns SCR_SUCCESS if successful */
int scr_meta_get_filetype(const scr_meta* meta, char** filetype)
{
  int rc = kvtree_util_get_str(meta, SCR_META_KEY_TYPE, filetype);
  return (rc == KVTREE_SUCCESS) ? SCR_SUCCESS : SCR_FAILURE;
}

/* get the completeness field in meta data, returns SCR_SUCCESS if successful */
int scr_meta_get_complete(const scr_meta* meta, int* complete)
{
  int rc = kvtree_util_get_int(meta, SCR_META_KEY_COMPLETE, complete);
  return (rc == KVTREE_SUCCESS) ? SCR_SUCCESS : SCR_FAILURE;
}

/* get the crc32 field in meta data, returns SCR_SUCCESS if a field is set */
int scr_meta_get_crc32(const scr_meta* meta, uLong* crc)
{
  int rc = kvtree_util_get_crc32(meta, SCR_META_KEY_CRC, crc);
  return (rc == KVTREE_SUCCESS) ? SCR_SUCCESS : SCR_FAILURE;
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
  if (kvtree_util_get_int(meta, SCR_META_KEY_COMPLETE, &complete) == KVTREE_SUCCESS) {
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
  if (kvtree_util_get_int(meta, SCR_META_KEY_RANK, &rank_meta) == KVTREE_SUCCESS) {
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
  if (kvtree_util_get_int(meta, SCR_META_KEY_RANKS, &ranks_meta) == KVTREE_SUCCESS) {
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
  if (kvtree_util_get_int(meta, SCR_META_KEY_CKPT, &ckpt_meta) == KVTREE_SUCCESS) {
    if (ckpt == ckpt_meta) {
      return SCR_SUCCESS;
    }
  }
  return SCR_FAILURE;
}

/* return SCR_SUCCESS if filetype is set in meta data, and if it matches the specified value */
int scr_meta_check_filetype(const scr_meta* meta, const char* filetype)
{
  char* filetype_meta = kvtree_elem_get_first_val(meta, SCR_META_KEY_TYPE);
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
  if (kvtree_util_get_bytecount(meta, SCR_META_KEY_SIZE, &filesize_meta) == KVTREE_SUCCESS) {
    if (filesize == filesize_meta) {
      return SCR_SUCCESS;
    }
  }
  return SCR_FAILURE;
}
