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
int scr_meta_delete(scr_meta* meta)
{
  int rc = scr_hash_delete(meta);
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

/* initialize meta structure to represent file, filetype, and complete */
int scr_meta_set(scr_meta* meta, const char* file, const char* type, unsigned long size, int checkpoint_id, int rank, int ranks, int complete)
{
  scr_meta_set_filename(meta, file);
  scr_meta_set_filetype(meta, type);
  scr_meta_set_filesize(meta, size);
  scr_meta_set_checkpoint(meta, checkpoint_id);
  scr_meta_set_rank(meta, rank);
  scr_meta_set_ranks(meta, ranks);
  scr_meta_set_complete(meta, complete);
  return SCR_SUCCESS;
}

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

/* sets the filename value in meta data, strips any leading directory */
int scr_meta_set_filename(scr_meta* meta, const char* file)
{
  /* split file into path and name components */
  char path[SCR_MAX_FILENAME];
  char name[SCR_MAX_FILENAME];
  scr_split_path(file, path, name);

  scr_hash_unset(meta, SCR_META_KEY_FILE);
  scr_hash_set_kv(meta, SCR_META_KEY_FILE, name);
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

/* gets filename recorded in meta data, returns SCR_SUCCESS if successful */
int scr_meta_get_filename(const scr_meta* meta, char** filename)
{
  char* filename_str = scr_hash_elem_get_first_val(meta, SCR_META_KEY_FILE);
  if (filename_str != NULL) {
    *filename = filename_str;
    return SCR_SUCCESS;
  }
  return SCR_FAILURE;
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
  char* filetype_str = scr_hash_elem_get_first_val(meta, SCR_META_KEY_TYPE);
  if (filetype_str != NULL) {
    *filetype = filetype_str;
    return SCR_SUCCESS;
  }
  return SCR_FAILURE;
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

/*
=========================================
Meta data files
=========================================
*/

/* build meta data filename for input file */
int scr_meta_name(char* metaname, const char* file)
{
    sprintf(metaname, "%s.scr", file);
    return SCR_SUCCESS;
}

/* read meta for file_orig and fill in meta structure */
int scr_meta_read(const char* file_orig, scr_meta* meta)
{
  /* build meta filename */
  char file_meta[SCR_MAX_FILENAME];
  scr_meta_name(file_meta, file_orig);

  /* can't read file, return error */
  if (access(file_meta, R_OK) < 0) {
    return SCR_FAILURE;
  }

  /* read the hash from disk */
  if (scr_hash_read(file_meta, meta) != SCR_SUCCESS) {
    return SCR_FAILURE;
  }

  return SCR_SUCCESS;
}

/* creates corresponding .scr meta file for file to record completion info */
int scr_meta_write(const char* file_orig, const scr_meta* meta)
{
  int rc = SCR_SUCCESS;

  /* create the .scr extension */
  char file_meta[SCR_MAX_FILENAME];
  scr_meta_name(file_meta, file_orig);

  /* check that the filename matches the filename in the meta data */
  char path[SCR_MAX_FILENAME];
  char name[SCR_MAX_FILENAME];
  scr_split_path(file_orig, path, name);
  if (scr_meta_check_filename(meta, name) != SCR_SUCCESS) {
    scr_abort(-1, "Basename in %s does not match filename in meta data @ %s:%d",
            file_orig, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* write the hash to disk */
  if (scr_hash_write(file_meta, meta) != SCR_SUCCESS) {
    scr_err("Opening meta file for write: scr_open(%s) @ %s:%d",
            file_meta, __FILE__, __LINE__
    );
    rc = SCR_FAILURE;
  }

  return rc;
}

/* unlink meta data file */
int scr_meta_unlink(const char* file)
{
  /* create the .scr extension for file */
  char metaname[SCR_MAX_FILENAME];
  scr_meta_name(metaname, file);

  /* delete the file */
  unlink(metaname);

  return SCR_SUCCESS;
}

/* TODO: this file isn't the most obvious location to place this function, but it uses crc and meta data */
/* compute crc32 for file and check value against meta data file, set it if not already set */
int scr_compute_crc(const char* file)
{
  /* check that we got a filename */
  if (file == NULL || strcmp(file, "") == 0) {
    return SCR_FAILURE;
  }

  int valid = 1;

  /* allocate a new meta data object */
  scr_meta* meta = scr_meta_new();

  /* read in the meta data for this file */
  if (valid && scr_meta_read(file, meta) != SCR_SUCCESS) {
    scr_err("Failed to read meta data file for file to compute CRC32: %s @ %s:%d",
            file, __FILE__, __LINE__
    );
    valid = 0;
  }

  /* check that the file is complete */
  if (valid && scr_meta_is_complete(meta) != SCR_SUCCESS) {
    scr_err("File is marked as incomplete: %s",
            file, __FILE__, __LINE__
    );
    valid = 0;
  }

  /* check that the filesize matches the value in the meta file */
  unsigned long size = scr_filesize(file);
  if (valid && scr_meta_check_filesize(meta, size) != SCR_SUCCESS) {
    scr_err("File size does not match size recorded in meta file: %s",
            file, __FILE__, __LINE__
    );
    valid = 0;
  }

  /* compute the CRC32 value for this file */
  uLong crc = crc32(0L, Z_NULL, 0);
  if (valid && scr_crc32(file, &crc) != SCR_SUCCESS) {
    scr_err("Computing CRC32 for file %s @ %s:%d",
              file, __FILE__, __LINE__
    );
    valid = 0;
  }

  /* now check the CRC32 value if it was set in the meta file, and set it if not */
  uLong crc_meta;
  if (valid && scr_meta_get_crc32(meta, &crc_meta) == SCR_SUCCESS) {
    /* the crc is already set in the meta file, let's check that we match */
    if (crc != crc_meta) {
      scr_err("CRC32 mismatch detected for file %s @ %s:%d",
              file, __FILE__, __LINE__
      );

      /* crc check failed, mark file as invalid */
      scr_meta_set_complete(meta, 0);
      scr_meta_write(file, meta);

      valid = 0;
    }
  } else {
    /* the crc was not set in the meta file, so let's set it now */
    scr_meta_set_crc32(meta, crc);
    scr_meta_write(file, meta);
  }

  /* delete the meta data object */
  scr_meta_delete(meta);

  if (valid) {
    return SCR_SUCCESS;
  }
  return SCR_FAILURE;
}
