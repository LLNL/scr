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
Metadata functions
=========================================
*/

/* build meta data filename for input file */
int scr_meta_name(char* metaname, const char* file)
{
    sprintf(metaname, "%s.scr", file);
    return SCR_SUCCESS;
}

/* initialize meta structure to represent file, filetype, and complete */
void scr_meta_set(struct scr_meta* meta, const char* file, int rank, int ranks, int checkpoint_id, int filetype, int complete)
{
    /* split file into path and name components */
    char path[SCR_MAX_FILENAME];
    char name[SCR_MAX_FILENAME];
    scr_split_path(file, path, name);

    meta->rank          = rank;
    meta->ranks         = ranks;
    meta->checkpoint_id = checkpoint_id;
    meta->filetype      = filetype;

    strcpy(meta->filename, name);
    meta->filesize       = scr_filesize(file);
    meta->complete       = complete;
    meta->crc32_computed = 0;
    meta->crc32          = crc32(0L, Z_NULL, 0);
}

/* initialize meta structure to represent file, filetype, and complete */
void scr_meta_copy(struct scr_meta* m1, const struct scr_meta* m2)
{
    memcpy(m1, m2, sizeof(struct scr_meta));
}

/* read meta for file_orig and fill in meta structure */
int scr_meta_read(const char* file_orig, struct scr_meta* meta)
{
  /* build meta filename */
  char file_meta[SCR_MAX_FILENAME];
  scr_meta_name(file_meta, file_orig);

  /* can't read file, return error */
  if (access(file_meta, R_OK) < 0) {
    return SCR_FAILURE;
  }

  /* create a new hash to read the file into */
  struct scr_hash* hash = scr_hash_new();

  /* read the meta data file into our hash */
  if (scr_hash_read(file_meta, hash) != SCR_SUCCESS) {
    scr_err("Opening meta file for read %s @ %s:%d",
            file_meta, __FILE__, __LINE__
    );
    scr_hash_delete(hash);
    return SCR_FAILURE;
  }

  /* check that we have exactly one checkpoint */
  struct scr_hash* ckpt_hash = scr_hash_get(hash, SCR_META_KEY_CKPT);
  if (scr_hash_size(ckpt_hash) != 1) {
    scr_err("More than one checkpoint found in meta file %s @ %s:%d",
            file_meta, __FILE__, __LINE__
    );
    scr_hash_delete(hash);
    return SCR_FAILURE;
  }

  /* get the first (and only) checkpoint id */
  char* ckpt_key = scr_hash_elem_get_first_val(hash, SCR_META_KEY_CKPT);
  struct scr_hash* ckpt = scr_hash_get(ckpt_hash, ckpt_key);

  /* check that we have exactly one rank */
  struct scr_hash* rank_hash = scr_hash_get(ckpt, SCR_META_KEY_RANK);
  if (scr_hash_size(rank_hash) != 1) {
    scr_err("More than one rank found in meta file %s @ %s:%d",
            file_meta, __FILE__, __LINE__
    );
    scr_hash_delete(hash);
    return SCR_FAILURE;
  }

  /* get the first (and only) rank */
  char* rank_key = scr_hash_elem_get_first_val(ckpt, SCR_META_KEY_RANK);
  struct scr_hash* rank = scr_hash_get(rank_hash, rank_key);

  /* check that we have exactly one file */
  struct scr_hash* file_hash = scr_hash_get(rank, SCR_META_KEY_FILE);
  if (scr_hash_size(file_hash) != 1) {
    scr_err("More than one file found in meta file %s @ %s:%d",
            file_meta, __FILE__, __LINE__
    );
    scr_hash_delete(hash);
    return SCR_FAILURE;
  }

  /* get the first (and only) file */
  char* file_key = scr_hash_elem_get_first_val(rank, SCR_META_KEY_FILE);
  struct scr_hash* file = scr_hash_get(file_hash, file_key);

  /* now get the number of ranks (under the checkpoint) */
  char* ranks_key = scr_hash_elem_get_first_val(ckpt, SCR_META_KEY_RANKS);

  /* get the file size */
  char* file_size_key = scr_hash_elem_get_first_val(file, SCR_META_KEY_SIZE);

  /* get the file type */
  char* file_type_key = scr_hash_elem_get_first_val(file, SCR_META_KEY_TYPE);

  /* get the crc, if one is set */
  char* file_crc_key = scr_hash_elem_get_first_val(file, SCR_META_KEY_CRC);

  /* get the complete flag */
  char* file_complete_key = scr_hash_elem_get_first_val(file, SCR_META_KEY_COMPLETE);

  /* set the checkpoint id in the meta data structure */
  meta->checkpoint_id = -1;
  if (ckpt_key != NULL) {
    meta->checkpoint_id = atoi(ckpt_key);
  }

  /* set the number of ranks in the meta data structure */
  meta->ranks = 0;
  if (ranks_key != NULL) {
    meta->ranks = atoi(ranks_key);
  }

  /* set the rank in the meta data structure */
  meta->rank = -1;
  if (rank_key != NULL) {
    meta->rank = atoi(rank_key);
  }

  /* copy the file name to the meta data structure */
  strncpy(meta->filename, "", sizeof(meta->filename));
  if (file_key != NULL) {
    char file_tmp[SCR_MAX_FILENAME];
    strncpy(file_tmp, file_key, sizeof(file_tmp));
    strncpy(meta->filename, basename(file_tmp), sizeof(meta->filename));
  }

  /* set the file size in the meta data structure */
  meta->filesize = 0;
  if (file_size_key != NULL) {
    meta->filesize = strtoul(file_size_key, NULL, 0);
  }

  /* set the file type in the meta data structure */
  meta->filetype = SCR_FILE_UNKNOWN;
  if (file_type_key != NULL) {
    if (strcmp(file_type_key, "FULL") == 0) {
      meta->filetype = SCR_FILE_FULL;
    } else if (strcmp(file_type_key, "XOR") == 0) {
      meta->filetype = SCR_FILE_XOR;
    } else {
    }
  }

  /* set the crc32 values in the meta data structure */
  meta->crc32_computed = 0;
  meta->crc32 = 0UL;
  if (file_crc_key != NULL) {
    meta->crc32_computed = 1;
    meta->crc32 = strtoul(file_crc_key, NULL, 0);
  }

  /* set the complete flag in the meta data structure */
  meta->complete = 0;
  if (file_complete_key != NULL) {
    meta->complete = atoi(file_complete_key);
  }

  /* free the meta data hash */
  scr_hash_delete(hash);

  return SCR_SUCCESS;
}

/* creates corresponding .scr meta file for file to record completion info */
int scr_meta_write(const char* file_orig, const struct scr_meta* meta)
{
  int rc = SCR_SUCCESS;

  /* create the .scr extension */
  char file_meta[SCR_MAX_FILENAME];
  scr_meta_name(file_meta, file_orig);

  /* create a new hash to write meta data structure to */
  struct scr_hash* hash = scr_hash_new();

  /* write the checkpoint id */
  struct scr_hash* ckpt = scr_hash_set_kv_int(hash, SCR_META_KEY_CKPT, meta->checkpoint_id);

  /* write the number of ranks, under the checkpoint id */
  scr_hash_set_kv_int(ckpt, SCR_META_KEY_RANKS, meta->ranks);

  /* write the rank owning the file, under the checkpoint id */
  struct scr_hash* rank = scr_hash_set_kv_int(ckpt, SCR_META_KEY_RANK, meta->rank);

  /* write the filename, under the rank */
  struct scr_hash* file = scr_hash_set_kv(rank, SCR_META_KEY_FILE, meta->filename);

  /* write the file size for this file */
  scr_hash_setf(file, NULL, "%s %lu", SCR_META_KEY_SIZE, meta->filesize);

  /* if the crc value has been computed, write the crc for this file */
  if (meta->crc32_computed) {
    scr_hash_setf(file, NULL, "%s %#lx", SCR_META_KEY_CRC, meta->crc32);
  }

  /* write the file type */
  switch (meta->filetype) {
  case SCR_FILE_FULL:
    scr_hash_set_kv(file, SCR_META_KEY_TYPE, "FULL");
    break;
  case SCR_FILE_XOR:
    scr_hash_set_kv(file, SCR_META_KEY_TYPE, "XOR");
    break;
  default:
    scr_err("Unknown file type in meta data structure writing %s @ %s:%d",
            file_meta, __FILE__, __LINE__
    );
    rc = SCR_FAILURE;
  }

  /* write whether the file is complete */
  scr_hash_set_kv_int(file, SCR_META_KEY_COMPLETE, meta->complete);

  /* write the hash to disk */
  if (scr_hash_write(file_meta, hash) != SCR_SUCCESS) {
    scr_err("Opening meta file for write: scr_open(%s) @ %s:%d",
            file_meta, __FILE__, __LINE__
    );
    rc = SCR_FAILURE;
  }

  /* delete the hash */
  scr_hash_delete(hash);

  return rc;
}

/* TODO: this file isn't the most obvious location to place this function, but it uses crc and meta data */
/* compute crc32 for file and check value against meta data file, set it if not already set */
int scr_compute_crc(const char* file)
{
  /* check that we got a filename */
  if (file == NULL || strcmp(file, "") == 0) {
    return SCR_FAILURE;
  }

  /* read in the meta data for this file */
  struct scr_meta meta;
  if (scr_meta_read(file, &meta) != SCR_SUCCESS) {
    scr_err("Failed to read meta data file for file to compute CRC32: %s @ %s:%d",
            file, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* check that the file is complete */
  if (!meta.complete) {
    scr_err("File is marked as incomplete: %s",
            file, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* check that the filesize matches the value in the meta file */
  unsigned long size = scr_filesize(file);
  if (meta.filesize != size) {
    scr_err("File size does not match size recorded in meta file: %s",
            file, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* compute the CRC32 value for this file */
  uLong crc = crc32(0L, Z_NULL, 0);
  if (scr_crc32(file, &crc) != SCR_SUCCESS) {
    scr_err("Computing CRC32 for file %s @ %s:%d",
              file, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* now check the CRC32 value if it was set in the meta file, and set it if not */
  if (meta.crc32_computed) {
    /* the crc is already set in the meta file, let's check that we match */
    if (meta.crc32 != crc) {
      scr_err("CRC32 mismatch detected for file %s @ %s:%d",
              file, __FILE__, __LINE__
      );

      /* crc check failed, mark file as invalid */
      meta.complete = 0;
      scr_meta_write(file, &meta);

      return SCR_FAILURE;
    }
  } else {
    /* the crc was not set in the meta file, so let's set it now */
    meta.crc32_computed     = 1;
    meta.crc32              = crc;

    /* and update the meta file on disk */
    scr_meta_write(file, &meta);
  }

  return SCR_SUCCESS;
}
