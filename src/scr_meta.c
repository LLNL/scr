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
  char file[SCR_MAX_FILENAME];
  scr_meta_name(file, file_orig);

  /* can't read file, return error */
  if (access(file, R_OK) < 0) { return SCR_FAILURE; }

  FILE* fs = fopen(file, "r");
  if (fs == NULL) {
    scr_err("Opening meta file for read: fopen(%s, \"r\") errno=%d %m @ %s:%d",
            file, errno, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* read field / value pairs from file */
  char field[SCR_MAX_FILENAME];
  char value[SCR_MAX_FILENAME];
  int n;
  do {
    n = fscanf(fs, "%s %s\n", field, value);
    if (n != EOF) {
      if (strcmp(field, "Rank:") == 0)         { meta->rank  = atoi(value); }
      if (strcmp(field, "Ranks:") == 0)        { meta->ranks = atoi(value); }
      if (strcmp(field, "CheckpointID:") == 0) { meta->checkpoint_id = atoi(value); }
      if (strcmp(field, "Filetype:") == 0)     { meta->filetype = atoi(value); }

      /* we take the basename of filename for backwards compatibility */
      if (strcmp(field, "Filename:") == 0)     { strcpy(meta->filename, basename(value)); }
      if (strcmp(field, "Filesize:") == 0)     { meta->filesize = strtoul(value, NULL, 0); }
      if (strcmp(field, "Complete:") == 0)     { meta->complete = atoi(value); }
      if (strcmp(field, "CRC32Computed:") == 0){ meta->crc32_computed = atoi(value); }
      if (strcmp(field, "CRC32:") == 0)        { meta->crc32 = strtoul(value, NULL, 0); }
    }
  } while (n != EOF);

  fclose(fs);

  return SCR_SUCCESS;
}

/* creates corresponding .scr meta file for file to record completion info */
int scr_meta_write(const char* file, const struct scr_meta* meta)
{
  /* create the .scr extension */
  char file_scr[SCR_MAX_FILENAME];
  scr_meta_name(file_scr, file);

  /* write out the meta data */
  char buf[SCR_MAX_FILENAME];
  int fd = scr_open(file_scr, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
  if (fd < 0) {
    scr_err("Opening meta file for write: scr_open(%s) errno=%d %m @ %s:%d",
            file_scr, errno, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* task id */
  sprintf(buf, "Rank: %d\n", meta->rank);
  scr_write(fd, buf, strlen(buf));

  /* number of tasks */
  sprintf(buf, "Ranks: %d\n", meta->ranks);
  scr_write(fd, buf, strlen(buf));

  /* checkpoint id */
  sprintf(buf, "CheckpointID: %d\n", meta->checkpoint_id);
  scr_write(fd, buf, strlen(buf));

  /* what type of file this is */
  sprintf(buf, "Filetype: %d\n", meta->filetype);
  scr_write(fd, buf, strlen(buf));

  /* filename */
  sprintf(buf, "Filename: %s\n", meta->filename);
  scr_write(fd, buf, strlen(buf));

  /* filesize in bytes */
  sprintf(buf, "Filesize: %ld\n", meta->filesize);
  scr_write(fd, buf, strlen(buf));

  /* whether this file is complete */
  sprintf(buf, "Complete: %d\n", meta->complete);
  scr_write(fd, buf, strlen(buf));

  /* whether a crc32 was computed for thie file */
  sprintf(buf, "CRC32Computed: %d\n", meta->crc32_computed);
  scr_write(fd, buf, strlen(buf));

  /* crc32 value for this file */
  sprintf(buf, "CRC32: 0x%lx\n", meta->crc32);
  scr_write(fd, buf, strlen(buf));

  /* flush and close the file */
  scr_close(file_scr, fd);

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
