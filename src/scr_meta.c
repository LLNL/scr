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
void scr_set_meta(struct scr_meta* meta, const char* file, int rank, int ranks, int checkpoint_id, int filetype, int complete)
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

    /* just duplicate the info as the source for now */
    strcpy(meta->src_filename, meta->filename);
    meta->src_filesize       = meta->filesize;
    meta->src_complete       = meta->complete;
    meta->src_crc32_computed = 0;
    meta->src_crc32          = crc32(0L, Z_NULL, 0);
}

/* initialize meta structure to represent file, filetype, and complete */
void scr_copy_meta(struct scr_meta* m1, const struct scr_meta* m2)
{
    memcpy(m1, m2, sizeof(struct scr_meta));
}

/* read meta for file_orig and fill in meta structure */
int scr_read_meta(const char* file_orig, struct scr_meta* meta)
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

      /* we take the basename of filename for backwards compatibility */
      if (strcmp(field, "SRCFilename:") == 0)  { strcpy(meta->src_filename, basename(value)); }
      if (strcmp(field, "SRCFilesize:") == 0)  { meta->src_filesize = strtoul(value, NULL, 0); }
      if (strcmp(field, "SRCComplete:") == 0)  { meta->src_complete = atoi(value); }
      if (strcmp(field, "SRCCRC32Computed:") == 0){ meta->src_crc32_computed = atoi(value); }
      if (strcmp(field, "SRCCRC32:") == 0)        { meta->src_crc32 = strtoul(value, NULL, 0); }
    }
  } while (n != EOF);

  fclose(fs);

  return SCR_SUCCESS;
}

/* creates corresponding .scr meta file for file to record completion info */
int scr_write_meta(const char* file, const struct scr_meta* meta)
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

  /* filename of the source file */
  sprintf(buf, "SRCFilename: %s\n", meta->src_filename);
  scr_write(fd, buf, strlen(buf));

  /* filesize of the source file */
  sprintf(buf, "SRCFilesize: %lu\n", meta->src_filesize);
  scr_write(fd, buf, strlen(buf));

  /* whether source file is complete */
  sprintf(buf, "SRCComplete: %d\n", meta->src_complete);
  scr_write(fd, buf, strlen(buf));

  /* whether a crc32 was computed for the source file */
  sprintf(buf, "SRCCRC32Computed: %d\n", meta->src_crc32_computed);
  scr_write(fd, buf, strlen(buf));

  /* crc32 value for the soruce file */
  sprintf(buf, "SRCCRC32: 0x%lx\n", meta->src_crc32);
  scr_write(fd, buf, strlen(buf));

  /* flush and close the file */
  scr_close(fd);

  return SCR_SUCCESS;
}
