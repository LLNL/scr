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

/* compute crc32, needed for uLong */
#include <zlib.h>

/* file types */
#define SCR_FILE_FULL (0)
#define SCR_FILE_XOR  (2)

/*
=========================================
Metadata functions
=========================================
*/

/* data structure for meta file */
struct scr_meta
{
  int rank;
  int ranks;
  int checkpoint_id;
  int filetype;

  char filename[SCR_MAX_FILENAME];
  unsigned long filesize;
  int complete;
  int crc32_computed;
  uLong crc32;
};

/* build meta data filename for input file */
int scr_meta_name(char* metaname, const char* file);

/* initialize meta structure to represent file, filetype, and complete */
void scr_meta_set(struct scr_meta* meta, const char* file, int rank, int ranks, int checkpoint_id, int filetype, int complete);

/* initialize meta structure to represent file, filetype, and complete */
void scr_meta_copy(struct scr_meta* m1, const struct scr_meta* m2);

/* read meta for file_orig and fill in meta structure */
int scr_meta_read(const char* file_orig, struct scr_meta* meta);

/* creates corresponding .scr meta file for file to record completion info */
int scr_meta_write(const char* file, const struct scr_meta* meta);

/* compute crc32 for file and check value against meta data file, set it if not already set */
int scr_compute_crc(const char* file);

#endif
