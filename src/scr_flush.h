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

#ifndef SCR_FLUSH_H
#define SCR_FLUSH_H

#include "scr_hash.h"
#include "scr_filemap.h"

int scr_dataset_build_name(int id, int64_t usecs, char* name, int n);

/* this is a hacky way to record data in flush file from complete checkpoint */
int scr_flush_verify(
  const scr_filemap* map, /* IN  - current filemap */
  int id,                 /* IN  - id of dataset to be flushed */
  char* dir,              /* OUT - buffer to hold subdirectory name */
  size_t dir_size         /* IN  - size of buffer in bytes */
);

/* given a filemap and a dataset id, prepare and return a list of files to be flushed,
 * also create corresponding directories and container files */
int scr_flush_prepare(const scr_filemap* map, int id, scr_hash* file_list);

/* given a dataset id that has been flushed, the list provided by scr_flush_prepare,
 * and data to include in the summary file, complete the flush by writing the summary file */
int scr_flush_complete(int id, scr_hash* file_list, scr_hash* data);

#endif
