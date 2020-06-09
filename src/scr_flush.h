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

#include "kvtree.h"
#include "scr_cache_index.h"

/* given file list from flush_prepare,
 * allocate and fill in lists of source and destination file paths,
 * caller should free arrays with call to list_free*/
int scr_flush_list_alloc(
  const kvtree* file_list,
  int* out_num_files,
  char*** out_src_filelist,
  char*** out_dst_filelist
);

/* free list allocated in list_alloc */
int scr_flush_list_free(
  int num_files,
  char*** ptr_src_filelist,
  char*** ptr_dst_filelist
);

/* create directories from basepath down to each file as needed */
int scr_flush_create_dirs(
  const char* basepath,       /* top-level directory, assumed to exist */
  int count,                  /* number of files */
  const char** dest_filelist, /* list of files */
  MPI_Comm comm               /* communicator of participating processes */
);

/* given a dataset, return a newly allocated string specifying the
 * metadata directory for that dataset, must be freed by caller */
char* scr_flush_dataset_metadir(const scr_dataset* dataset);

/* create entry in scr index file to indicate that a new dataset
 * has been started to be copied to the prefix directory, but mark
 * it as incomplete */
int scr_flush_init_index(scr_dataset* dataset);

/* given a cache index and a dataset id, prepare and return a list of files to be flushed */
int scr_flush_prepare(const scr_cache_index* cindex, int id, kvtree* file_list);

/* given a dataset id that has been flushed and the list provided by scr_flush_prepare,
 * complete the flush by writing the summary file */
int scr_flush_complete(const scr_cache_index* cindex, int id, kvtree* file_list);

#endif
