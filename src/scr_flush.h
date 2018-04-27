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
#include "scr_filemap.h"

/* given a dataset, return a newly allocated string specifying the
 * metadata directory for that dataset, must be freed by caller */
char* scr_flush_dataset_metadir(const scr_dataset* dataset);

/* given a filemap and a dataset id, prepare and return a list of files to be flushed,
 * also create corresponding directories and container files */
int scr_flush_prepare(const scr_filemap* map, int id, kvtree* file_list);

/* given a dataset id that has been flushed, the list provided by scr_flush_prepare,
 * and data to include in the summary file, complete the flush by writing the summary file */
int scr_flush_complete(int id, kvtree* file_list, kvtree* data);

#endif
