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

/* Defines a data structure that keeps track of the number
 * and the names of the files a process writes out in a given
 * dataset. */

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>

#include "mpi.h"

#include "scr_globals.h"
#include "scr.h"
#include "scr_io.h"
#include "scr_err.h"
#include "scr_util.h"
#include "scr_cache_index.h"

#include "spath.h"
#include "kvtree.h"
#include "kvtree_util.h"

/* reads specified file and fills in cache index structure */
int scr_cache_index_read(const spath* path_file, scr_cache_index* cindex)
{
  /* check that we have a cindex pointer and a hash within the cindex */
  if (cindex == NULL) {
    return SCR_FAILURE;
  }

  /* assume we'll fail */
  int rc = SCR_FAILURE;

  /* can't read file, return error (special case so as not to print error message below) */
  /* get file name */
  char* file = spath_strdup(path_file);

  /* attempt to read the file */
  if (scr_file_is_readable(file) == SCR_SUCCESS) {
    /* ok, now try to read the file */
    if (kvtree_read_file(file, cindex) == KVTREE_SUCCESS) {
      /* successfully read the cache index file */
      rc = SCR_SUCCESS;
    } else {
      scr_err("Reading cache index %s @ %s:%d",
        file, __FILE__, __LINE__
      );
    }
  }

  /* free file name string */
  scr_free(&file);

  return rc;
}

/* writes given cache index to specified file */
int scr_cache_index_write(const spath* file, const scr_cache_index* cindex)
{
  /* check that we have a cindex pointer */
  if (cindex == NULL) {
    return SCR_FAILURE;
  }

  /* write out the hash */
  if (kvtree_write_path(file, cindex) != KVTREE_SUCCESS) {
    char path_err[SCR_MAX_FILENAME];
    spath_strcpy(path_err, sizeof(path_err), file);
    scr_err("Writing cache index %s @ %s:%d",
      path_err, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  return SCR_SUCCESS;
}
