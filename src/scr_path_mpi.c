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

#include "mpi.h"

#include "scr.h"
#include "scr_err.h"
#include "scr_path.h"
#include "scr_util.h"

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>

#include <stdint.h>

/*
=========================================
Functions to send/recv paths with MPI
=========================================
*/

/* broacast path from root to all ranks in comm,
 * receivers must pass in a newly allocated path from scr_path_new() */
int scr_path_bcast(scr_path* path, int root, MPI_Comm comm)
{
  /* if pointer is NULL, throw an error */
  if (path == NULL) {
    scr_abort(-1, "NULL pointer passed for path @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* lookup our rank in comm */
  int rank;
  MPI_Comm_rank(comm, &rank);

  /* determine number of bytes to send */
  int bytes;
  int components = scr_path_components(path);
  if (rank == root) {
    if (components > 0) {
      /* figure out string length of path (including terminating NULL) */
      bytes = scr_path_strlen(path) + 1;
    } else {
      /* we use 0 bytes to denote a NULL path,
       * since even an empty string contains at least one byte */
      bytes = 0;
    }
  } else {
    /* as a receiver, verify that we were given an empty path */
    if (components > 0) {
      scr_abort(-1, "Non-null path passed as input in receiver to bcast path @ %s:%d",
        __FILE__, __LINE__
      );
    }
  }

  /* broadcast number of bytes in path */
  MPI_Bcast(&bytes, 1, MPI_INT, root, comm);

  /* if path is NULL, we're done */
  if (bytes == 0) {
    return SCR_SUCCESS;
  }

  /* otherwise, allocate bytes to receive str */
  char* str;
  if (rank == root) {
    /* the root converts the path to a string */
    str = scr_path_strdup(path);
  } else {
    /* non-root processes need to allocate an array */
    str = (char*) malloc((size_t)bytes);
  }
  if (str == NULL) {
    scr_abort(-1, "Failed to allocate memory to bcast path @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* broadcast the string */
  MPI_Bcast(str, bytes, MPI_CHAR, root, comm);

  /* if we're not the rank, append the string to our path */
  if (rank != root) {
    scr_path_append_str(path, str);
  }

  /* free string */
  scr_free(&str);

  return SCR_SUCCESS;
}
