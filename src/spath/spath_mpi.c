#include "mpi.h"

#include "spath.h"
#include "spath_util.h"

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
 * receivers must pass in a newly allocated path from spath_new() */
int spath_bcast(spath* path, int root, MPI_Comm comm)
{
  /* if pointer is NULL, return an error */
  if (path == NULL) {
    fprintf(stderr, "NULL pointer passed for path @ %s:%d\n",
      __FILE__, __LINE__
    );
    return SPATH_FAILURE; 
  }

  /* lookup our rank in comm */
  int rank;
  MPI_Comm_rank(comm, &rank);

  /* determine number of bytes to send */
  int bytes;
  int components = spath_components(path);
  if (rank == root) {
    if (components > 0) {
      /* figure out string length of path (including terminating NULL) */
      bytes = spath_strlen(path) + 1;
    } else {
      /* we use 0 bytes to denote a NULL path,
       * since even an empty string contains at least one byte */
      bytes = 0;
    }
  } else {
    /* as a receiver, verify that we were given an empty path */
    if (components > 0) {
      fprintf(stderr, "Non-null path passed as input in receiver to bcast path @ %s:%d",
        __FILE__, __LINE__
      );
      MPI_Abort(comm, -1);
    }
  }

  /* broadcast number of bytes in path */
  MPI_Bcast(&bytes, 1, MPI_INT, root, comm);

  /* if path is NULL, we're done */
  if (bytes == 0) {
    return SPATH_SUCCESS;
  }

  /* otherwise, allocate bytes to receive str */
  char* str;
  if (rank == root) {
    /* the root converts the path to a string */
    str = spath_strdup(path);
  } else {
    /* non-root processes need to allocate an array */
    str = (char*) SPATH_MALLOC((size_t)bytes);
  }
  if (str == NULL) {
    fprintf(stderr, "Failed to allocate memory to bcast path @ %s:%d",
      __FILE__, __LINE__
    );
    MPI_Abort(comm, -1);
  }

  /* broadcast the string */
  MPI_Bcast(str, bytes, MPI_CHAR, root, comm);

  /* if we're not the rank, append the string to our path */
  if (rank != root) {
    spath_append_str(path, str);
  }

  /* free string */
  spath_free(&str);

  return SPATH_SUCCESS;
}
