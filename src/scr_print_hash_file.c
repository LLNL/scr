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

/* Utility to pretty print a hash file to the screen. */

#include "scr.h"
#include "scr_io.h"
#include "scr_err.h"
#include "scr_util.h"
#include "scr_hash.h"

#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>

int main(int argc, char* argv[])
{
  int rc = 0;

  /* check that we were given exactly one filename argument */
  if (argc != 2) {
    printf("Usage: scr_print_hash_file <hashfile>\n");
    return 1;
  }

  /* read in the filename */
  char* filename = strdup(argv[1]);
  if (filename != NULL) {
    /* get a hash to read in the file */
    scr_hash* hash = scr_hash_new();

    /* read in the file */
    if (scr_hash_read(filename, hash) == SCR_SUCCESS) {
      /* we read the file, now print it out */
      scr_hash_print(hash, 0);
    } else {
      scr_err("Could not read file %s @ %s:%d",
              filename, __FILE__, __LINE__
      );
      rc = 1;
    }

    /* free the hash object */
    scr_hash_delete(&hash);

    /* free our strdup'd filename */
    scr_free(&filename);
  } else {
    scr_err("Could not copy filename from command line @ %s:%d",
            __FILE__, __LINE__
    );
    rc = 1;
  }

  return rc;
}
