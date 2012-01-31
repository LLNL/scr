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

/* Utility to compute the crc32 value of a file.
 * Given a filename as a command line argument,
 * compute and print out that file's crc32 value. */

#include "scr.h"
#include "scr_io.h"
#include "scr_err.h"
#include "scr_util.h"

#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>

/* compute crc32 */
#include <zlib.h>

int buffer_size = 128*1024;

int main(int argc, char* argv[])
{
  /* TODO: need to check that we got one and only one parameter */

  /* read in the filename */
  char* filename = strdup(argv[1]);

  /* open the file for reading */
  uLong crc = crc32(0L, Z_NULL, 0);
  if (scr_crc32(filename, &crc) != SCR_SUCCESS) {
    scr_err("Failed to compute CRC32 for file %s @ file %s:%d",
            filename, __FILE__, __LINE__
    );
    return 1;
  }

  /* print out the crc32 value */
  printf("%lx\n", (unsigned long) crc);

  /* free off the string we strdup'ed at the start */
  scr_free(&filename);

  return 0;
}
