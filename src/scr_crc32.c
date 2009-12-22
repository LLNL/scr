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
  int fd = scr_open(filename, O_RDONLY);
  if (fd < 0) {
    scr_err("Failed to open file: scr_open(%s) errno=%d %m @ file %s:%d",
            filename, errno, __FILE__, __LINE__
    );
    return 1;
  }

  /* read the file data in and compute its crc32 */
  int nread = 0;
  char buf[buffer_size];
  uLong crc = crc32(0L, Z_NULL, 0);
  do {
    nread = scr_read(fd, buf, buffer_size);
    if (nread > 0) {
      crc = crc32(crc, (const Bytef*) buf, (uInt) nread);
    }
  } while (nread == buffer_size);

  /* if we got an error, don't print anything and bailout */
  if (nread < 0) {
    return 1;
  }

  /* close the file and print out its crc32 value */
  printf("%lx\n", (unsigned long) crc);
  close(fd);

  /* free off the string we strdup'ed at the start */
  free(filename);

  return 0;
}
