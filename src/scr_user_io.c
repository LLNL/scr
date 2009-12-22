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

/* The ultimate interface would include SCR_Open/Read/Write/Close and
 * SCR would implement its own file system interface, which would enable
 * SCR to store data in memory rather than a file system. 
 * This code is a placeholder for that interface. */

#include <stdlib.h>
#include <stdio.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <strings.h>

/* variable length args */
#include <stdarg.h>
#include <errno.h>

/* gethostbyname */
#include <netdb.h>

/* basename/dirname */
#include <unistd.h>
#include <libgen.h>

/* gettimeofday */
#include <sys/time.h>

/* localtime, asctime */
#include <time.h>

/* compute crc32 */
#include <zlib.h>

#include "mpi.h"
#include "scr.h"
#include "scr_io.h"

/* -----------------
Open and Close
----------------- */

/* user asks to open file in its normal directory location
   but reroute placement of temporary file under the covers */
static int SCR_Open(const char* file, int flags, mode_t mode)
{
  int fd;

  /* if not enabled, we still need to open file */
  if (! scr_enabled) {
    fd = open(file, flags, mode);
    return fd;
  }

  /* bail out if not initialized -- will get bad results */
  if (! scr_initialized) { scr_err("SCR has not been initialized @ %s:%d", __FILE__, __LINE__); return SCR_FAILURE; }

  scr_dbg(2, "Opening %s in place of %s", temp, file);

  // open the temp file
  fd = scr_open(file, flags, mode);

  return fd;
}

// TODO: Could lookup filename based on filedescriptor
// close out the checkpoint file and complete it
static int SCR_Close(int fd)
{
  int rc = SCR_SUCCESS;

  /* bail out if not initialized -- will get bad results */
  if (! scr_initialized) { scr_err("SCR has not been initialized @ %s:%d", __FILE__, __LINE__); return SCR_FAILURE; }

  rc = scr_close(fd);

  return rc;
}

static int SCR_Read(int fd, void* buf, size_t size)
{
  int rc = SCR_SUCCESS;

  /* bail out if not initialized -- will get bad results */
  if (! scr_initialized) { scr_err("SCR has not been initialized @ %s:%d", __FILE__, __LINE__); return SCR_FAILURE; }

  rc = scr_read(fd, buf, size);

  return rc;
}

static int SCR_Write(int fd, const void* buf, size_t size)
{
  int rc = SCR_SUCCESS;

  /* bail out if not initialized -- will get bad results */
  if (! scr_initialized) { scr_err("SCR has not been initialized @ %s:%d", __FILE__, __LINE__); return SCR_FAILURE; }

  rc = scr_write(fd, buf, size);

  return rc;
}

static int SCR_Seek(int fd, size_t offset)
{
  int rc = SCR_SUCCESS;

  /* bail out if not initialized -- will get bad results */
  if (! scr_initialized) { scr_err("SCR has not been initialized @ %s:%d", __FILE__, __LINE__); return SCR_FAILURE; }

  rc = scr_seek(fd, offset);

  return rc;
}
