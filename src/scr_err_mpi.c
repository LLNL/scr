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

#include <stdlib.h>
#include <stdio.h>

/* variable length args */
#include <stdarg.h>
#include <errno.h>

#include "mpi.h"

#include "scr_conf.h"
#include "scr_globals.h"

/*
=========================================
Error and Debug Messages
=========================================
*/

/* print error message to stdout */
void scr_err(const char *fmt, ...)
{
  va_list argp;
  fprintf(stdout, "SCR %s ERROR: rank %d on %s: ", SCR_ERR_VERSION, scr_my_rank_world, scr_my_hostname);
  va_start(argp, fmt);
  vfprintf(stdout, fmt, argp);
  va_end(argp);
  fprintf(stdout, "\n");
}

/* print warning message to stdout */
void scr_warn(const char *fmt, ...)
{
  va_list argp;
  fprintf(stdout, "SCR %s WARNING: rank %d on %s: ", SCR_ERR_VERSION, scr_my_rank_world, scr_my_hostname);
  va_start(argp, fmt);
  vfprintf(stdout, fmt, argp);
  va_end(argp);
  fprintf(stdout, "\n");
}

/* print message to stdout if scr_debug is set and it is >= level */
void scr_dbg(int level, const char *fmt, ...)
{
  va_list argp;
  if (level == 0 || (scr_debug > 0 && scr_debug >= level)) {
    fprintf(stdout, "SCR %s: rank %d on %s: ", SCR_ERR_VERSION, scr_my_rank_world, scr_my_hostname);
    va_start(argp, fmt);
    vfprintf(stdout, fmt, argp);
    va_end(argp);
    fprintf(stdout, "\n");
  }
}

/* print abort message and call MPI_Abort to kill run */
void scr_abort(int rc, const char *fmt, ...)
{
  va_list argp;
  fprintf(stderr, "SCR %s ABORT: rank %d on %s: ", SCR_ERR_VERSION, scr_my_rank_world, scr_my_hostname);
  va_start(argp, fmt);
  vfprintf(stderr, fmt, argp);
  va_end(argp);
  fprintf(stderr, "\n");

  MPI_Abort(MPI_COMM_WORLD, 0);
}
