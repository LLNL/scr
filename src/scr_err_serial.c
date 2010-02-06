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

/* This implements the scr_err.h interface, but for serial jobs,
 * like the SCR utilities. */

#include "scr_err.h"

#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>

/* variable length args */
#include <stdarg.h>
#include <errno.h>

/*
=========================================
Error and Debug Messages
=========================================
*/

/* print message to stderr */
void scr_err(const char *fmt, ...)
{
  /* get my hostname */
  char hostname[256];
  if (gethostname(hostname, sizeof(hostname)) != 0) {
    /* TODO: error! */
  }

  va_list argp;
  fprintf(stderr, "SCR ERROR: %s: ", hostname);
  va_start(argp, fmt);
  vfprintf(stderr, fmt, argp);
  va_end(argp);
  fprintf(stderr, "\n");
}

/* print message to stdout if scr_debug is set and it is >= level */
void scr_dbg(int level, const char *fmt, ...)
{
  /* get my hostname */
  char hostname[256];
  if (gethostname(hostname, sizeof(hostname)) != 0) {
    /* TODO: error! */
  }

  va_list argp;
  /*
  if (level == 0 || (scr_debug > 0 && scr_debug >= level)) {
  */
    fprintf(stdout, "SCR: %s: ", hostname);
    va_start(argp, fmt);
    vfprintf(stdout, fmt, argp);
    va_end(argp);
    fprintf(stdout, "\n");
  /*
  }
  */
}

/* print abort message and kill run */
void scr_abort(int rc, const char *fmt, ...)
{
  /* get my hostname */
  char hostname[256];
  if (gethostname(hostname, sizeof(hostname)) != 0) {
    /* TODO: error! */
  }

  va_list argp;
  fprintf(stderr, "SCR ABORT: %s: ", hostname);
  va_start(argp, fmt);
  vfprintf(stderr, fmt, argp);
  va_end(argp);
  fprintf(stderr, "\n");

  exit(rc);
}
