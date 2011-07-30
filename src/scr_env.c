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

#include "scr_env.h"
#include "scr_err.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#ifdef HAVE_LIBYOGRT
#include "yogrt.h"
#endif /* HAVE_LIBYOGRT */

/*
=========================================
This file contains functions that read / write
machine-dependent information.
=========================================
*/

/* returns the number of seconds remaining in the time allocation */
int scr_env_seconds_remaining()
{
  /* returning a negative number tells the caller this functionality is disabled */
  int secs = -1;

  /* call libyogrt if we have it */
  #ifdef HAVE_LIBYOGRT
    secs = yogrt_remaining();
    if (secs < 0) {
      secs = 0;
    }
  #endif /* HAVE_LIBYOGRT */

  return secs;
}

/* allocate and return a string containing the current username */
char* scr_env_username()
{
  char* name = NULL;

  /* read $USER environment variable for username */
  char* value;
  if ((value = getenv("USER")) != NULL) {
    name = strdup(value);
    if (name == NULL) {
      scr_err("Failed to allocate memory to record username (%s) @ %s:%d",
              value, __FILE__, __LINE__
      );
    }
  }

  return name;
}

/* allocate and return a string containing the current job id */
char* scr_env_jobid()
{
  char* jobid = NULL;

  /* read $SLURM_JOBID environment variable for jobid string */
  char* value;
  if ((value = getenv("SLURM_JOBID")) != NULL) {
    jobid = strdup(value);
    if (jobid == NULL) {
      scr_err("Failed to allocate memory to record jobid (%s) @ %s:%d",
              value, __FILE__, __LINE__
      );
    }
  }

  return jobid;
}

/* allocate and return a string containing the current cluster name */
char* scr_env_cluster()
{
  char* name = NULL;

  /* TODO: compute name of cluster */

  return name;
}
