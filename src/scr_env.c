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

#include <config.h>

#include "scr_env.h"
#include "scr_err.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#ifdef HAVE_LIBYOGRT
#include "yogrt.h"
#endif /* HAVE_LIBYOGRT */

#if (SCR_MACHINE_TYPE == SCR_TLCC) || (SCR_MACHINE_TYPE == SCR_CRAY_XT)
#include <unistd.h> /* gethostname */
#endif

#if SCR_MACHINE_TYPE == SCR_BGQ
#include "firmware/include/personality.h" /* Personality_t */
#include "spi/include/kernel/location.h"  /* Kernel_GetPersonality */
#include "hwi/include/common/uci.h"       /* bg_decodeComputeCardCoreOnNodeBoardUCI */
#endif

/*
=========================================
This file contains functions that read / write
machine-dependent information.
=========================================
*/

/* returns the number of seconds remaining in the time allocation */
long int scr_env_seconds_remaining()
{
  /* returning a negative number tells the caller this functionality is disabled */
  long int secs = -1;

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

  char* value;
  #if (SCR_MACHINE_TYPE == SCR_TLCC) || (SCR_MACHINE_TYPE == SCR_BGQ)
    /* read $SLURM_JOBID environment variable for jobid string */
    if ((value = getenv("SLURM_JOBID")) != NULL) {
      jobid = strdup(value);
      if (jobid == NULL) {
        scr_err("Failed to allocate memory to record jobid (%s) @ %s:%d",
                value, __FILE__, __LINE__
        );
      }
    }
  #elif SCR_MACHINE_TYPE == SCR_CRAY_XT
    /* read $PBS_JOBID environment variable for jobid string */
    if ((value = getenv("PBS_JOBID")) != NULL) {
      jobid = strdup(value);
      if (jobid == NULL) {
        scr_err("Failed to allocate memory to record jobid (%s) @ %s:%d",
                value, __FILE__, __LINE__
        );
      }
    }
  #endif

  return jobid;
}

/* allocate and return a string containing the node name */
char* scr_env_nodename()
{
  char* name = NULL;

  #if (SCR_MACHINE_TYPE == SCR_TLCC) || (SCR_MACHINE_TYPE == SCR_CRAY_XT)
    /* we just use the string returned by gethostname */
    char hostname[256];
    if (gethostname(hostname, sizeof(hostname)) == 0) {
      name = strdup(hostname);
    } else {
      scr_err("Call to gethostname failed @ %s:%d",
        __FILE__, __LINE__
      );
    }
  #elif SCR_MACHINE_TYPE == SCR_BGQ
    /* here, we derive a string from the personality */
    Personality_t personality;
    unsigned int x, y, m, n, j, c;
    Kernel_GetPersonality(&personality, sizeof(personality));
    bg_decodeComputeCardCoreOnNodeBoardUCI(personality.Kernel_Config.UCI,&x,&y,&m,&n,&j,&c);

    /* construct the hostname */
    char hostname[256];
    int num = snprintf(
      hostname, sizeof(hostname),
      "R%X%X-M%d-N%02d-J%02d-A%dof%d-B%dof%d-C%dof%d-D%dof%d-E%dof%d",
      x, y, m, n, j,
      personality.Network_Config.Acoord, personality.Network_Config.Anodes,
      personality.Network_Config.Bcoord, personality.Network_Config.Bnodes,
      personality.Network_Config.Ccoord, personality.Network_Config.Cnodes,
      personality.Network_Config.Dcoord, personality.Network_Config.Dnodes,
      personality.Network_Config.Ecoord, personality.Network_Config.Enodes
    );

    /* check that we constructed the hostname correctly */
    if (num < 0) {
      /* error */
      scr_err("Error calling snprintf when building hostname rc=%d @ %s:%d",
        num, __FILE__, __LINE__
      );
    } else if (num >= sizeof(hostname)) {
      /* name was truncated */
      scr_err("Temporary buffer of %d bytes too small to construct hostname, needed %d bytes @ %s:%d",
        sizeof(hostname), num, __FILE__, __LINE__
      );
    } else {
      /* duplicate the name */
      name = strdup(hostname);
    }
  #endif

  return name;
}

/* allocate and return a string containing the current cluster name */
char* scr_env_cluster()
{
  char* name = NULL;

  /* TODO: compute name of cluster */

  return name;
}
