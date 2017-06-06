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

#ifndef SCR_ENV_H
#define SCR_ENV_H

/*
=========================================
This file contains functions that read / write
machine-dependent information.
=========================================
*/

/* returns the number of seconds remaining in the time allocation */
long int scr_env_seconds_remaining(void);

/* allocate and return a string containing the current username */
char* scr_env_username(void);

/* allocate and return a string containing the current job id */
char* scr_env_jobid(void);

/* allocate and return a string containing the node name */
char* scr_env_nodename(void);

/* read cluster name of current job */
char* scr_env_cluster(void);

/* setup/teardown of environment services */
int scr_env_init(void);
int scr_env_finailize(void);

#endif
