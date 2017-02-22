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

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
extern "C" {
#endif

/* Version information */
#define SCR_MAJOR_VERSION "1"
#define SCR_MINOR_VERSION "1"
#define SCR_PATCH_VERSION "8"
#define SCR_VERSION "v1.1.8"

/* constants returned from SCR functions for success and failure */
#define SCR_SUCCESS (0)

/* maximum characters in a filename returned by SCR */
#define SCR_MAX_FILENAME 1024

/* see the SCR user manual for full details on these functions */

/*****************
 * Init and finalize routines
 ****************/

/* initialize the SCR library */
int SCR_Init(void);

/* shut down the SCR library */
int SCR_Finalize(void);

/*****************
 * File registration
 ****************/

/* determine the path and filename to be used to open a file */
int SCR_Route_file(const char* name, char* file);

/*****************
 * Restart routines
 ****************/

/* determine whether SCR has a restart available to read */
int SCR_Have_restart(int* flag);

/* inform library that restart is starting */
int SCR_Start_restart(void);

/* inform library that the current restart is complete */
int SCR_Complete_restart(void);

/*****************
 * Checkpoint routines
 ****************/

/* determine whether a checkpoint should be taken at the current time */
int SCR_Need_checkpoint(int* flag);

/* inform library that a new checkpoint is starting */
int SCR_Start_checkpoint(void);

/* inform library that the current checkpoint is complete */
int SCR_Complete_checkpoint(int valid);

/*****************
 * Environment and configuration routines
 ****************/

/* get and return the SCR version */
char* SCR_Get_version(void);

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
} /* extern "C" */
#endif
