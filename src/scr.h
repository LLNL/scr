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

/* constants returned from SCR functions for success and failure */
#define SCR_SUCCESS (0)

/* DO NOT USE THIS CONSTANT, test via (!= SCR_SUCCESS) */
#define SCR_FAILURE (1)

/* maximum characters in a filename returned by SCR */
#define SCR_MAX_FILENAME 1024

/* see the SCR user manual for full details on these functions */

/* initialize the SCR library */
int SCR_Init();

/* shut down the SCR library */
int SCR_Finalize();

/* determine whether a checkpoint should be taken at the current point in time */
int SCR_Need_checkpoint(int* flag);

/* inform library that a new checkpoint is starting */
int SCR_Start_checkpoint();

/* determine the path and filename that should be used to open a file */
int SCR_Route_file(const char* name, char* file);

/* inform library that the current checkpoint is complete */
int SCR_Complete_checkpoint(int valid);

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
} /* extern "C" */
#endif
