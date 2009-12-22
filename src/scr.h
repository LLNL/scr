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

#define SCR_SUCCESS (0)
#define SCR_FAILURE (1)

#define SCR_MAX_FILENAME 1024

/*
==========================
USER INTERFACE FUNCTIONS
==========================
*/

int SCR_Init();
int SCR_Finalize();

/* v1.1 API */
int SCR_Need_checkpoint(int* flag);
int SCR_Start_checkpoint();
int SCR_Route_file(const char* name, char* file);
int SCR_Complete_checkpoint(int valid);
