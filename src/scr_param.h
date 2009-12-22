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

#ifndef SCR_PARAM_H
#define SCR_PARAM_H

int scr_param_init();

int scr_param_finalize();

/* given a parameter name like SCR_FLUSH, return its value checking the following order:
 *   environment variable
 *   system config file
*/
char* scr_param_get(char* name);

#endif
