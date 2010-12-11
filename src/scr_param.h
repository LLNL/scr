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

#include "scr_hash.h"

/* This will search a series of locations to find a given parameter name:
 *   environment variable
 *   user config file
 *   system config file
 *
 * It uses reference counting such that multiple callers may init and
 * finalize the parameters independently of one another.
*/

/* read config files and store contents */
int scr_param_init();

/* free contents from config files */
int scr_param_finalize();

/* searchs for name and returns a character pointer to its value if set,
 * returns NULL if not found */
char* scr_param_get(char* name);

/* searchs for name and returns a newly allocated hash of its value if set,
 * returns NULL if not found */
scr_hash* scr_param_get_hash(char* name);

#endif
