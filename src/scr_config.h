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

#ifndef SCR_CONFIG_H
#define SCR_CONFIG_H

#include "scr_hash.h"

int scr_config_read_common(const char* file, scr_hash* hash);

int scr_config_read(const char* file, scr_hash* hash);

#endif
