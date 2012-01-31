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

/* Reads configuration file into hash */

#include "scr_conf.h"
#include "scr.h"
#include "scr_err.h"
#include "scr_io.h"
#include "scr_hash.h"
#include "scr_config.h"

#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <strings.h>

/* variable length args */
#include <stdarg.h>
#include <errno.h>

/* read parameters from config file and fill in hash */
int scr_config_read(const char* file, scr_hash* hash)
{
  int rc = scr_config_read_common(file, hash);
  return rc;
}
