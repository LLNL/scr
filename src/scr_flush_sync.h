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

#ifndef SCR_FLUSH_SYNC_H
#define SCR_FLUSH_SYNC_H

#include "scr_filemap.h"

/* flush files from cache to parallel file system under SCR_PREFIX */
int scr_flush_sync(scr_filemap* map, int id);

#endif
