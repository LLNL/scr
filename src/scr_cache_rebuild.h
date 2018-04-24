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

#ifndef SCR_CACHE_REBUILD_H
#define SCR_CACHE_REBUILD_H

#include "scr_cache_index.h"

/* distribute and rebuild files in cache */
int scr_cache_rebuild(scr_cache_index* cindex);

/* remove any dataset ids from flush file which are not in cache,
 * and add any datasets in cache that are not in the flush file */
int scr_flush_file_rebuild(const scr_cache_index* cindenx);

#endif
