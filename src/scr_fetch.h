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

#ifndef SCR_FETCH_H
#define SCR_FETCH_H

/* attempt to fetch most recent checkpoint from prefix directory into cache */
int scr_fetch_latest(scr_filemap* map, int* fetch_attempted);

/* fetch files from given dataset id and name from parallel file system,
 * return its checkpoint id */
int scr_fetch_dset(scr_cache_index* cindex, int dset_id, const char* dset_name, int* checkpoint_id);

#endif
