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

int scr_container_get_name_size_offset_length(
  const scr_hash* segment, const scr_hash* containers,
  char** name, unsigned long* size, unsigned long* offset, unsigned long* length
);

/* attempt to fetch most recent checkpoint from prefix directory into cache */
int scr_fetch_sync(scr_filemap* map, int* fetch_attempted);

#endif
