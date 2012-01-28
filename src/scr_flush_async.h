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

#ifndef SCR_FLUSH_ASYNC_H
#define SCR_FLUSH_ASYNC_H

#include "scr_filemap.h"

/* stop all ongoing asynchronous flush operations */
int scr_flush_async_stop();

/* start an asynchronous flush from cache to parallel file system under SCR_PREFIX */
int scr_flush_async_start(scr_filemap* map, int id);

/* check whether the flush from cache to parallel file system has completed */
int scr_flush_async_test(scr_filemap* map, int id, double* bytes);

/* complete the flush from cache to parallel file system */
int scr_flush_async_complete(scr_filemap* map, int id);

/* wait until the checkpoint currently being flushed completes */
int scr_flush_async_wait(scr_filemap* map);

#endif
