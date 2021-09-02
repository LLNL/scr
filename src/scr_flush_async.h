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
int scr_flush_async_stop(void);

/* returns 1 if any async flush is ongoing, 0 otherwise */
int scr_flush_async_in_progress(void);

/* returns 1 if any id is in async list, 0 otherwise */
int scr_flush_async_in_list(int id);

/* start an asynchronous flush from cache to parallel file system under SCR_PREFIX */
int scr_flush_async_start(scr_cache_index* cindex, int id);

/* check whether the flush from cache to parallel file system has completed */
int scr_flush_async_test(scr_cache_index* cindex, int id);

/* complete the flush from cache to parallel file system */
int scr_flush_async_complete(scr_cache_index* cindex, int id);

/* wait until the dataset currently being flushed completes */
int scr_flush_async_wait(scr_cache_index* cindex, int id);

/* wait until all datasets currently being flushed complete */
int scr_flush_async_waitall(scr_cache_index* cindex);

/* progress each dataset in turn until all are complete,
 * or we find the first that is still going */
int scr_flush_async_progall(scr_cache_index* cindex);

/* get ordered list of ids being flushed,
 * caller is responsible for freeing ids with scr_free */
int scr_flush_async_get_list(scr_cache_index* cindex, int* num, int** ids);

/* initialize the async transfer processes */
int scr_flush_async_init(void);

/* finalize the async transfer processes */
int scr_flush_async_finalize(void);

#endif /* SCR_FLUSH_ASYNC_H */
