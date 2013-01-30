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

#ifndef SCR_CONF_H
#define SCR_CONF_H

/* read in the config.h to pick up any parameters set from configure 
 * these values will override any settings below */
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

/* =========================================================================
 * Defines for checkpoint copy type later below, don't change this defines.
 * ========================================================================= */

/* redundancy shemes: enum as powers of two for binary and/or operations,
 * don't change these */
#define SCR_COPY_NULL    (0)
#define SCR_COPY_SINGLE  (1)
#define SCR_COPY_PARTNER (2)
#define SCR_COPY_XOR     (4)
#define SCR_COPY_FILE    (8)

#define SCR_GROUP_NODE  "NODE"
#define SCR_GROUP_WORLD "WORLD"

/* whether SCR is enabled by default */
#ifndef SCR_ENABLE
#define SCR_ENABLE (1)
#endif

/* default debug message level for SCR */
#ifndef SCR_DEBUG
#define SCR_DEBUG (0)
#endif

/* whether to enable logging in SCR */
#ifndef SCR_LOG_ENABLE
#define SCR_LOG_ENABLE (1)
#endif

/* default number of halt seconds to apply to a job */
#ifndef SCR_HALT_SECONDS
#define SCR_HALT_SECONDS (0)
#endif

/* =========================================================================
 * Default config file location, control direcotry, and cache and checkpoint configuration.
 * ========================================================================= */

/* default location for system configuration file */
#ifndef SCR_CONFIG_FILE
#define SCR_CONFIG_FILE "/etc/scr.conf"
#endif

/* base control directory */
#ifndef SCR_CNTL_BASE
#define SCR_CNTL_BASE "/tmp"
#endif

/* default base cache directory */
#ifndef SCR_CACHE_BASE
#define SCR_CACHE_BASE "/tmp"
#endif

/* default cache size (max number of checkpoints to keep in cache) */
#ifndef SCR_CACHE_SIZE
#define SCR_CACHE_SIZE (1)
#endif

/* default redundancy scheme */
#ifndef SCR_COPY_TYPE
#define SCR_COPY_TYPE (SCR_COPY_XOR)
#endif

/* default set size */
#ifndef SCR_SET_SIZE
#define SCR_SET_SIZE (8)
#endif

/* default hop distance */
#ifndef SCR_GROUP
#define SCR_GROUP (SCR_GROUP_NODE)
#endif

/* =========================================================================
 * Default buffer sizes for MPI and file I/O operations.
 * ========================================================================= */

/* buffer size to use for MPI send / recv operations */
#ifndef SCR_MPI_BUF_SIZE
/* #define SCR_MPI_BUF_SIZE (1*1024*1024) */
#define SCR_MPI_BUF_SIZE (128*1024)  /* very strange that this lower number beats the upper one, but whatever ... */
#endif

/* buffer size to use for file I/O operations */
#ifndef SCR_FILE_BUF_SIZE
#define SCR_FILE_BUF_SIZE (1024*1024)
#endif

/* =========================================================================
 * Default settings for distribute, fetch, and flush operations.
 * ========================================================================= */

/* whether the distribute operation is enabled by default (needed for scalable restart) */
#ifndef SCR_DISTRIBUTE
#define SCR_DISTRIBUTE (1)
#endif

/* whether fetch operations should be enabled by default */
#ifndef SCR_FETCH
#define SCR_FETCH (1)
#endif

/* max number of processes which can be fetching data at the same time (flow control) */
#ifndef SCR_FETCH_WIDTH
#define SCR_FETCH_WIDTH (256)
#endif

/* set to 0 to disable flush, set to a positive number to set how many checkpoints between flushes */
#ifndef SCR_FLUSH
#define SCR_FLUSH (10)
#endif

/* max number of processes which can be flushing data at the same time (flow control) */
#ifndef SCR_FLUSH_WIDTH
#define SCR_FLUSH_WIDTH (SCR_FETCH_WIDTH)
#endif

/* whether to force a flush on a restart (useful for codes that must restart from parallel file system) */
#ifndef SCR_FLUSH_ON_RESTART
#define SCR_FLUSH_ON_RESTART (0)
#endif

/* when set, SCR will flush on restart and disable fetch for codes that must restart from the PFS */
#ifndef SCR_GLOBAL_RESTART
#define SCR_GLOBAL_RESTART (0)
#endif

/* whether to switch from synchronous to asynchronous flushes */
#ifndef SCR_FLUSH_ASYNC
#define SCR_FLUSH_ASYNC (0)
#endif

/* aggregrate bandwidth limit to impose during asynchronous flushes */
#ifndef SCR_FLUSH_ASYNC_BW
#define SCR_FLUSH_ASYNC_BW (200*1024*1024)
#endif

/* maximum percent cpu time allowed during asynchronous flushes (does not yet work well) */
#ifndef SCR_FLUSH_ASYNC_PERCENT
#define SCR_FLUSH_ASYNC_PERCENT (0.0) /* TODO: the fsync complicates this throttling, disable it for now */
#endif

/* =========================================================================
 * Default checksum settings.
 * ========================================================================= */

/* whether to compute and check CRC values when copying a file */
#ifndef SCR_CRC_ON_COPY
#define SCR_CRC_ON_COPY (0)
#endif

/* whether to compute and check CRC values during flush and fetch operations */
#ifndef SCR_CRC_ON_FLUSH
#define SCR_CRC_ON_FLUSH (1)
#endif

/* whether to compute and check CRC values when deleting a file */
#ifndef SCR_CRC_ON_DELETE
#define SCR_CRC_ON_DELETE (0)
#endif

#ifndef SCR_PRESERVE_DIRECTORIES
#define SCR_PRESERVE_DIRECTORIES (0)
#endif

#ifndef SCR_USE_CONTAINERS
#define SCR_USE_CONTAINERS (0)
#endif

#ifndef SCR_CONTAINER_SIZE
#define SCR_CONTAINER_SIZE (100*1024*1024*1024ULL)
#endif

/* =========================================================================
 * The following settings adjust when SCR_Need_checkpoint() will return true.
 * If all settings are 0, all options are disabled and Need_checkpoint() always returns true.
 * ========================================================================= */

/* number of times to call Need_checkpoint before returning true, set to 0 to disable */
#ifndef SCR_CHECKPOINT_INTERVAL
#define SCR_CHECKPOINT_INTERVAL (0)
#endif

/* number of seconds to wait between checkpoints, set to 0 to disable */
#ifndef SCR_CHECKPOINT_SECONDS
#define SCR_CHECKPOINT_SECONDS (0)
#endif

/* max percent runtime to spend on checkpointing, set to 0 to disable */
#ifndef SCR_CHECKPOINT_OVERHEAD
#define SCR_CHECKPOINT_OVERHEAD (0)
#endif

/* =========================================================================
 * The following applies to the scr_transfer process.
 * ========================================================================= */

/* number of seconds to sleep between checking the transfer file */
#ifndef SCR_TRANSFER_SECS
#define SCR_TRANSFER_SECS (60.0)
#endif

/* =========================================================================
 * The following applies to scr_io operations
 * ========================================================================= */

/* number of times to attempt to open a file before giving up */
#ifndef SCR_OPEN_TRIES
#define SCR_OPEN_TRIES (5)
#endif

/* number of microseconds to sleep between consecutive file open attempts */
#ifndef SCR_OPEN_USLEEP
#define SCR_OPEN_USLEEP (100)
#endif

#endif /* SCR_CONF_H */
