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
#include "config.h"

/* =========================================================================
 * Defines for checkpoint copy type later below, don't change this defines.
 * ========================================================================= */

/* redundancy schemes: enum as powers of two for binary and/or operations,
 * don't change these */
#define SCR_COPY_NULL    (0)
#define SCR_COPY_SINGLE  (1)
#define SCR_COPY_PARTNER (2)
#define SCR_COPY_XOR     (4)
#define SCR_COPY_FILE    (8)
#define SCR_COPY_RS      (16)

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
#define SCR_LOG_ENABLE (0)
#endif

/* whether to enable text file logging in SCR */
#ifndef SCR_LOG_TXT_ENABLE
#define SCR_LOG_TXT_ENABLE (1)
#endif

/* whether to enable syslog logging in SCR */
#ifndef SCR_LOG_SYSLOG_ENABLE
#define SCR_LOG_SYSLOG_ENABLE (1)
#endif

/* text to prepend to syslog messages */
#ifndef SCR_LOG_SYSLOG_PREFIX
#define SCR_LOG_SYSLOG_PREFIX "SCR"
#endif

/* syslog facility */
#ifndef SCR_LOG_SYSLOG_FACILITY
#define SCR_LOG_SYSLOG_FACILITY LOG_LOCAL7
#endif

/* syslog level */
#ifndef SCR_LOG_SYSLOG_LEVEL
#define SCR_LOG_SYSLOG_LEVEL LOG_INFO
#endif

/* default number of halt seconds to apply to a job */
#ifndef SCR_HALT_SECONDS
#define SCR_HALT_SECONDS (0)
#endif

/* whether SCR will call exit if halt condition is detected */
#ifndef SCR_HALT_EXIT
#define SCR_HALT_EXIT (0)
#endif

/* =========================================================================
 * Default config file location, control directory, and cache and checkpoint configuration.
 * ========================================================================= */

/* default location for system configuration file */
#ifndef SCR_CONFIG_FILE
#define SCR_CONFIG_FILE "/etc/scr.conf"
#endif

/* default name of user config file */
#define SCR_CONFIG_FILE_USER ".scrconf"

/* name of application config file */
#define SCR_CONFIG_FILE_APP "app.conf"

/* base control directory */
#ifndef SCR_CNTL_BASE
#define SCR_CNTL_BASE "/dev/shm"
#endif

/* default base cache directory */
#ifndef SCR_CACHE_BASE
#define SCR_CACHE_BASE "/dev/shm"
#endif

/* default cache size (max number of checkpoints to keep in cache) */
#ifndef SCR_CACHE_SIZE
#define SCR_CACHE_SIZE (1)
#endif

/* default redundancy scheme */
#ifndef SCR_COPY_TYPE
#define SCR_COPY_TYPE (SCR_COPY_XOR)
#endif

/* default failure group */
#ifndef SCR_GROUP
#define SCR_GROUP (SCR_GROUP_NODE)
#endif

/* default failure group set size */
#ifndef SCR_SET_SIZE
#define SCR_SET_SIZE (8)
#endif

/* default number of checksum blocks */
#ifndef SCR_SET_FAILURES
#define SCR_SET_FAILURES (2)
#endif

/* default cache bypass setting */
#ifndef SCR_CACHE_BYPASS
#define SCR_CACHE_BYPASS (1)
#endif

/* =========================================================================
 * Default buffer sizes for MPI and file I/O operations.
 * ========================================================================= */

/* buffer size to use for MPI send / recv operations */
#ifndef SCR_MPI_BUF_SIZE
#define SCR_MPI_BUF_SIZE (1*1024*1024)
#endif

/* buffer size to use for file I/O operations */
#ifndef SCR_FILE_BUF_SIZE
#define SCR_FILE_BUF_SIZE (32*1024*1024)
#endif

/* whether file metadata should also be copied */
#ifndef SCR_COPY_METADATA
#define SCR_COPY_METADATA (1)
#endif

/* whether to have AXL create directories for files during a flush,
 * by default we disable this since SCR takes on that role */
#ifndef SCR_AXL_MKDIR
#define SCR_AXL_MKDIR (0)
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

/* AXL type to use when fetching datasets */
#ifndef SCR_FETCH_TYPE
#define SCR_FETCH_TYPE ("SYNC")
#endif

/* whether to use implied bypass on fetch to read files from file system rather than actually copy to cache */
#ifndef SCR_FETCH_BYPASS
#define SCR_FETCH_BYPASS (0)
#endif

/* set to 0 to disable flush, set to a positive number to set how many checkpoints between flushes */
#ifndef SCR_FLUSH
#define SCR_FLUSH (10)
#endif

/* max number of processes which can be flushing data at the same time (flow control) */
#ifndef SCR_FLUSH_WIDTH
#define SCR_FLUSH_WIDTH (SCR_FETCH_WIDTH)
#endif

/* AXL type to use when flushing datasets */
#ifndef SCR_FLUSH_TYPE
#define SCR_FLUSH_TYPE ("SYNC")
#endif

/* whether to force a flush on a restart (useful for codes that must restart from parallel file system) */
#ifndef SCR_FLUSH_ON_RESTART
#define SCR_FLUSH_ON_RESTART (0)
#endif

/* when set, SCR will flush on restart and set fetch to bypass mode for codes that must restart from the PFS */
#ifndef SCR_GLOBAL_RESTART
#define SCR_GLOBAL_RESTART (0)
#endif

/* whether to switch from synchronous to asynchronous flushes */
#ifndef SCR_FLUSH_ASYNC
#define SCR_FLUSH_ASYNC (0)
#endif

/* Finalize async transfers in scr_poststage rather than in SCR_Finalize() */
#ifndef SCR_FLUSH_POSTSTAGE
#define SCR_FLUSH_POSTSTAGE (0)
#endif

/* aggregrate bandwidth limit to impose during asynchronous flushes */
#ifndef SCR_FLUSH_ASYNC_BW
#define SCR_FLUSH_ASYNC_BW (200*1024*1024)
#endif

/* maximum percent cpu time allowed during asynchronous flushes (does not yet work well) */
#ifndef SCR_FLUSH_ASYNC_PERCENT
#define SCR_FLUSH_ASYNC_PERCENT (0.0) /* TODO: the fsync complicates this throttling, disable it for now */
#endif

/* sleep time when polling for an async flush to complete */
#ifndef SCR_FLUSH_ASYNC_USLEEP
#define SCR_FLUSH_ASYNC_USLEEP (1000)
#endif

/* max number of checkpoints to keep in prefix (0 disables) */
#ifndef SCR_PREFIX_SIZE
#define SCR_PREFIX_SIZE (0)
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
