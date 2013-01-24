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

#include "scr_globals.h"

/*
=========================================
Globals
=========================================
*/

/* There are three directories where SCR manages files: control, cache, and prefix.
 * 
 * The control directory is a fixed location where a job records its state and reads files
 * to interpret commands from the user.  This directory is fixed (hard coded) so that
 * scr utility scripts know where to look to read and write these files.
 *
 * The cache directory is where the job will cache its checkpoint files.
 * This can be changed by the user (via SCR_CACHE_BASE) to target
 * different devices (e.g. RAM disc vs. SSD). By default, it uses the same prefix as the
 * control directory.
 *
 * The prefix directory is where the job will create checkpoint directories and flush
 * checkpoint files to.  Typically, this is on a parallel file system and is set via SCR_PREFIX.
 * If SCR_PREFIX is not set, the current working directory of the running program is used.
*/

char scr_cntl_base[SCR_MAX_FILENAME]  = SCR_CNTL_BASE;  /* base directory for control directory */
char scr_cache_base[SCR_MAX_FILENAME] = SCR_CACHE_BASE; /* base directory for cache directory */

char* scr_cntl_prefix = NULL; /* path of control directory (adds to base directory) */

char* scr_prefix          = NULL; /* path of SCR_PREFIX directory on PFS */
char* scr_prefix_scr      = NULL; /* path of .scr subdir in SCR_PREFIX dir */
scr_path* scr_prefix_path = NULL; /* scr_prefix in scr_path form */

/* these files live in the control directory */
scr_path* scr_master_map_file = NULL;
scr_path* scr_map_file        = NULL;
char* scr_transfer_file   = NULL;

/* we keep the halt, flush, and nodes files in the prefix directory
 * so that the batch script and / or external commands can access them */
scr_path* scr_halt_file  = NULL;
scr_path* scr_flush_file = NULL;
scr_path* scr_nodes_file = NULL;

scr_filemap* scr_map = NULL;    /* memory cache of filemap contents */
scr_hash* scr_halt_hash = NULL; /* memory cache of halt file contents */

char* scr_username    = NULL;           /* username of owner for running job */
char* scr_jobid       = NULL;           /* unique job id string of current job */
char* scr_jobname     = NULL;           /* jobname string, used to tie different runs together */
char* scr_clustername = NULL;           /* name of cluster running job */
int scr_dataset_id    = 0;              /* keeps track of the dataset id */
int scr_checkpoint_id = 0;              /* keeps track of the checkpoint id */
int scr_in_output     = 0;              /* flag tracks whether we are between start and complete calls */
int scr_initialized   = 0;              /* indicates whether the library has been initialized */
int scr_enabled       = SCR_ENABLE;     /* indicates whether the library is enabled */
int scr_debug         = SCR_DEBUG;      /* set debug verbosity */
int scr_log_enable    = SCR_LOG_ENABLE; /* whether to log SCR events */
int scr_page_size     = 0;              /* records block size for aligning MPI and file buffers */

int scr_cache_size    = SCR_CACHE_SIZE; /* set number of checkpoints to keep at one time */
int scr_copy_type     = SCR_COPY_TYPE;  /* select which redundancy algorithm to use */
char* scr_group       = NULL;           /* name of process group likely to fail */
int scr_set_size      = SCR_SET_SIZE;   /* specify number of tasks in xor set */

size_t scr_mpi_buf_size  = SCR_MPI_BUF_SIZE;  /* set MPI buffer size to chunk file transfer */
size_t scr_file_buf_size = SCR_FILE_BUF_SIZE; /* set buffer size to chunk file copies to/from parallel file system */

int scr_halt_seconds     = SCR_HALT_SECONDS; /* secs remaining in allocation before job should be halted */

int scr_distribute       = SCR_DISTRIBUTE;       /* whether to call scr_distribute_files during SCR_Init */
int scr_fetch            = SCR_FETCH;            /* whether to call scr_fetch_files during SCR_Init */
int scr_fetch_width      = SCR_FETCH_WIDTH;      /* specify number of processes to read files simultaneously */
int scr_flush            = SCR_FLUSH;            /* how many checkpoints between flushes */
int scr_flush_width      = SCR_FLUSH_WIDTH;      /* specify number of processes to write files simultaneously */
int scr_flush_on_restart = SCR_FLUSH_ON_RESTART; /* specify whether to flush cache on restart */
int scr_global_restart   = SCR_GLOBAL_RESTART;   /* set if code must be restarted from parallel file system */

int    scr_flush_async             = SCR_FLUSH_ASYNC;         /* whether to use asynchronous flush */
double scr_flush_async_bw          = SCR_FLUSH_ASYNC_BW;      /* bandwidth limit imposed during async flush */
double scr_flush_async_percent     = SCR_FLUSH_ASYNC_PERCENT; /* runtime limit imposed during async flush */
int    scr_flush_async_in_progress = 0;                       /* tracks whether an async flush is currently underway */
int    scr_flush_async_dataset_id  = -1;                      /* tracks the id of the checkpoint being flushed */
double scr_flush_async_bytes       = 0.0;                     /* records the total number of bytes to be flushed */

int scr_crc_on_copy   = SCR_CRC_ON_COPY;   /* whether to enable crc32 checks during scr_swap_files() */
int scr_crc_on_flush  = SCR_CRC_ON_FLUSH;  /* whether to enable crc32 checks during flush and fetch */
int scr_crc_on_delete = SCR_CRC_ON_DELETE; /* whether to enable crc32 checks when deleting checkpoints */

int scr_preserve_directories = SCR_PRESERVE_DIRECTORIES; /* whether to preserve user-defined directories during flush */
int scr_use_containers           = SCR_USE_CONTAINERS;   /* whether to fetch from / flush to container files */
unsigned long scr_container_size = SCR_CONTAINER_SIZE;   /* max number of bytes to store in a container */

int    scr_checkpoint_interval = SCR_CHECKPOINT_INTERVAL; /* times to call Need_checkpoint between checkpoints */
int    scr_checkpoint_seconds  = SCR_CHECKPOINT_SECONDS;  /* min number of seconds between checkpoints */
double scr_checkpoint_overhead = SCR_CHECKPOINT_OVERHEAD; /* max allowed overhead for checkpointing */
int    scr_need_checkpoint_count = 0;   /* tracks the number of times Need_checkpoint has been called */
double scr_time_checkpoint_total = 0.0; /* keeps a running total of the time spent to checkpoint */
int    scr_time_checkpoint_count = 0;   /* keeps a running count of the number of checkpoints taken */

time_t scr_timestamp_checkpoint_start;  /* record timestamp of start of checkpoint */
double scr_time_checkpoint_start;       /* records the start time of the current checkpoint */
double scr_time_checkpoint_end;         /* records the end time of the current checkpoint */

time_t scr_timestamp_compute_start;     /* record timestamp of start of compute phase */
double scr_time_compute_start;          /* records the start time of the current compute phase */
double scr_time_compute_end;            /* records the end time of the current compute phase */

char* scr_my_hostname = NULL; /* hostname of local process */

MPI_Comm scr_comm_world = MPI_COMM_NULL; /* dup of MPI_COMM_WORLD */
int scr_ranks_world     = 0;             /* number of ranks in the job */
int  scr_my_rank_world  = MPI_PROC_NULL; /* my rank in world */

MPI_Comm scr_comm_node        = MPI_COMM_NULL; /* communicator of all tasks on the same node */
MPI_Comm scr_comm_node_across = MPI_COMM_NULL; /* communicator of tasks with same rank on each node */

scr_hash* scr_groupdesc_hash = NULL; /* hash defining group descriptors to be used */
scr_hash* scr_storedesc_hash = NULL; /* hash defining store descriptors to be used */
scr_hash* scr_reddesc_hash   = NULL; /* hash defining redudancy descriptors to be used */

int scr_ngroupdescs = 0;              /* number of descriptors in scr_groupdescs */
scr_groupdesc* scr_groupdescs = NULL; /* group descriptor structs */

int scr_nstoredescs = 0;                  /* number of descriptors in scr_storedescs */
scr_storedesc* scr_storedescs = NULL;     /* store descriptor structs */
scr_storedesc* scr_storedesc_cntl = NULL; /* store descriptor struct for control directory */

int scr_nreddescs = 0;            /* number of redundancy descriptors in scr_reddescs list */
scr_reddesc* scr_reddescs = NULL; /* pointer to list of redundancy descriptors */
