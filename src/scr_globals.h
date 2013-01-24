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

#ifndef SCR_GLOBALS_H
#define SCR_GLOBALS_H

#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <strings.h>
#include <ctype.h>

/* variable length args */
#include <stdarg.h>
#include <errno.h>

/* gethostbyname */
#include <netdb.h>

/* basename/dirname */
#include <unistd.h>
#include <libgen.h>

/* gettimeofday */
#include <sys/time.h>

/* localtime, asctime */
#include <time.h>

/* compute crc32 */
#include <zlib.h>

/* list data structures */
/* need at least version 8.5 of queue.h from Berkeley */
/*#include <sys/queue.h>*/
#include "queue.h"

#include "mpi.h"

#include "scr_conf.h"
#include "scr.h"
#include "scr_err.h"
#include "scr_io.h"
#include "scr_util.h"
#include "scr_util_mpi.h"
#include "scr_path.h"
#include "scr_path_mpi.h"
#include "scr_split.h"
#include "scr_meta.h"
#include "scr_dataset.h"
#include "scr_halt.h"
#include "scr_log.h"
#include "scr_hash.h"
#include "scr_hash_mpi.h"
#include "scr_filemap.h"
#include "scr_config.h"
#include "scr_param.h"
#include "scr_env.h"
#include "scr_index_api.h"

#include "scr_groupdesc.h"
#include "scr_storedesc.h"
#include "scr_reddesc.h"
#include "scr_reddesc_apply.h"
#include "scr_reddesc_recover.h"
#include "scr_summary.h"
#include "scr_flush_file_mpi.h"
#include "scr_cache.h"
#include "scr_cache_rebuild.h"
#include "scr_fetch.h"
#include "scr_flush.h"
#include "scr_flush_sync.h"
#include "scr_flush_async.h"

/*
=========================================
Globals
=========================================
*/

extern char scr_cntl_base[SCR_MAX_FILENAME];  /* base directory for control directory */
extern char scr_cache_base[SCR_MAX_FILENAME]; /* base directory for cache directory */

extern char* scr_cntl_prefix; /* path of control directory (adds to base directory) */

extern char* scr_prefix;          /* path of SCR_PREFIX directory on PFS */
extern char* scr_prefix_scr;      /* path to .scr subdir in SCR_PREFIX dir */
extern scr_path* scr_prefix_path; /* scr_prefix in scr_path form */

/* these files live in the control directory */
extern scr_path* scr_master_map_file;
extern scr_path* scr_map_file;
extern char* scr_transfer_file;

/* we keep the halt, flush, and nodes files in the prefix directory
 * so that the batch script and / or external commands can access them */
extern scr_path* scr_halt_file;
extern scr_path* scr_flush_file;
extern scr_path* scr_nodes_file;

extern scr_filemap* scr_map;    /* memory cache of filemap contents */
extern scr_hash* scr_halt_hash; /* memory cache of halt file contents */

extern char* scr_username;    /* username of owner for running job */
extern char* scr_jobid;       /* unique job id string of current job */
extern char* scr_jobname;     /* jobname string, used to tie different runs together */
extern char* scr_clustername; /* name of cluster job is running on */
extern int scr_dataset_id;    /* keeps track of the current dataset id */
extern int scr_checkpoint_id; /* keeps track of the current checkpoint id */
extern int scr_in_output ;    /* flag tracks whether we are between start and complete calls */
extern int scr_initialized;   /* indicates whether the library has been initialized */
extern int scr_enabled;       /* indicates whether the library is enabled */
extern int scr_debug;         /* set debug verbosity */
extern int scr_log_enable;    /* whether to log SCR events */
extern int scr_page_size;     /* records block size for aligning MPI and file buffers */

extern int scr_cache_size;    /* number of checkpoints to keep in cache at one time */
extern int scr_copy_type;     /* select which redundancy algorithm to use */
extern char* scr_group;       /* name of process group likely to fail */
extern int scr_set_size;      /* specify number of tasks in xor set */

extern size_t scr_mpi_buf_size;  /* set MPI buffer size to chunk file transfer */
extern size_t scr_file_buf_size; /* set buffer size to chunk file copies to/from parallel file system */

extern int scr_halt_seconds; /* secs remaining in allocation before job should be halted */

extern int scr_distribute;       /* whether to call scr_distribute_files during SCR_Init */
extern int scr_fetch;            /* whether to call scr_fetch_files during SCR_Init */
extern int scr_fetch_width;      /* specify number of processes to read files simultaneously */
extern int scr_flush;            /* how many checkpoints between flushes */
extern int scr_flush_width;      /* specify number of processes to write files simultaneously */
extern int scr_flush_on_restart; /* specify whether to flush cache on restart */
extern int scr_global_restart;   /* set if code must be restarted from parallel file system */

extern int scr_flush_async;             /* whether to use asynchronous flush */
extern double scr_flush_async_bw;       /* bandwidth limit imposed during async flush */
extern double scr_flush_async_percent;  /* runtime limit imposed during async flush */
extern int scr_flush_async_in_progress; /* tracks whether an async flush is currently underway */
extern int scr_flush_async_dataset_id;  /* tracks the id of the checkpoint being flushed */
extern double scr_flush_async_bytes;    /* records the total number of bytes to be flushed */

extern int scr_crc_on_copy;   /* whether to enable crc32 checks during scr_swap_files() */
extern int scr_crc_on_flush;  /* whether to enable crc32 checks during flush and fetch */
extern int scr_crc_on_delete; /* whether to enable crc32 checks when deleting checkpoints */

extern int scr_preserve_directories;     /* whether to preserve user-defined directories during flush */
extern int scr_use_containers;           /* whether to fetch from / flush to container files */
extern unsigned long scr_container_size; /* max number of bytes to store in a container */

extern int    scr_checkpoint_interval;   /* times to call Need_checkpoint between checkpoints */
extern int    scr_checkpoint_seconds;    /* min number of seconds between checkpoints */
extern double scr_checkpoint_overhead;   /* max allowed overhead for checkpointing */
extern int    scr_need_checkpoint_count; /* tracks the number of times Need_checkpoint has been called */
extern double scr_time_checkpoint_total; /* keeps a running total of the time spent to checkpoint */
extern int    scr_time_checkpoint_count; /* keeps a running count of the number of checkpoints taken */

extern time_t scr_timestamp_checkpoint_start; /* record timestamp of start of checkpoint */
extern double scr_time_checkpoint_start;      /* records the start time of the current checkpoint */
extern double scr_time_checkpoint_end;        /* records the end time of the current checkpoint */

extern time_t scr_timestamp_compute_start;    /* record timestamp of start of compute phase */
extern double scr_time_compute_start;         /* records the start time of the current compute phase */
extern double scr_time_compute_end;           /* records the end time of the current compute phase */

extern char* scr_my_hostname; /* hostname of local process */

extern MPI_Comm scr_comm_world;   /* dup of MPI_COMM_WORLD */
extern int scr_ranks_world;       /* number of ranks in the job */
extern int  scr_my_rank_world;    /* my rank in world */

extern MPI_Comm scr_comm_node;        /* communicator of all tasks on the same node */
extern MPI_Comm scr_comm_node_across; /* communicator of tasks with same rank on each node */

extern scr_hash* scr_groupdesc_hash; /* hash defining group descriptors to be used */
extern scr_hash* scr_storedesc_hash; /* hash defining store descriptors to be used */
extern scr_hash* scr_reddesc_hash;   /* hash defining redudancy descriptors to be used */

extern int scr_ngroupdescs;           /* number of descriptors in scr_groupdescs */
extern scr_groupdesc* scr_groupdescs; /* group descriptor structs */

extern int scr_nstoredescs;               /* number of descriptors in scr_storedescs */
extern scr_storedesc* scr_storedescs;     /* store descriptor structs */
extern scr_storedesc* scr_storedesc_cntl; /* store descriptor struct for control directory */

extern int scr_nreddescs;         /* number of redundancy descriptors in scr_reddescs list */
extern scr_reddesc* scr_reddescs; /* pointer to list of redundancy descriptors */

#endif
