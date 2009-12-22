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

#include "scr.h"
#include "scr_io.h"
#include "scr_meta.h"
#include "scr_halt.h"
#include "scr_log.h"
#include "scr_copy_xor.h"
#include "scr_hash.h"
#include "scr_filemap.h"
#include "scr_param.h"

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#ifdef HAVE_LIBYOGRT
#include "yogrt.h"
#endif /* HAVE_LIBYOGRT */

/*
=========================================
Globals
=========================================
*/

#define SCR_SUMMARY_FILE_VERSION_2 (2)
#define SCR_SUMMARY_FILE_VERSION_3 (3)

/* redundancy shemes: enum as powers of two for binary and/or operations */
#define SCR_COPY_LOCAL   (1)
#define SCR_COPY_PARTNER (2)
#define SCR_COPY_XOR     (4)

/* deepest level to which checkpoint is stored */
#define SCR_FLUSH_CACHE ("CACHE")
#define SCR_FLUSH_PFS   ("PFS")

/* copy file operation flags: copy file vs. move file */
#define COPY_FILES 0
#define MOVE_FILES 1

#ifndef SCR_ENABLE
#define SCR_ENABLE (1)
#endif
#ifndef SCR_DEBUG
#define SCR_DEBUG (0)
#endif
#ifndef SCR_CACHE_SIZE
#define SCR_CACHE_SIZE (2)
#endif
#ifndef SCR_COPY_TYPE
#define SCR_COPY_TYPE (SCR_COPY_XOR)
#endif
#ifndef SCR_XOR_SIZE
#define SCR_XOR_SIZE (8)
#endif
#ifndef SCR_PARTNER_DISTANCE
#define SCR_PARTNER_DISTANCE (1)
#endif
#ifndef SCR_MPI_BUF_SIZE
/* #define SCR_MPI_BUF_SIZE (1*1024*1024) */
#define SCR_MPI_BUF_SIZE (128*1024)  /* very strange that this lower number beats the upper one, but whatever ... */
#endif
#ifndef SCR_HALT_SECONDS
#define SCR_HALT_SECONDS (0)
#endif
#ifndef SCR_FETCH
#define SCR_FETCH (1)
#endif
#ifndef SCR_FETCH_WIDTH
#define SCR_FETCH_WIDTH (256)
#endif
#ifndef SCR_DISTRIBUTE
#define SCR_DISTRIBUTE (1)
#endif
#ifndef SCR_FLUSH
#define SCR_FLUSH (10)
#endif
#ifndef SCR_FLUSH_WIDTH
#define SCR_FLUSH_WIDTH (SCR_FETCH_WIDTH)
#endif
#ifndef SCR_FLUSH_ON_RESTART
#define SCR_FLUSH_ON_RESTART (0)
#endif
#ifndef SCR_FILE_BUF_SIZE
#define SCR_FILE_BUF_SIZE (1024*1024)
#endif
#ifndef SCR_CRC_ON_COPY
#define SCR_CRC_ON_COPY (0)
#endif
#ifndef SCR_CRC_ON_FLUSH
#define SCR_CRC_ON_FLUSH (1)
#endif
#ifndef SCR_CHECKPOINT_FREQUENCY
#define SCR_CHECKPOINT_FREQUENCY (0)
#endif
#ifndef SCR_CHECKPOINT_SECONDS
#define SCR_CHECKPOINT_SECONDS (0)
#endif
#ifndef SCR_CHECKPOINT_OVERHEAD
#define SCR_CHECKPOINT_OVERHEAD (0)
#endif

#ifndef SCR_CNTL_BASE
#define SCR_CNTL_BASE "/tmp"
#endif
#ifndef SCR_CACHE_BASE
#define SCR_CACHE_BASE "/tmp"
#endif

/* There are three prefix directories where SCR manages files: control, cache, and pfs.
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
 * The pfs prefix directory is where the job will create checkpoint directories and flush
 * checkpoint files to.  Typically, this is on a parallel file system and is set via SCR_PREFIX.
 * If SCR_PREFIX is not set, the current working directory of the running program is used.
*/

static char scr_cntl_base[SCR_MAX_FILENAME]  = SCR_CNTL_BASE;
static char scr_cache_base[SCR_MAX_FILENAME] = SCR_CACHE_BASE;

static char scr_cntl_prefix[SCR_MAX_FILENAME]  = "";
static char scr_cache_prefix[SCR_MAX_FILENAME] = "";
static char scr_par_prefix[SCR_MAX_FILENAME]   = "";

static char scr_master_map_file[SCR_MAX_FILENAME];
static char scr_map_file[SCR_MAX_FILENAME];
static char scr_halt_file[SCR_MAX_FILENAME];
static char scr_flush_file[SCR_MAX_FILENAME];
static char scr_nodes_file[SCR_MAX_FILENAME];

static struct scr_haltdata halt;

static char* scr_username    = NULL;       /* username of owner for running job */
static char* scr_jobid       = NULL;       /* unique job id string of current job */
static char* scr_jobname     = NULL;       /* jobname string, used to tie different runs together */
static int scr_checkpoint_id = 0;          /* keeps track of the checkpoint id */
static int scr_initialized   = 0;          /* indicates whether the library has been initialized */
static int scr_enabled       = SCR_ENABLE; /* indicates whether the librarys is enabled */
static int scr_debug         = SCR_DEBUG;  /* set debug verbosity */
static struct scr_hash* scr_checkpoint_file_list = NULL; /* keeps track of all files written during a checkpoint */

static int scr_cache_size       = SCR_CACHE_SIZE;       /* set number of checkpoints to keep at one time */
static int scr_copy_type        = SCR_COPY_TYPE;        /* select which redundancy algorithm to use */
static int scr_partner_distance = SCR_PARTNER_DISTANCE; /* number of nodes away to choose parnter */
static int scr_xor_size         = SCR_XOR_SIZE;         /* specify number of tasks in xor set */
static size_t scr_mpi_buf_size  = SCR_MPI_BUF_SIZE;     /* set MPI buffer size to chunk file transfer */

static int scr_halt_seconds      = SCR_HALT_SECONDS; /* secs remaining in allocation before job should be halted */

static int scr_fetch            = SCR_FETCH;            /* whether to call scr_fetch_files during SCR_Init */
static int scr_fetch_width      = SCR_FETCH_WIDTH;      /* specify number of processes to read files simultaneously */
static int scr_distribute       = SCR_DISTRIBUTE;       /* whether to call scr_distribute_files during SCR_Init */
static int scr_flush            = SCR_FLUSH;            /* how many checkpoints between flushes */
static int scr_flush_width      = SCR_FLUSH_WIDTH;      /* specify number of processes to write files simultaneously */
static int scr_flush_on_restart = SCR_FLUSH_ON_RESTART; /* specify whether to flush cache on restart */
static size_t scr_file_buf_size = SCR_FILE_BUF_SIZE;    /* set buffer size to chunk file copies to/from parallel file system */

static int scr_crc_on_copy  = SCR_CRC_ON_COPY;  /* whether to enable crc32 checks during scr_swap_files() */
static int scr_crc_on_flush = SCR_CRC_ON_FLUSH; /* whether to enable crc32 checks during flush and fetch */

static int    scr_checkpoint_frequency = SCR_CHECKPOINT_FREQUENCY; /* times to call Need_checkpoint between checkpoints */
static int    scr_checkpoint_seconds   = SCR_CHECKPOINT_SECONDS;   /* min number of seconds between checkpoints */
static double scr_checkpoint_overhead  = SCR_CHECKPOINT_OVERHEAD;  /* max allowed overhead for checkpointing */
static int    scr_need_checkpoint_id   = 0;    /* tracks the number of times Need_checkpoint has been called */
static double scr_time_checkpoint_total = 0.0; /* keeps a running total of the time spent to checkpoint */
static int    scr_time_checkpoint_count = 0;   /* keeps a running count of the number of checkpoints taken */

static time_t scr_timestamp_checkpoint_start;  /* record timestamp of start of checkpoint */
static double scr_time_checkpoint_start;       /* records the start time of the current checkpoint */
static double scr_time_checkpoint_end;         /* records the end time of the current checkpoint */

static time_t scr_timestamp_compute_start;     /* record timestamp of start of compute phase */
static double scr_time_compute_start;          /* records the start time of the current compute phase */
static double scr_time_compute_end;            /* records the end time of the current compute phase */

static int   scr_log_enable = 1; /* whether to log SCR events */

static MPI_Comm scr_comm_world; /* dup of MPI_COMM_WORLD */
static MPI_Comm scr_comm_local; /* contains all tasks local to the same node */
static MPI_Comm scr_comm_level; /* contains tasks across all nodes at the same local rank level */
static MPI_Comm scr_comm_xor;   /* contains tasks in same xor set */

static int scr_ranks_world; /* number of ranks in the job */
static int scr_ranks_local; /* number of ranks on my node */
static int scr_ranks_level; /* number of ranks at my level (i.e., number of processes with same local rank across all nodes) */
static int scr_ranks_xor;   /* number of ranks in my XOR set */

static int scr_xor_set_id;

static int  scr_my_rank_world;  /* my rank in world */
static int  scr_my_rank_local;  /* my local rank on my node */
static int  scr_my_rank_level;  /* my rank in processes at my level */
static int  scr_my_rank_xor;    /* my rank within my XOR set */
static char scr_my_hostname[256];

static int  scr_lhs_rank;       /* relative rank of left-hand-size partner */
static int  scr_lhs_rank_world; /* rank of left-hand-size partner in world */
static char scr_lhs_hostname[256];

static int  scr_rhs_rank;       /* relative rank of right-hand-size partner */
static int  scr_rhs_rank_world; /* rank of right-hand-side partner in world */
static char scr_rhs_hostname[256];

/*
=========================================
Error and Debug Messages
=========================================
*/

/* print message to stderr */
void scr_err(const char *fmt, ...)
{
  va_list argp;
  fprintf(stderr, "SCR ERROR: rank %d on %s: ", scr_my_rank_world, scr_my_hostname);
  va_start(argp, fmt);
  vfprintf(stderr, fmt, argp);
  va_end(argp);
  fprintf(stderr, "\n");
}

/* print message to stdout if scr_debug is set and it is >= level */
void scr_dbg(int level, const char *fmt, ...)
{
  va_list argp;
  if (level == 0 || (scr_debug > 0 && scr_debug >= level)) {
    fprintf(stdout, "SCR: rank %d on %s: ", scr_my_rank_world, scr_my_hostname);
    va_start(argp, fmt);
    vfprintf(stdout, fmt, argp);
    va_end(argp);
    fprintf(stdout, "\n");
  }
}

/* print abort message and call MPI_Abort to kill run */
void scr_abort(int rc, const char *fmt, ...)
{
  va_list argp;
  fprintf(stderr, "SCR ABORT: rank %d on %s: ", scr_my_rank_world, scr_my_hostname);
  va_start(argp, fmt);
  vfprintf(stderr, fmt, argp);
  va_end(argp);
  fprintf(stderr, "\n");

  MPI_Abort(MPI_COMM_WORLD, 0);
}

/*
=========================================
MPI utility functions
=========================================
*/

/* returns whether all flags in world are true (i.e., non-zero) */
static int scr_alltrue(int flag)
{
  int all_true = 0;
  MPI_Allreduce(&flag, &all_true, 1, MPI_INT, MPI_LAND, scr_comm_world);
  return all_true;
}

/*
=========================================
Metadata functions
=========================================
*/

/* marks file as incomplete by deleting corresponding .scr meta file */
static int scr_incomplete(const char* file)
{
  /* create the .scr extension for file */
  char file_scr[SCR_MAX_FILENAME];
  scr_meta_name(file_scr, file);

  /* delete the .scr file ==> the current file is not complete */
  unlink(file_scr);

  return SCR_SUCCESS;
}

/* creates corresponding .scr meta file for file to record completion info */
static int scr_complete(const char* file, const struct scr_meta* meta)
{
  int rc = SCR_SUCCESS;

  /* just need to write out the meta data */
  scr_write_meta(file, meta);

  return rc;
}

/*
=========================================
Checkpoint functions
=========================================
*/

/*
  READ:
  master process on each node reads filemap
  and distributes pieces to others

  WRITE:
  all processes send their file info to master
  and master writes it out

  master filemap file
    list of ranks this node has files for
      for each rank, list of checkpoint ids
        for each checkpoint id, list of locations (RAM,SSD,PFS,etc)
            for each location, list of files for this rank for this checkpoint

  GOALS: 
    - support different number of processes per node on
      a restart
    - support multiple files per rank per checkpoint
    - support multiple checkpoints at different cache levels
*/

/* returns the checkpoint directory for a given checkpoint id */
static int scr_checkpoint_dir(int checkpoint_id, char* dir)
{
  /* TODO: have this return more specific directory names */
  sprintf(dir, "%s/checkpoint_%d", scr_cache_prefix, checkpoint_id);
  return SCR_SUCCESS;
}

/* create a checkpoint directory given a checkpoint id, waits for all tasks on the same node before returning */
static int scr_create_checkpoint_dir(int checkpoint_id)
{
  char dir[SCR_MAX_FILENAME];
  int rc = scr_checkpoint_dir(checkpoint_id, dir);
  if (scr_my_rank_local == 0) {
    scr_dbg(2, "Creating checkpoint directory: %s", dir);
    rc = scr_mkdir(dir, S_IRWXU);
  }
  MPI_Barrier(scr_comm_local);
  return rc;
}

/* remove a checkpoint directory given a checkpoint id, waits for all tasks on the same node before removing */
static int scr_remove_checkpoint_dir(int checkpoint_id)
{
  char dir[SCR_MAX_FILENAME];
  int rc = scr_checkpoint_dir(checkpoint_id, dir);
  MPI_Barrier(scr_comm_local);
  if (scr_my_rank_local == 0) {
    scr_dbg(2, "Removing checkpoint directory: %s", dir);
    rmdir(dir);
  }
  return rc;
}

/* removes entries in flush file for given checkpoint id */
static int scr_remove_checkpoint_flush(int checkpoint_id)
{
  /* all master tasks write this file to their node */
  if (scr_my_rank_local == 0) {
    char str_ckpt[SCR_MAX_FILENAME];
    sprintf(str_ckpt, "%d", checkpoint_id);
    struct scr_hash* hash = scr_hash_new();
    scr_hash_read(scr_flush_file, hash);
    scr_hash_unset(hash, str_ckpt);
    scr_hash_write(scr_flush_file, hash);
    scr_hash_delete(hash);
  }
  return SCR_SUCCESS;
}

/* return the number of checkpoints in cache */
static int scr_num_checkpoints()
{
  int n = 0;
  struct scr_filemap* map = scr_filemap_new();
  if (scr_filemap_read(scr_map_file, map) == SCR_SUCCESS) {
    n = scr_filemap_num_checkpoints(map);
  }
  scr_filemap_delete(map);
  return n;
}

/* return the latest checkpoint id */
static int scr_latest_checkpoint()
{
  int id = -1;
  struct scr_filemap* map = scr_filemap_new();
  if (scr_filemap_read(scr_map_file, map) == SCR_SUCCESS) {
    id = scr_filemap_latest_checkpoint(map);
  }
  scr_filemap_delete(map);
  return id;
}

/* return the oldest checkpoint id */
static int scr_oldest_checkpoint()
{
  int id = -1;
  struct scr_filemap* map = scr_filemap_new();
  if (scr_filemap_read(scr_map_file, map) == SCR_SUCCESS) {
    id = scr_filemap_oldest_checkpoint(map);
  }
  scr_filemap_delete(map);
  return id;
}

/* remove all checkpoint files assciated with specified checkpoint */
static int scr_unlink_checkpoint(int ckpt)
{
  /* print a message to differentiate some verbose debug messages */
  if (scr_my_rank_world == 0) {
    scr_dbg(1, "Deleting checkpoint %d from cache", ckpt);
  }

  /* if we fail to read the filemap, then assume we don't have any files */
  struct scr_filemap* map = scr_filemap_new();
  if (scr_filemap_read(scr_map_file, map) != SCR_SUCCESS) {
    scr_filemap_delete(map);
    return 1;
  }

  /* for each file of each checkpoint we have of each rank, delete the file */
  struct scr_hash_elem* rank_elem;
  for (rank_elem = scr_filemap_first_rank_by_checkpoint(map, ckpt);
       rank_elem != NULL;
       rank_elem = scr_hash_elem_next(rank_elem))
  {
    int rank = scr_hash_elem_key_int(rank_elem);
    struct scr_hash_elem* file_elem;
    for (file_elem = scr_filemap_first_file(map, ckpt, rank);
         file_elem != NULL;
         file_elem = scr_hash_elem_next(file_elem))
    {
      char* file = scr_hash_elem_key(file_elem); 
      if (access(file, R_OK) == 0) {
        /* delete the file */
        unlink(file);

        /* remove the corresponding meta file */
        scr_incomplete(file);

        scr_dbg(2, "scr_unlink_checkpoint: unlink(%s)",
                file
        );
      }
    }
  }

  /* remove all associations for this checkpoint from the filemap */
  scr_filemap_remove_checkpoint(map, ckpt);

  /* update the filemap on disk */
  scr_filemap_write(scr_map_file, map);

  /* free map object */
  scr_filemap_delete(map);

  return 1;
}

/* remove all checkpoint files recorded in filemap, and the filemap itself */
static int scr_unlink_all()
{
  /* if we fail to read the filemap, then assume we don't have any files */
  struct scr_filemap* map = scr_filemap_new();
  if (scr_filemap_read(scr_map_file, map) != SCR_SUCCESS) {
    scr_filemap_delete(map);
    return 1;
  }

  /* count the maximum number of checkpoints belonging to any rank on our node */
  int num_ckpts = scr_num_checkpoints();
  int max_num_ckpts = -1;
  MPI_Allreduce(&num_ckpts, &max_num_ckpts, 1, MPI_INT, MPI_MAX, scr_comm_local);
  while (max_num_ckpts > 0) {
    /* there's at least one checkpoint left, get the maximum latest checkpoint id */
    int ckpt = scr_latest_checkpoint();
    int max_ckpt = -1;
    MPI_Allreduce(&ckpt, &max_ckpt, 1, MPI_INT, MPI_MAX, scr_comm_local);

    /* remove this checkpoint from all tasks */
    scr_unlink_checkpoint(max_ckpt);
    scr_remove_checkpoint_dir(max_ckpt);
    scr_remove_checkpoint_flush(max_ckpt);

    /* get the number of checkpoints left on the node */
    num_ckpts = scr_num_checkpoints();
    max_num_ckpts = -1;
    MPI_Allreduce(&num_ckpts, &max_num_ckpts, 1, MPI_INT, MPI_MAX, scr_comm_local);
  }

  /* now delete the filemap itself */
  unlink(scr_map_file);
  scr_dbg(2, "scr_unlink_all: unlink(%s)",
          scr_map_file
  );

  /* free map object */
  scr_filemap_delete(map);

  return 1;
}

/* checks whether specifed file exists, is readable, and is complete */
static int scr_bool_have_file(const char* file, int ckpt, int rank)
{
  /* if no filename is given return false */
  if (file == NULL || strcmp(file,"") == 0) {
    scr_dbg(2, "scr_bool_have_file: File name is null or the empty string");
    return 0;
  }

  /* check that we can read the file */
  if (access(file, R_OK) < 0) {
    scr_dbg(2, "scr_bool_have_file: Do not have read access to file: %s", file);
    return 0;
  }

  /* check that we can read meta file for the file */
  struct scr_meta meta;
  if (scr_read_meta(file, &meta) != SCR_SUCCESS) {
    scr_dbg(2, "scr_bool_have_file: Failed to read meta data file for file: %s", file);
    return 0;
  }

  /* TODO: check that filesizes match (use strtol while reading data) */
  /* TODO: check that crc32 match if set */

  /* check that the file is complete */
  if (!meta.complete) {
    scr_dbg(2, "scr_bool_have_file: File is marked as incomplete: %s", file);
    return 0;
  }

  /* check that the file really belongs to the checkpoint id we think it does */
  if (meta.checkpoint_id != ckpt) {
    scr_dbg(2, "scr_bool_have_file: File's checkpoint ID (%d) does not match id in meta data file (%d) for %s",
            ckpt, meta.checkpoint_id, file\
    );
    return 0;
  }

  /* check that the file really belongs to the rank we think it does */
  if (meta.rank != rank) {
    scr_dbg(2, "scr_bool_have_file: File's rank (%d) does not match rank in meta data file (%d) for %s",
            rank, meta.rank, file
    );
    return 0;
  }

  /* check that the file meta data has the right number of ranks */
  if (meta.ranks != scr_ranks_world) {
    scr_dbg(2, "scr_bool_have_file: File's number of ranks (%d) does not match number of ranks in current job (%d) for %s",
            scr_ranks_world, meta.ranks, file
    );
    return 0;
  }

  /* if we made it here, the file is good */
  return 1;
}

/* check whether we have all files for a given rank of a given checkpoint */
static int scr_bool_have_files(int ckpt, int rank)
{
  /* read in the filemap */
  struct scr_filemap* map = scr_filemap_new();
  if (scr_filemap_read(scr_map_file, map) != SCR_SUCCESS) {
    scr_filemap_delete(map);
    return 0;
  }

  /* check that we have any files for the specified rank */
  if (!scr_filemap_have_rank_by_checkpoint(map, ckpt, rank)) {
    scr_filemap_delete(map);
    return 0;
  }

  /* check whether we have all of the files we should */
  int expected_files = scr_filemap_num_expected_files(map, ckpt, rank);
  int num_files = scr_filemap_num_files(map, ckpt, rank);
  if (num_files != expected_files) {
    scr_filemap_delete(map);
    return 0;
  }

  /* check the integrity of each of the files */
  int missing_a_file = 0;
  struct scr_hash_elem* file_elem;
  for (file_elem = scr_filemap_first_file(map, ckpt, rank);
       file_elem != NULL;
       file_elem = scr_hash_elem_next(file_elem))
  {
    char* file = scr_hash_elem_key(file_elem);
    if (!scr_bool_have_file(file, ckpt, rank)) {
      missing_a_file = 1;
    }
  }
  if (missing_a_file) {
    scr_filemap_delete(map);
    return 0;
  }

  /* if we make it here, we have all of our files */
  scr_filemap_delete(map);
  return 1;
}

/* opens the filemap, inspects that all listed files are readable and complete, unlinks any that are not */
static int scr_clean_files()
{
  /* create a map to remember which files to keep */
  struct scr_filemap* keep_map = scr_filemap_new();

  /* read in the filemap */
  struct scr_filemap* map = scr_filemap_new();
  int failed_map_read = 0;
  if (scr_filemap_read(scr_map_file, map) != SCR_SUCCESS) { failed_map_read = 1; }

  /* scan each file for each rank of each checkpoint */
  struct scr_hash_elem* ckpt_elem;
  for (ckpt_elem = scr_filemap_first_checkpoint(map);
       ckpt_elem != NULL;
       ckpt_elem = scr_hash_elem_next(ckpt_elem))
  {
    int ckpt = scr_hash_elem_key_int(ckpt_elem);
    struct scr_hash_elem* rank_elem;
    for (rank_elem = scr_filemap_first_rank_by_checkpoint(map, ckpt);
         rank_elem != NULL;
         rank_elem = scr_hash_elem_next(rank_elem))
    {
      int rank = scr_hash_elem_key_int(rank_elem);

      /* first time through the file list, check that we have each file */
      int missing_file = 0;
      struct scr_hash_elem* file_elem = NULL;
      for (file_elem = scr_filemap_first_file(map, ckpt, rank);
           file_elem != NULL;
           file_elem = scr_hash_elem_next(file_elem))
      {
        /* get filename and check that we can read it */
        char* file = scr_hash_elem_key(file_elem);
        if (!scr_bool_have_file(file, ckpt, rank)) {
            missing_file = 1;
            scr_dbg(1, "File is unreadable or incomplete: CheckpointID %d, Rank %d, File: %s",
                    ckpt, rank, file
            );
        }
      }

      /* check whether we have all the files we think we should */
      int expected_files = scr_filemap_num_expected_files(map, ckpt, rank);
      int num_files = scr_filemap_num_files(map, ckpt, rank);
      if (num_files != expected_files) { missing_file = 1; }

      /* if we have all the files, set the expected file number in the keep_map */
      if (!failed_map_read && !missing_file) {
        scr_filemap_set_expected_files(keep_map, ckpt, rank, expected_files);
      }

      /* second time through, either add all files to keep_map or delete them all */
      for (file_elem = scr_filemap_first_file(map, ckpt, rank);
           file_elem != NULL;
           file_elem = scr_hash_elem_next(file_elem))
      {
        /* get the filename of the current file, and assume we can read it ok */
        char* file = scr_hash_elem_key(file_elem);

        /* if we failed to read any file, delete them all */
        if (failed_map_read || missing_file) {
          /* inform user on what we're doing */
          scr_dbg(1, "Deleting file: CheckpointID %d, Rank %d, File: %s",
                  ckpt, rank, file
          );

          /* delete the file */
          unlink(file);

          /* delete the meta file */
          scr_incomplete(file);
        } else {
          /* keep this file */
          scr_filemap_copy_file(keep_map, map, ckpt, rank, file);
        }
      }
    }
  }

  /* if the read of the file map failed, delete the map itself, otherwise, write out the (possibly) updated filemap out */
  if (failed_map_read) {
    unlink(scr_map_file);
  } else {
    scr_filemap_write(scr_map_file, keep_map);
  }

  /* free memory allocated for filemap */
  scr_filemap_delete(map);
  scr_filemap_delete(keep_map);

  return SCR_SUCCESS;
}

/* returns true iff each file in the filemap can be read */
static int scr_check_files(struct scr_filemap* map, int checkpoint_id)
{
  /* for each file of each rank for specified checkpoint id */
  int failed_read = 0;
  struct scr_hash_elem* rank_elem;
  for (rank_elem = scr_filemap_first_rank_by_checkpoint(map, checkpoint_id);
       rank_elem != NULL;
       rank_elem = scr_hash_elem_next(rank_elem))
  {
    int rank = scr_hash_elem_key_int(rank_elem);
    struct scr_hash_elem* file_elem;
    for (file_elem = scr_filemap_first_file(map, checkpoint_id, rank);
         file_elem != NULL;
         file_elem = scr_hash_elem_next(file_elem))
    {
      char* file = scr_hash_elem_key(file_elem);
      /* check that we can read the file */
      if (access(file, R_OK) < 0) { failed_read = 1; }

      /* check that we can read meta file for the file */
      struct scr_meta meta;
      if (scr_read_meta(file, &meta) != SCR_SUCCESS) {
        failed_read = 1;
      } else {
        /* TODO: check that filesizes match (use strtol while reading data) */
        /* check that the file is complete */
        if (!meta.complete) { failed_read = 1; }
      }
    }
  }

  /* if we failed to read a file, assume the set is incomplete */
  if (failed_read) {
    /* TODO: want to unlink all files in this case? */
    return SCR_FAILURE;
  }
  return SCR_SUCCESS;
}

/*
=========================================
File Copy Functions
=========================================
*/

/* TODO: could perhaps use O_DIRECT here as an optimization */
/* TODO: could apply compression/decompression here */
/* copy src_file (full path) to dest_path and return new full path in dest_file */
static int scr_copy_to(const char* src_file, const char* dest_path, char* dest_file, uLong* src_crc)
{
  /* split src_file into path and filename */
  char path[SCR_MAX_FILENAME];
  char name[SCR_MAX_FILENAME];
  scr_split_path(src_file, path, name);

  /* create dest_file using dest_path and filename */
  scr_build_path(dest_file, dest_path, name);

  /* open src_file for reading */
  int fd_src = scr_open(src_file, O_RDONLY);
  if (fd_src < 0) {
    scr_err("Opening file to copy: scr_open(%s) errno=%d %m @ %s:%d",
            src_file, errno, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* open dest_file for writing */
  int fd_dest = scr_open(dest_file, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
  if (fd_dest < 0) {
    scr_err("Opening file for writing: scr_open(%s) errno=%d %m @ %s:%d",
            dest_file, errno, __FILE__, __LINE__
    );
    scr_close(fd_src);
    return SCR_FAILURE;
  }

  /* TODO:
  posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED | POSIX_FADV_SEQUENTIAL)
  that tells the kernel that you don't ever need the pages
  from the file again, and it won't bother keeping them in the page cache.
  */
  posix_fadvise(fd_src,  0, 0, POSIX_FADV_DONTNEED | POSIX_FADV_SEQUENTIAL);
  posix_fadvise(fd_dest, 0, 0, POSIX_FADV_DONTNEED | POSIX_FADV_SEQUENTIAL);

  /* allocate buffer to read in file chunks */
  char* buf = (char*) malloc(scr_file_buf_size);
  if (buf == NULL) {
    scr_err("Allocating memory: malloc(%ld) errno=%d %m @ %s:%d",
            scr_file_buf_size, errno, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* initialize crc values */
  uLong crc = crc32(0L, Z_NULL, 0);

  /* write chunks */
  int copying = 1;
  while (copying) {
    int nread = scr_read(fd_src, buf, scr_file_buf_size);
    if (nread > 0) {
      if (scr_crc_on_flush) { crc = crc32(crc, (const Bytef*) buf, (uInt) nread); }
      scr_write(fd_dest, buf, nread);
    }
    if (nread < scr_file_buf_size) { copying = 0; }
  }

  /* free buffer */
  free(buf);

  /* close source and destination files */
  scr_close(fd_dest);
  scr_close(fd_src);

  *src_crc = crc;

  return SCR_SUCCESS;
}

/* scr_swap_files -- copy or move a file from one node to another
 * COPY_FILES
 *   if file_me != NULL, send file_me to rank_send, who will make a copy,
 *   copy file from rank_recv if there is one to receive
 * MOVE_FILES
 *   if file_me != NULL, move file_me to rank_send
 *   save file from rank_recv if there is one to receive
 *   To conserve space (i.e., RAM disc), if file_me exists,
 *   any incoming file will overwrite file_me in place, one block at a time.
 *   It is then truncated and renamed according the size and name of the incoming file,
 *   or it is deleted (moved) if there is no incoming file.
 */
static int scr_swap_files(int swap_type,
                  const char* file_send, struct scr_meta* meta_send, int rank_send,
                  const char* dir_recv,  int rank_recv, char* file_recv_out,
                  MPI_Comm comm)
{
  int rc = SCR_SUCCESS;
  int num_req;
  MPI_Request request[2];
  MPI_Status  status[2];
  int have_outgoing = 0, have_incoming = 0;
  char null_string[1] = "";

  if (rank_send != -1) { have_outgoing = 1; }
  if (rank_recv != -1) { have_incoming = 1; }

  /* if file_me is NULL, then there is no file to send (we'll send '\0' instead) */
  if (file_send == NULL) { file_send = null_string; }

  /* exchange file names with partners */
  char file_recv[SCR_MAX_FILENAME] = "";
  num_req = 0;
  if (have_incoming) {
    MPI_Irecv(file_recv, SCR_MAX_FILENAME, MPI_BYTE, rank_recv, 0, comm, &request[num_req]);
    num_req++;
  }
  if (have_outgoing) {
    MPI_Isend((char*)file_send, strlen(file_send)+1, MPI_BYTE, rank_send, 0, comm, &request[num_req]);
    num_req++;
  }
  MPI_Waitall(num_req, request, status);

  /* if file names are "", there is no file to send or receive */
  if (have_outgoing && strcmp(file_send,"") == 0) { have_outgoing = 0; }
  if (have_incoming && strcmp(file_recv,"") == 0) { have_incoming = 0; }

  /* define the path to store our partner's file */
  char file_recv_scr[SCR_MAX_FILENAME] = "";
  if (have_incoming) {
    /* create subdirectory */
    scr_mkdir(dir_recv, S_IRWXU);

    /* set full path to filename */
    char path[SCR_MAX_FILENAME]     = "";
    char filename[SCR_MAX_FILENAME] = "";
    scr_split_path(file_recv, path, filename);
    scr_build_path(file_recv_scr, dir_recv, filename);

    scr_dbg(2, "Receiving %s as %s", file_recv, file_recv_scr);

    /* remove the completion marker for partner's file */
    scr_incomplete(file_recv_scr);
  }

  /* allocate MPI send buffer */
  char *buf_send = NULL;
  if (have_outgoing) {
    buf_send = (char*) malloc(scr_mpi_buf_size);
    if (buf_send == NULL) {
      scr_err("Allocating memory: malloc(%ld) errno=%d %m @ %s:%d",
              scr_mpi_buf_size, errno, __FILE__, __LINE__
      );
      return SCR_FAILURE;
    }
  }

  /* allocate MPI recv buffer */
  char *buf_recv = NULL;
  if (have_incoming) {
    buf_recv = (char*) malloc(scr_mpi_buf_size);
    if (buf_recv == NULL) {
      scr_err("Allocating memory: malloc(%ld) errno=%d %m @ %s:%d",
              scr_mpi_buf_size, errno, __FILE__, __LINE__
      );
      return SCR_FAILURE;
    }
  }

  /* initialize crc values */
  uLong crc32_send = crc32(0L, Z_NULL, 0);
  uLong crc32_recv = crc32(0L, Z_NULL, 0);

  /* exchange files */
  if (swap_type == COPY_FILES) {
    /* open the files */
    int fd_send = -1, fd_recv = -1;
    if (have_outgoing) {
      /* open the file to send: read-only mode */
      fd_send = scr_open(file_send, O_RDONLY);
      if (fd_send < 0) {
        /* TODO: skip writes? */
        scr_err("Opening file for send: scr_open(%s, O_RDONLY) errno=%d %m @ %s:%d",
                file_send, errno, __FILE__, __LINE__
               );
      }
    }
    if (have_incoming) {
      /* open the file to recv: truncate, write-only mode */
      fd_recv = scr_open(file_recv_scr, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
      if (fd_recv < 0) {
        /* TODO: skip writes? */
        scr_err("Opening file for recv: scr_open(%s, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR) errno=%d %m @ %s:%d",
                file_recv_scr, errno, __FILE__, __LINE__
               );
      }
    }

    /* exchange file chunks */
    int nread, nwrite;
    int sending = 0, receiving = 0;
    if (have_outgoing) { sending   = 1; }
    if (have_incoming) { receiving = 1; }
    while (sending || receiving) {
      if (receiving) {
        MPI_Irecv(buf_recv, scr_mpi_buf_size, MPI_BYTE, rank_recv, 0, comm, &request[0]);
      }

      if (sending) {
        nread = scr_read(fd_send, buf_send, scr_mpi_buf_size);
        if (scr_crc_on_copy && nread > 0) { crc32_send = crc32(crc32_send, (const Bytef*) buf_send, (uInt) nread); }
        if (nread < 0) { nread = 0; }
        MPI_Isend(buf_send, nread, MPI_BYTE, rank_send, 0, comm, &request[1]);
        MPI_Wait(&request[1], &status[1]);
        if (nread < scr_mpi_buf_size) { sending = 0; }
      }

      if (receiving) {
        MPI_Wait(&request[0], &status[0]);
        MPI_Get_count(&status[0], MPI_BYTE, &nwrite);
        if (scr_crc_on_copy && nwrite > 0) { crc32_recv = crc32(crc32_recv, (const Bytef*) buf_recv, (uInt) nwrite); }
        scr_write(fd_recv, buf_recv, nwrite);
        if (nwrite < scr_mpi_buf_size) { receiving = 0; }
      }
    }

    /* close the files */
    if (have_outgoing) {
      /* don't need an fsync here (read-only) */
      scr_close(fd_send);
    }
    if (have_incoming) {
      scr_close(fd_recv);
    }

    /* set crc field on our file if it hasn't been set already */
    if (scr_crc_on_copy && have_outgoing) {
      if (!meta_send->crc32_computed) {
        meta_send->crc32_computed = 1;
        meta_send->crc32          = crc32_send;
        scr_complete(file_send, meta_send);
      } else {
        /* TODO: we could check that the crc on the sent file matches and take some action if not */
      }
    }
  } else if (swap_type == MOVE_FILES) {
    /* since we'll overwrite our send file in place with the recv file, which may be larger,
     * we need to keep track of how many bytes we've sent and whether we've sent them all */
    unsigned long filesize_send = 0;

    /* open our file */
    int fd = -1;
    if (have_outgoing) {
      /* we'll overwrite our send file (or just read it if there is no incoming) */
      filesize_send = scr_filesize(file_send);
      fd = scr_open(file_send, O_RDWR);
      if (fd < 0) {
        /* TODO: skip writes and return error? */
        scr_err("Opening file for send/recv: scr_open(%s, O_RDWR) errno=%d %m @ %s:%d",
                file_send, errno, __FILE__, __LINE__
               );
      }
    } else if (have_incoming) {
      /* we'll write our recv file from scratch */
      fd = scr_open(file_recv_scr, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
      if (fd < 0) {
        /* TODO: skip writes and return error? */
        scr_err("Opening file for recv: scr_open(%s, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR) errno=%d %m @ %s:%d",
                file_recv_scr, errno, __FILE__, __LINE__
               );
      }
    }

    /* exchange file chunks */
    int nread, nwrite;
    off_t read_pos = 0, write_pos = 0;
    int sending = 0, receiving = 0;
    if (have_outgoing) { sending   = 1; }
    if (have_incoming) { receiving = 1; }
    while (sending || receiving) {
      if (receiving) {
        /* prepare a buffer to receive up to scr_mpi_buf_size bytes */
        MPI_Irecv(buf_recv, scr_mpi_buf_size, MPI_BYTE, rank_recv, 0, comm, &request[0]);
      }

      if (sending) {
        /* compute number of bytes to read */
        unsigned long count = filesize_send - read_pos;
        if (count > scr_mpi_buf_size) { count = scr_mpi_buf_size; }

        /* read a chunk of up to scr_mpi_buf_size bytes into buf_send */
        lseek(fd, read_pos, SEEK_SET); /* seek to read position */
        nread = scr_read(fd, buf_send, count);
        if (scr_crc_on_copy && nread > 0) { crc32_send = crc32(crc32_send, (const Bytef*) buf_send, (uInt) nread); }
        if (nread < 0) { nread = 0; }
        read_pos += (off_t) nread; /* update read pointer */

        /* send chunk (if nread is smaller than scr_mpi_buf_size, then we've read the whole file) */
        MPI_Isend(buf_send, nread, MPI_BYTE, rank_send, 0, comm, &request[1]);
        MPI_Wait(&request[1], &status[1]);

        /* check whether we've read the whole file */
        if (filesize_send == read_pos && count < scr_mpi_buf_size) { sending = 0; }
      }

      if (receiving) {
        /* count the number of bytes received */
        MPI_Wait(&request[0], &status[0]);
        MPI_Get_count(&status[0], MPI_BYTE, &nwrite);
        if (scr_crc_on_copy && nwrite > 0) { crc32_recv = crc32(crc32_recv, (const Bytef*) buf_recv, (uInt) nwrite); }

        /* write those bytes to file (if nwrite is smaller than scr_mpi_buf_size, then we've received the whole file) */
        lseek(fd, write_pos, SEEK_SET); /* seek to write position */
        scr_write(fd, buf_recv, nwrite);
        write_pos += (off_t) nwrite; /* update write pointer */

        /* if nwrite is smaller than scr_mpi_buf_size, then assume we've received the whole file */
        if (nwrite < scr_mpi_buf_size) { receiving = 0; }
      }
    }

    /* close file and cleanup */
    if (have_outgoing && have_incoming) {
      /* sent and received a file; close it, truncate it to write size, rename it, and remove its completion marker */
      scr_close(fd);
      truncate(file_send, write_pos);
      rename(file_send, file_recv_scr);
      scr_incomplete(file_send);
    } else if (have_outgoing) {
      /* only sent a file; close it, delete it, and remove its completion marker */
      scr_close(fd);
      unlink(file_send);
      scr_incomplete(file_send);
    } else if (have_incoming) {
      /* only received a file; just need to close it */
      scr_close(fd);
    }

    if (scr_crc_on_copy && have_outgoing) {
      if (!meta_send->crc32_computed) {
        /* we transfer this meta data across below, so may as well update these fields so we can use them */
        meta_send->crc32_computed = 1;
        meta_send->crc32          = crc32_send;
        /* do not complete file send, just deleted it above */
      } else {
        /* TODO: we could check that the crc on the sent file matches and take some action if not */
      }
    }
  } else {
    scr_err("Unknown file transfer type: %d @ %s:%d", swap_type, __FILE__, __LINE__);
    return SCR_FAILURE;
  } /* end file copy / move */

  /* free the MPI buffers */
  if (have_outgoing) { free(buf_send); buf_send = NULL; }
  if (have_incoming) { free(buf_recv); buf_recv = NULL; }

  /* exchange meta file info with partners */
  struct scr_meta  meta;
  struct scr_meta* meta_recv = &meta;
  num_req = 0;
  if (have_incoming) {
    MPI_Irecv((void*) meta_recv, sizeof(struct scr_meta), MPI_BYTE, rank_recv, 0, comm, &request[num_req]);
    num_req++;
  }
  if (have_outgoing) {
    MPI_Isend((void*) meta_send, sizeof(struct scr_meta), MPI_BYTE, rank_send, 0, comm, &request[num_req]);
    num_req++;
  }
  MPI_Waitall(num_req, request, status);

  /* mark received file as complete */
  if (have_incoming) {
    /* check that our written file is the correct size */
    unsigned long filesize_wrote = scr_filesize(file_recv_scr);
    if (filesize_wrote < meta_recv->filesize) {
      meta_recv->complete = 0;
      rc = SCR_FAILURE;
    }

    /* check that there was no corruption in receiving the file */
    if (scr_crc_on_copy && meta_recv->crc32_computed && crc32_recv != meta_recv->crc32) {
      meta_recv->complete = 0;
      rc = SCR_FAILURE;
    }

    scr_complete(file_recv_scr, meta_recv);

    /* fill in the name of the received file, the caller may need it */
    if (file_recv_out != NULL) { strcpy(file_recv_out, file_recv_scr); }
  }

  return rc;
}

/* given a comm as input, find the left and right partner ranks and hostnames */
static int scr_set_partners(MPI_Comm comm, int dist,
                            int* lhs_rank, int* lhs_rank_world, char* lhs_hostname,
                            int* rhs_rank, int* rhs_rank_world, char* rhs_hostname
                           )
{
  /* find our position in the communicator */
  int my_rank, ranks;
  MPI_Comm_rank(comm, &my_rank);
  MPI_Comm_size(comm, &ranks);

  /* shift parter distance to a valid range */
  while (dist > ranks) { dist -= ranks; }
  while (dist < 0)     { dist += ranks; }

  /* compute ranks to our left and right partners */
  int lhs = (my_rank + ranks - dist) % ranks;
  int rhs = (my_rank + ranks + dist) % ranks;
  (*lhs_rank) = lhs;
  (*rhs_rank) = rhs;

  /* fetch hostnames from my left and right partners */
  strcpy(lhs_hostname, "");
  strcpy(rhs_hostname, "");

  MPI_Request request[2];
  MPI_Status  status[2];

  /* shift hostnames to the right */
  MPI_Irecv(lhs_hostname,    sizeof(scr_my_hostname), MPI_BYTE, lhs, 0, comm, &request[0]);
  MPI_Isend(scr_my_hostname, sizeof(scr_my_hostname), MPI_BYTE, rhs, 0, comm, &request[1]);
  MPI_Waitall(2, request, status);

  /* shift hostnames to the left */
  MPI_Irecv(rhs_hostname,    sizeof(scr_my_hostname), MPI_BYTE, rhs, 0, comm, &request[0]);
  MPI_Isend(scr_my_hostname, sizeof(scr_my_hostname), MPI_BYTE, lhs, 0, comm, &request[1]);
  MPI_Waitall(2, request, status);

  /* map ranks in comm to ranks in scr_comm_world */
  MPI_Group group, group_world;
  int lhs_world, rhs_world;
  MPI_Comm_group(comm, &group);
  MPI_Comm_group(scr_comm_world, &group_world);
  MPI_Group_translate_ranks(group, 1, &lhs, group_world, &lhs_world);
  MPI_Group_translate_ranks(group, 1, &rhs, group_world, &rhs_world);
  (*lhs_rank_world) = lhs_world;
  (*rhs_rank_world) = rhs_world;

  return SCR_SUCCESS;
}

/* copy files to a partner node */
static int scr_copy_partner(struct scr_filemap* map, int checkpoint_id)
{
  int rc = SCR_SUCCESS;
  int tmp_rc;

  /* first, determine the max number of files on our level comm to know how many times to call swap */
  MPI_Status status;
  int send_num = scr_filemap_num_files(map, checkpoint_id, scr_my_rank_world);
  int recv_num = 0;
  MPI_Sendrecv(&send_num, 1, MPI_INT, scr_rhs_rank, 0, &recv_num, 1, MPI_INT, scr_lhs_rank, 0, scr_comm_level, &status);

  /* record how many files our partner sent to us (need to be able to distinguish between 0 files and not knowing) */
  scr_filemap_set_expected_files(map, checkpoint_id, scr_lhs_rank_world, recv_num);

  /* define directory to receive partner file in */
  char ckpt_path[SCR_MAX_FILENAME];
  scr_checkpoint_dir(checkpoint_id, ckpt_path);

  /* point to our first file (gets set to NULL if we don't have any) */
  struct scr_hash_elem* file_elem = scr_filemap_first_file(map, checkpoint_id, scr_my_rank_world);

  /* for each potential file, step through a call to swap */
  while (send_num > 0 || recv_num > 0) {
    /* if we have a file to send, get the filename and read in its meta info */
    int send_rank = -1;
    int recv_rank = -1;
    char* file = NULL;
    struct scr_meta meta;
    if (send_num > 0) {
      file = scr_hash_elem_key(file_elem);
      scr_read_meta(file, &meta);
      file_elem = scr_hash_elem_next(file_elem);
      send_rank = scr_rhs_rank;
      send_num--;

      /* TODO: this should really go in the distribute phase somewhere, but there's nice symmetry here */
      /* remove the partner tag from our file (may have a marker from a distribute) */
      scr_filemap_unset_tag(map, checkpoint_id, scr_my_rank_world, file, "Partner");
    }
    if (recv_num > 0) {
      recv_rank = scr_lhs_rank;
      recv_num--;
    }

    /* exhange files with left and right side partners */
    char file_partner[SCR_MAX_FILENAME];
    tmp_rc = scr_swap_files(COPY_FILES, file, &meta, send_rank, ckpt_path, recv_rank, file_partner, scr_comm_level);
    if (tmp_rc != SCR_SUCCESS) { rc = tmp_rc; }

    /* if our partner sent us a file, add it to our filemap using its COMM_WORLD rank */
    if (recv_rank != -1 && strcmp(file_partner, "") != 0) {
      tmp_rc = scr_filemap_add_file(map, checkpoint_id, scr_lhs_rank_world, file_partner);
      if (tmp_rc != SCR_SUCCESS) { rc = tmp_rc; }

      /* remember the hostname that gave us this file (needed for drain) */
      tmp_rc = scr_filemap_set_tag(map, checkpoint_id, scr_lhs_rank_world, file_partner, "Partner", scr_lhs_hostname);
      if (tmp_rc != SCR_SUCCESS) { rc = tmp_rc; }

      /* TODO: tag which node our partners files are from in the filemap */
      /* scr_lhs_hostname */
    }
  }

  return rc;
}

/* TODO: abstract this in someway so it can be moved to scr_copy_xor */
/* set the ranks array in the header
 * (We implement this function here rather in scr_copy_xor.c,
 * so that the serial rebuild program does not need to include MPI.) */
static int scr_copy_xor_header_set_ranks(struct scr_copy_xor_header* h, MPI_Comm comm, MPI_Comm comm_world)
{
  MPI_Comm_size(comm_world, &h->nranks);

  /* get the size of the xor communicator, and fill in the ranks */
  int i;
  MPI_Comm_size(comm, &h->xor_nranks);
  if (h->xor_nranks > 0) {
    h->xor_ranks  = (int*) malloc(h->xor_nranks * sizeof(int));
    /* TODO: check for null */

    /* map ranks in comm to ranks in scr_comm_world */
    MPI_Group group, group_world;
    MPI_Comm_group(comm, &group);
    MPI_Comm_group(comm_world, &group_world);
    for (i=0; i < h->xor_nranks; i++) {
      MPI_Group_translate_ranks(group, 1, &i, group_world, &(h->xor_ranks[i]));
    }
  } else {
    h->xor_ranks = NULL;
  }

  return SCR_SUCCESS;
}

/* reduce-scatter XOR file of checkpoint files of ranks in same XOR set */
static int scr_copy_xor(struct scr_filemap* map, int checkpoint_id)
{
  int rc = SCR_SUCCESS;
  int tmp_rc;

  /* allocate buffer to read a piece of my file */
  char* send_buf = (char*) malloc(scr_mpi_buf_size);
  if (send_buf == NULL) {
    scr_abort(-1, "Allocating memory for send buffer: malloc(%d) errno=%d %m @ %s:%d",
            scr_mpi_buf_size, errno, __FILE__, __LINE__
    );
  }

  /* allocate buffer to read a piece of the recevied chunk file */
  char* recv_buf = (char*) malloc(scr_mpi_buf_size);
  if (recv_buf == NULL) {
    scr_abort(-1, "Allocating memory for recv buffer: malloc(%d) errno=%d %m @ %s:%d",
            scr_mpi_buf_size, errno, __FILE__, __LINE__
    );
  }

  /* count the number of files I have and allocate space in structures for each of them */
  int num_files = scr_filemap_num_files(map, checkpoint_id, scr_my_rank_world);
  int* fds = NULL;
  unsigned long* filesizes = NULL;
  if (num_files > 0) {
    fds = (int*) malloc(num_files * sizeof(int));
    filesizes = (unsigned long*) malloc(num_files * sizeof(unsigned long));
  }

  struct scr_copy_xor_header h;
  scr_copy_xor_header_set_ranks(&h, scr_comm_xor, scr_comm_world);
  scr_copy_xor_header_alloc_my_files(&h, scr_my_rank_world, num_files);

  /* open each file, get the filesize of each, and read the meta data of each */
  int i = 0;
  unsigned long my_bytes = 0;
  struct scr_hash_elem* file_elem;
  for (file_elem = scr_filemap_first_file(map, checkpoint_id, scr_my_rank_world);
       file_elem != NULL;
       file_elem = scr_hash_elem_next(file_elem))
  {
    /* get the filename */
    char* file = scr_hash_elem_key(file_elem);

    /* get the filesize of this file and add the byte count to the total */
    filesizes[i] = scr_filesize(file);
    my_bytes += filesizes[i];

    /* read the meta for this file */
    scr_read_meta(file, &(h.my_files[i]));

    /* open the file */
    fds[i]  = scr_open(file, O_RDONLY);
    if (fds[i] < 0) {
      /* TODO: try again? */
      scr_abort(-1, "Opening checkpoint file for copying: scr_open(%s, O_RDONLY) errno=%d %m @ %s:%d",
                file, errno, __FILE__, __LINE__
      );
    }

    i++;
  }

  /* allreduce to get maximum filesize */
  unsigned long max_bytes;
  MPI_Allreduce(&my_bytes, &max_bytes, 1, MPI_UNSIGNED_LONG, MPI_MAX, scr_comm_xor);

  /* TODO: use unsigned long integer arithmetic (with proper byte padding) instead of char to speed things up */

  /* compute chunk size according to maximum file length and number of ranks in xor set */
  /* if filesize doesn't divide evenly, then add one byte to chunk_size */
  /* TODO: check that scr_ranks_xor is > 1 for this divide to be safe (or at partner selection time) */
  size_t chunk_size = max_bytes / (unsigned long) (scr_ranks_xor - 1);
  if ((scr_ranks_xor - 1) * chunk_size < max_bytes) {
    chunk_size++;
  }

  /* TODO: need something like this to handle 0-byte files? */
  if (chunk_size == 0) {
    chunk_size++;
  }

  /* record the checkpoint_id and the chunk size in the xor chunk header */
  h.checkpoint_id = checkpoint_id;
  h.chunk_size    = chunk_size;

  /* set chunk filenames of form:  <xor_rank+1>_of_<xor_ranks>_in_<local_id>_<set_id>.xor */
  char my_chunk_file[SCR_MAX_FILENAME];
  char ckpt_path[SCR_MAX_FILENAME];
  scr_checkpoint_dir(checkpoint_id, ckpt_path);
  sprintf(my_chunk_file,  "%s/%d_of_%d_in_%dx%d.xor",
          ckpt_path, scr_my_rank_xor+1, scr_ranks_xor, scr_my_rank_local, scr_xor_set_id);

  /* open my chunk file */
  int fd_chunk = scr_open(my_chunk_file, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
  if (fd_chunk < 0) {
    /* TODO: try again? */
    scr_abort(-1, "Opening XOR chunk file for writing: scr_open(%s) errno=%d %m @ %s:%d",
            my_chunk_file, errno, __FILE__, __LINE__
    );
  }

  MPI_Request request[2];
  MPI_Status  status[2];

  /* tell right hand side partner how many files we have */
  int num_files_lhs = 0;
  MPI_Irecv(&num_files_lhs, 1, MPI_INT, scr_lhs_rank, 0, scr_comm_xor, &request[0]);
  MPI_Isend(&num_files,     1, MPI_INT, scr_rhs_rank, 0, scr_comm_xor, &request[1]);
  MPI_Waitall(2, request, status);
  scr_copy_xor_header_alloc_partner_files(&h, scr_lhs_rank_world, num_files_lhs);

  /* exchange meta with our partners */
  /* TODO: this is part of the meta data, but it will take significant work to change scr_meta */
  MPI_Irecv(h.partner_files, h.partner_nfiles * sizeof(struct scr_meta), MPI_BYTE, scr_lhs_rank, 0, scr_comm_xor, &request[0]);
  MPI_Isend(h.my_files,      h.my_nfiles      * sizeof(struct scr_meta), MPI_BYTE, scr_rhs_rank, 0, scr_comm_xor, &request[1]);
  MPI_Waitall(2, request, status);

  /* write out the xor chunk header */
  scr_copy_xor_header_write(fd_chunk, &h);

  /* XOR Reduce_scatter */
  size_t nread = 0;
  while (nread < chunk_size) {
    size_t count = chunk_size - nread;
    if (count > scr_mpi_buf_size) {
      count = scr_mpi_buf_size;
    }

    int chunk_id;
    for(chunk_id = scr_ranks_xor-1; chunk_id >= 0; chunk_id--) {
      /* read the next set of bytes for this chunk from my file into send_buf */
      if (chunk_id > 0) {
        int chunk_id_rel = (scr_my_rank_xor + scr_ranks_xor + chunk_id) % scr_ranks_xor;
        if (chunk_id_rel > scr_my_rank_xor) {
          chunk_id_rel--;
        }
        unsigned long offset = chunk_size * (unsigned long) chunk_id_rel + nread;
        scr_read_pad_n(num_files, fds, send_buf, count, offset, filesizes);
      } else {
        memset(send_buf, 0, count);
      }

      /* TODO: XORing with unsigned long would be faster here (if chunk size is multiple of this size) */
      /* merge the blocks via xor operation */
      if (chunk_id < scr_ranks_xor-1) {
        int i;
        for (i = 0; i < count; i++) {
          send_buf[i] ^= recv_buf[i];
        }
      }

      if (chunk_id > 0) {
        /* not our chunk to write, forward it on and get the next */
        MPI_Irecv(recv_buf, count, MPI_BYTE, scr_lhs_rank, 0, scr_comm_xor, &request[0]);
        MPI_Isend(send_buf, count, MPI_BYTE, scr_rhs_rank, 0, scr_comm_xor, &request[1]);
        MPI_Waitall(2, request, status);
      } else {
        /* write send block to send chunk file */
        scr_write(fd_chunk, send_buf, count);
      }
    }

    nread += count;
  }

  /* close my chunkfile, with fsync */
  scr_close(fd_chunk);

  /* close my checkpoint files */
  for (i=0; i < num_files; i++) {
    scr_close(fds[i]);
  }

  /* free the buffers */
  scr_copy_xor_header_free(&h);
  if (filesizes != NULL) { free(filesizes); filesizes = NULL; }
  if (fds       != NULL) { free(fds);       fds       = NULL; }
  free(send_buf);
  free(recv_buf);

  /* TODO: need to check for errors */
  /* write meta file for xor chunk */
  struct scr_meta meta;
  scr_set_meta(&meta, my_chunk_file, scr_my_rank_world, scr_ranks_world, checkpoint_id, SCR_FILE_XOR, 1);
  scr_complete(my_chunk_file, &meta);

  /* add the chunk file to our filemap */
  tmp_rc = scr_filemap_add_file(map, checkpoint_id, scr_my_rank_world, my_chunk_file);
  if (tmp_rc != SCR_SUCCESS) {
    rc = tmp_rc;
  }
  /* TODO: tag file based on its filetype? */

  return rc;
}

/* apply redundancy scheme to file and return number of bytes copied in bytes parameter */
int scr_copy_files(int checkpoint_id, double* bytes)
{
  /* initialize to 0 */
  *bytes = 0.0;

  /* read in the filemap */
  struct scr_filemap* map = scr_filemap_new();
  scr_filemap_read(scr_map_file, map);

  /* step through each of my files for the latest checkpoint to scan for any incomplete files */
  int valid = 1;
  double my_bytes = 0.0;
  struct scr_hash_elem* file_elem;
  for (file_elem = scr_filemap_first_file(map, checkpoint_id, scr_my_rank_world);
       file_elem != NULL;
       file_elem = scr_hash_elem_next(file_elem))
  {
    /* get the filename */
    char* file = scr_hash_elem_key(file_elem);

    /* check the file */
    if (!scr_bool_have_file(file, checkpoint_id, scr_my_rank_world)) {
      scr_dbg(2, "scr_copy_files: File determined to be invalid: %s", file);
      valid = 0;
    }

    /* add up the number of bytes on our way through */
    my_bytes += (double) scr_filesize(file);
  }

  /* determine whether everyone's file is good */
  int all_valid = scr_alltrue(valid);
  if (!all_valid) {
    if (scr_my_rank_world == 0) {
      scr_dbg(1, "scr_copy_files: Exiting copy since one or more checkpoint files is invalid");
    }
    scr_filemap_delete(map);
    return SCR_FAILURE;
  }

  /* start timer;
   * since this is after the allreduce, it gives a more acurate measure of
   * the true copy bandwidth by excluding straggler effects */
  time_t timestamp_start;
  double time_start;
  if (scr_my_rank_world == 0) {
    timestamp_start = scr_log_seconds();
    time_start = MPI_Wtime();
  }

  /* apply the redundancy scheme */
  int rc = SCR_FAILURE;
  switch (scr_copy_type) {
  case SCR_COPY_LOCAL:
    rc = SCR_SUCCESS;
    break;
  case SCR_COPY_PARTNER:
    rc = scr_copy_partner(map, checkpoint_id);
    break;
  case SCR_COPY_XOR:
    rc = scr_copy_xor(map, checkpoint_id);
    break;
  }

  /* record the number of files this task wrote during this checkpoint (needed to remember when a task writes 0 files) */
  scr_filemap_set_expected_files(map, checkpoint_id, scr_my_rank_world, scr_filemap_num_files(map, checkpoint_id, scr_my_rank_world));

  /* write out the new filemap which may include new files via the copy */
  int tmp_rc = scr_filemap_write(scr_map_file, map);
  if (tmp_rc != SCR_SUCCESS) {
    rc = tmp_rc;
  }

  /* determine whether everyone succeeded in their copy */
  int valid_copy = (rc == SCR_SUCCESS);
  if (!valid_copy) {
    scr_err("src_copy_files failed with return code %d @ %s:%d", rc, __FILE__, __LINE__);
  }
  int all_valid_copy = scr_alltrue(valid_copy);
  rc = all_valid_copy ? SCR_SUCCESS : SCR_FAILURE;

  /* add up total number of bytes */
  MPI_Allreduce(&my_bytes, bytes, 1, MPI_DOUBLE, MPI_SUM, scr_comm_world);

  /* stop timer and report performance info */
  if (scr_my_rank_world == 0) {
    double time_end = MPI_Wtime();
    double time_diff = time_end - time_start;
    double bw = *bytes / (1024.0 * 1024.0 * time_diff);
    scr_dbg(1, "scr_copy_files: seconds %f, bytes %e, MB/sec %f, per proc MB/sec %f",
            time_diff, *bytes, bw, bw/scr_ranks_world
    );

    /* log data on the copy in the database */
    if (scr_log_enable) {
      char ckpt_path[SCR_MAX_FILENAME];
      scr_checkpoint_dir(checkpoint_id, ckpt_path);
      scr_log_transfer("COPY", ckpt_path, ckpt_path, &scr_checkpoint_id, &timestamp_start, &time_diff, bytes);
    }
  }

  /* free map object */
  scr_filemap_delete(map);

  return rc;
}

/*
=========================================
Flush and fetch functions
=========================================
*/

/* read in scr_summary.txt file from dir */
static int scr_read_summary(const char* dir, int* num_files, struct scr_meta** v)
{
  /* initialize num_files and the data pointer */
  *num_files = 0;
  *v = NULL;

  /* check whether we can read the summary file */
  char file[SCR_MAX_FILENAME];
  scr_build_path(file, dir, "scr_summary.txt");
  if (access(file, R_OK) < 0) { return SCR_FAILURE; }

  /* TODO: copy summary file to cache for faster access? */

  /* open the summary file */
  FILE* fs = fopen(file, "r");
  if (fs == NULL) {
    scr_err("Opening summary file for read: fopen(%s, \"r\") errno=%d %m @ %s:%d",
            file, errno, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* assume we have one file per rank */
  *num_files = scr_ranks_world;

  /* read the first line (all versions have at least one header line) */
  int linenum = 0;
  char line[2048];
  char field[2048];
  fgets(line, sizeof(line), fs);
  linenum++;

  /* get the summary file version number, if no number, assume version=1 */
  int version = 1;
  sscanf(line, "%s", field);
  if (strcmp(field, "Version:") == 0) {
    sscanf(line, "%s %d", field, &version);
  }

  /* all versions greater than 1, have two header lines, read and throw away the second */
  if (version > 1) {
    /* version 3 and higher writes the number of rows in the file (ranks may write 0 or more files) */
    if (version >= 3) {
      fgets(line, sizeof(line), fs);
      linenum++;
      sscanf(line, "%s %d", field, num_files);
    }
    fgets(line, sizeof(line), fs);
    linenum++;
  }

  /* now we know how many records we'll be reading, so allocate space for them */
  struct scr_meta* data = NULL;
  if (*num_files > 0) {
    data = (struct scr_meta*) malloc(*num_files * sizeof(struct scr_meta));
    if (data == NULL) {
      *num_files = 0;
      scr_err("Could not allocate space to read in summary file %s, which has %d records.", file, *num_files);
      fclose(fs);
      return SCR_FAILURE;
    }
  } else {
    *num_files = 0;
    scr_err("No file records found in summary file %s, perhaps it is corrupt or incomplete.", file);
    fclose(fs);
    return SCR_FAILURE;
  }

  /* read the record for each rank */
  int bad_values = 0;
  int i;
  for(i=0; i < *num_files; i++) {
    int expected_n, n;
    int rank, scr, ranks, pattern, checkpoint_id, complete, match_filesize;
    char filename[SCR_MAX_FILENAME];
    unsigned long exp_filesize, filesize;
    int crc_computed = 0;
    uLong crc = 0UL;

    /* read a line from the file, parse depending on version */
    if (version == 1) {
      expected_n = 10;
      n = fscanf(fs, "%d\t%d\t%d\t%d\t%d\t%d\t%lu\t%d\t%lu\t%s\n",
                 &rank, &scr, &ranks, &pattern, &checkpoint_id, &complete, &exp_filesize, &match_filesize, &filesize, filename
          );
      linenum++;
    } else {
      expected_n = 11;
      n = fscanf(fs, "%d\t%d\t%d\t%d\t%d\t%lu\t%d\t%lu\t%s\t%d\t0x%lx\n",
                 &rank, &scr, &ranks, &checkpoint_id, &complete, &exp_filesize, &match_filesize, &filesize, filename,
                 &crc_computed, &crc
          );
      linenum++;
    }

    /* check the return code returned from the read */
    if (n == EOF) {
      scr_err("Early EOF in summary file %s at line %d.  Only read %d of %d expected records.", file, linenum, i, *num_files);
      *num_files = 0;
      if (data != NULL) { free(data); data = NULL; }
      fclose(fs);
      return SCR_FAILURE;
    } else if (n != expected_n) {
      scr_err("Invalid read of record %d in %s at line %d.", i, file, linenum);
      *num_files = 0;
      if (data != NULL) { free(data); data = NULL; }
      fclose(fs);
      return SCR_FAILURE;
    }
    scr_dbg(2, "scr_summary.txt: rank %d file %s", rank, filename);

    /* TODO: check whether all files are complete, match expected size, number of ranks, checkpoint_id, etc */
    if (rank < 0 || rank >= scr_ranks_world) {
      bad_values = 1;
      scr_err("Invalid rank detected (%d) in a job with %d tasks in %s at line %d.",
              rank, scr_my_rank_world, file, linenum
      );
    }

    /* TODO: don't need to store entire path
    char path[SCR_MAX_FILENAME], name[SCR_MAX_FILENAME];
    scr_split_path(filename, path, name);
    printf("%d %s %s\n", rank, path, name);
    */

    /* chop to basename of filename */
    char* base = basename(filename);

    data[i].rank          = rank;
    data[i].ranks         = ranks;
    data[i].checkpoint_id = checkpoint_id;
    data[i].filetype      = SCR_FILE_FULL;

    strcpy(data[i].filename, base);
    data[i].filesize       = exp_filesize;
    data[i].complete       = complete;
    data[i].crc32_computed = crc_computed;
    data[i].crc32          = crc;

    strcpy(data[i].src_filename, base);
    data[i].src_filesize       = exp_filesize;
    data[i].src_complete       = complete;
    data[i].src_crc32_computed = 0;
    data[i].src_crc32          = crc32(0L, Z_NULL, 0);
  }

  /* close the file */
  fclose(fs);

  /* if we found any problems while reading the file, free the memory and return with an error */
  if (bad_values) {
    *num_files = 0;
    if (data != NULL) { free(data); data = NULL; }
    return SCR_FAILURE;
  }

  /* otherwise, update the caller's pointer and return */
  *v = data;
  return SCR_SUCCESS;
}

/* write out scr_summary.txt file to dir */
static int scr_write_summary(const char* dir, int num_files, const struct scr_meta* data)
{
  /* build the filename */
  char file[SCR_MAX_FILENAME];
  scr_build_path(file, dir, "scr_summary.txt");

  /* open the summary file */
  FILE* fs = fopen(file, "w");
  if (fs == NULL) {
    scr_err("Opening summary file for writing: fopen(%s, \"w\") errno=%d %m @ %s:%d",
            file, errno, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* write the file version */
  fprintf(fs, "Version: %d\n", SCR_SUMMARY_FILE_VERSION_3);

  /* write the number of rows */
  fprintf(fs, "Rows: %d\n", num_files);

  /* write the header */
  fprintf(fs, "Rank\tSCR\tRanks\tID\tComp\tExpSize\tExists\tSize\tFilename\tCRC\tCRC32\n");

  /* read the record for each rank */
  int i;
  for(i=0; i < num_files; i++) {
    fprintf(fs, "%d\t%d\t%d\t%d\t%d\t%lu\t%d\t%lu\t%s\t%d\t0x%lx\n",
               data[i].rank, 1, data[i].ranks, data[i].checkpoint_id,
               data[i].complete, data[i].filesize, 1, data[i].filesize, data[i].filename,
               data[i].crc32_computed, data[i].crc32
              );
  }

  /* close the file */
  fclose(fs);

  return SCR_SUCCESS;
}

/* returns true if the given checkpoint id needs to be flushed */
static int scr_bool_need_flush(int checkpoint_id)
{
  int need_flush = 0;

  if (scr_my_rank_local == 0) {
    /* read the flush file */
    struct scr_hash* hash = scr_hash_new();
    scr_hash_read(scr_flush_file, hash);

    /* if we have the checkpoint in cache, but not on the parallel file system, then it needs to be flushed */
    char str_ckpt[SCR_MAX_FILENAME];
    sprintf(str_ckpt, "%d", scr_checkpoint_id);
    struct scr_hash* in_cache = scr_hash_get_kv(hash, str_ckpt, SCR_FLUSH_CACHE);
    struct scr_hash* in_pfs   = scr_hash_get_kv(hash, str_ckpt, SCR_FLUSH_PFS);
    if (in_cache != NULL && in_pfs == NULL) {
      need_flush = 1;
    }

    /* free the hash object */
    scr_hash_delete(hash);
  }
  MPI_Bcast(&need_flush, 1, MPI_INT, 0, scr_comm_local);

  return need_flush;
}

/* indicates whether checkpoint cache has a complete and valid checkpoint set which should be flushed */
static int scr_set_flush(int checkpoint_id, const char* location)
{
  /* all master tasks write this file to their node */
  if (scr_my_rank_local == 0) {
    char str_ckpt[SCR_MAX_FILENAME];
    sprintf(str_ckpt, "%d", checkpoint_id);
    struct scr_hash* hash = scr_hash_new();
    scr_hash_read(scr_flush_file, hash);
    scr_hash_set_kv(hash, str_ckpt, location);
    scr_hash_write(scr_flush_file, hash);
    scr_hash_delete(hash);
  }
  return SCR_SUCCESS;
}

/* fetch file name in meta from dir and build new full path in newfile, return whether operation succeeded */
static int scr_fetch_a_file(const char* src_dir, const struct scr_meta* meta, const char* dst_dir, char* newfile)
{
  int success = SCR_SUCCESS;

  /* build full path to file */
  char filename[SCR_MAX_FILENAME];
  scr_build_path(filename, src_dir, meta->filename);

  /* fetch file */
  uLong crc;
  success = scr_copy_to(filename, dst_dir, newfile, &crc);

  /* check that crc matches crc stored in meta */
  if (success == SCR_SUCCESS && meta->crc32_computed && crc != meta->crc32) {
    success = SCR_FAILURE;
    scr_err("scr_fetch_a_file: CRC32 mismatch detected when fetching file from %s to %s @ %s:%d",
            filename, newfile, __FILE__, __LINE__
           );
    /* delete the file -- it's corrupted */
    unlink(newfile);
    /* TODO: would be good to log this, but right now only rank 0 can write log entries */
    /*
    if (scr_log_enable) {
      time_t now = scr_log_seconds();
      scr_log_event("CRC32 MISMATCH", filename, NULL, &now, NULL);
    }
    */
  }

  return success;
}

/* fetch files from parallel file system */
static int scr_fetch_files(const char* dir)
{
  int i, j;
  int rc = SCR_SUCCESS;
  int* num_files     = NULL;
  int* offset_files  = NULL;
  int checkpoint_id  = -1;
  double total_bytes = 0.0;

  /* start timer */
  time_t timestamp_start;
  double time_start;
  if (scr_my_rank_world == 0) {
    timestamp_start = scr_log_seconds();
    time_start = MPI_Wtime();
  }

  /* have rank 0 read summary file, if it exists */
  int read_summary = SCR_FAILURE;
  int total_files = 0;
  struct scr_meta* data = NULL;
  char fetch_dir[SCR_MAX_FILENAME];
  char fetch_dir_target[SCR_MAX_FILENAME];
  if (scr_my_rank_world == 0) {
    /* this may take a while, so tell user what we're doing */
    scr_dbg(1, "scr_fetch_files: Initiating fetch");

    /* build the fetch directory path */
    strcpy(fetch_dir, dir);
    if (access(fetch_dir, R_OK) == 0) {
      /* tell user where the fetch is coming from */
      char fetch_target[SCR_MAX_FILENAME];
      int fetch_target_len = readlink(fetch_dir, fetch_target, sizeof(fetch_target));
      if (fetch_target_len >= 0) {
        fetch_target[fetch_target_len] = '\0';
        snprintf(fetch_dir_target, sizeof(fetch_dir_target), "%s --> %s", fetch_dir, fetch_target);
      } else {
        snprintf(fetch_dir_target, sizeof(fetch_dir_target), "%s", fetch_dir);
      }
      if (scr_log_enable) {
        time_t now = scr_log_seconds();
        scr_log_event("FETCH STARTED", fetch_dir_target, NULL, &now, NULL);
      }
      scr_dbg(1, "scr_fetch_files: Fetching from %s", fetch_dir_target);

      /* read data from the summary file */
      read_summary = scr_read_summary(fetch_dir, &total_files, &data);

      /* allocate arrays to hold number of files and offset for each rank */
      num_files    = (int*) malloc(scr_ranks_world * sizeof(int));
      offset_files = (int*) malloc(scr_ranks_world * sizeof(int));
      if (num_files == NULL || offset_files == NULL) {
        scr_err("scr_fetch_files: Failed to allocate memory for scatter @ %s:%d",
                __FILE__, __LINE__
        );
        read_summary = SCR_FAILURE;
      } else {
        /* initialize these arrays to zero */
        for (i=0; i < scr_ranks_world; i++) {
          num_files[i]    = 0;
          offset_files[i] = 0;
        }

        /* step through each rows in the data to determine number and offset per rank */
        int curr_rank = -1;
        for (i=0; i < total_files; i++) {
          int next_rank = data[i].rank;
          if (next_rank != curr_rank) {
            /* check order of ranks in file */
            if (next_rank < curr_rank) {
              scr_err("scr_fetch_files: Unexpected rank order in summary file got %d expected something over %d @ %s:%d",
                      next_rank, curr_rank, __FILE__, __LINE__
              );
              read_summary = SCR_FAILURE;
            }
            curr_rank = next_rank;
            offset_files[curr_rank] = i;
          }
          num_files[curr_rank]++;
          checkpoint_id = data[i].checkpoint_id;
          total_bytes += (double) data[i].filesize;
        }
      }
    } else {
      scr_err("scr_fetch_files: Failed to access directory %s @ %s:%d", fetch_dir, __FILE__, __LINE__);
    }
  }

  /* broadcast whether the summary file was read successfully */
  MPI_Bcast(&read_summary, 1, MPI_INT, 0, scr_comm_world);
  if (read_summary != SCR_SUCCESS) {
    if (scr_my_rank_world == 0) {
      if (data != NULL) { free(data); data = NULL; }
      scr_dbg(1, "scr_fetch_files: Failed to read summary file @ %s:%d", __FILE__, __LINE__);
      if (scr_log_enable) {
        time_t now = scr_log_seconds();
        scr_log_event("FETCH FAILED", fetch_dir_target, NULL, &now, NULL);
      }
    }
    return SCR_FAILURE;
  }

  /* broadcast fetch directory */
  int dirsize = 0;
  if (scr_my_rank_world == 0) {
    dirsize = strlen(fetch_dir) + 1;
  }
  MPI_Bcast(&dirsize, 1, MPI_INT, 0, scr_comm_world);
  MPI_Bcast(fetch_dir, dirsize, MPI_BYTE, 0, scr_comm_world);

  /* broadcast the checkpoint id */
  MPI_Bcast(&checkpoint_id, 1, MPI_INT, 0, scr_comm_world);

  /* scatter the number of files per rank */
  int my_num_files = 0;
  MPI_Scatter(num_files, 1, MPI_INT, &my_num_files, 1, MPI_INT, 0, scr_comm_world);

  /* delete any existing checkpoint files for this checkpoint id (do this before filemap_read) */
  scr_unlink_checkpoint(checkpoint_id);
  scr_remove_checkpoint_dir(checkpoint_id);
  scr_remove_checkpoint_flush(checkpoint_id);
  scr_create_checkpoint_dir(checkpoint_id);

  /* read the filemap */
  struct scr_filemap* map = scr_filemap_new();
  scr_filemap_read(scr_map_file, map);

  /* TODO: have each process write its scr_filemap during flush and read this back on fetch */

  /* flow control rate of file reads from rank 0 by scattering file names to processes */
  int success = 1;
  if (scr_my_rank_world == 0) {
    scr_filemap_set_expected_files(map, checkpoint_id, scr_my_rank_world, my_num_files);
    for (j=0; j < my_num_files; j++) {
      /* copy my meta from data into a local struct */
      struct scr_meta meta;
      scr_copy_meta(&meta, &data[j]);

      /* fetch file */
      char newfile[SCR_MAX_FILENAME];
      char ckpt_path[SCR_MAX_FILENAME];
      scr_checkpoint_dir(checkpoint_id, ckpt_path);
      if (scr_fetch_a_file(fetch_dir, &meta, ckpt_path, newfile) != SCR_SUCCESS) {
        success = 0;
      }

      /* mark the file as complete */
      scr_complete(newfile, &meta);
      
      /* add the file to our filemap */
      scr_filemap_add_file(map, checkpoint_id, scr_my_rank_world, newfile);
      /* TODO: tag file based on its filetype? */
    }

    /* now, have a sliding window of w processes read simultaneously */
    int w = scr_fetch_width;
    if (w > scr_ranks_world-1) { w = scr_ranks_world-1; }

    /* allocate MPI_Request arrays and an array of ints */
    int* done = (int*) malloc(w * sizeof(int));
    MPI_Request* req_recv = (MPI_Request*) malloc(w * sizeof(MPI_Request));
    MPI_Request* req_send = (MPI_Request*) malloc(w * sizeof(MPI_Request));
    MPI_Status status;
    if (done == NULL || req_recv == NULL || req_send == NULL) {
      scr_err("scr_fetch_files: Failed to allocate memory for flow control @ %s:%d", __FILE__, __LINE__);
      if (done     != NULL) { free(done);     done     = NULL; }
      if (req_recv != NULL) { free(req_recv); req_recv = NULL; }
      if (req_send != NULL) { free(req_send); req_send = NULL; }
      MPI_Abort(1, scr_comm_world);
      exit(1);
    }

    int outstanding = 0;
    int index = 0;
    i = 1;
    while (i < scr_ranks_world || outstanding > 0) {
      /* issue up to w outstanding sends and receives */
      while (i < scr_ranks_world && outstanding < w) {
        /* post a receive for the response message we'll get back when rank i is done */
        MPI_Irecv(&done[index], 1, MPI_INT, i, 0, scr_comm_world, &req_recv[index]);

        /* post a send to tell rank i to start */
        MPI_Isend(&data[offset_files[i]], num_files[i] * sizeof(struct scr_meta), MPI_BYTE, i, 0, scr_comm_world, &req_send[index]);

        /* update the number of outstanding requests */
        i++;
        outstanding++;
        index++;
      }

      /* wait to hear back from any rank */
      MPI_Waitany(w, req_recv, &index, &status);

      /* once we hear back from a rank, the send to that rank should also be done */
      MPI_Wait(&req_send[index], &status);

      /* TODO: want to check success code from processes here (e.g., we could abort read early if rank 1 has trouble?) */

      /* one less request outstanding now */
      outstanding--;
    }

    /* free the MPI_Request arrays */
    free(done);     done     = NULL;
    free(req_recv); req_recv = NULL;
    free(req_send); req_send = NULL;

    /* free the summary data buffer */
    free(num_files);    num_files    = NULL;
    free(offset_files); offset_files = NULL;
    free(data);         data         = NULL;
  } else {
    data = (struct scr_meta*) malloc(my_num_files * sizeof(struct scr_meta));
    if (data == NULL) {
      scr_err("scr_fetch_files: Failed to allocate memory to receive file meta @ %s:%d", __FILE__, __LINE__);
      MPI_Abort(1, scr_comm_world);
      exit(1);
    }

    /* receive meta data info for my file from rank 0 */
    MPI_Status status;
    MPI_Recv(data, my_num_files * sizeof(struct scr_meta), MPI_BYTE, 0, 0, scr_comm_world, &status);

    scr_filemap_set_expected_files(map, checkpoint_id, scr_my_rank_world, my_num_files);
    for (j=0; j < my_num_files; j++) {
      /* copy my meta from data into a local struct */
      struct scr_meta meta;
      scr_copy_meta(&meta, &data[j]);

      /* fetch file */
      char newfile[SCR_MAX_FILENAME];
      char ckpt_path[SCR_MAX_FILENAME];
      scr_checkpoint_dir(checkpoint_id, ckpt_path);
      if (scr_fetch_a_file(fetch_dir, &meta, ckpt_path, newfile) != SCR_SUCCESS) {
        success = 0;
      }

      /* mark the file as complete */
      scr_complete(newfile, &meta);
      
      /* add the file to our filemap */
      scr_filemap_add_file(map, checkpoint_id, scr_my_rank_world, newfile);
      /* TODO: tag file based on its filetype? */
    }

    /* tell rank 0 that we're done, indicate whether we were succesful */
    MPI_Send(&success, 1, MPI_INT, 0, 0, scr_comm_world);

    free(data); data = NULL;
  }

  /* write out the new filemap (do this before unlink) */
  scr_filemap_write(scr_map_file, map);

  /* free map object */
  scr_filemap_delete(map);

  /* check that all processes copied their file successfully */
  if (!scr_alltrue(success)) {
    /* TODO: rather than kick out, we should write a filemap and try to rebuild missing files */

    /* if we failed to copy some files in, we need to delete them */
    scr_unlink_checkpoint(checkpoint_id);
    scr_remove_checkpoint_dir(checkpoint_id);
    scr_remove_checkpoint_flush(checkpoint_id);

    if (scr_my_rank_world == 0) {
      scr_dbg(1, "scr_fetch_files: One or more processes failed to read its files @ %s:%d", __FILE__, __LINE__);
      if (scr_log_enable) {
        time_t now = scr_log_seconds();
        scr_log_event("FETCH FAILED", fetch_dir_target, &checkpoint_id, &now, NULL);
      }
    }
    return SCR_FAILURE;
  }

  /* set the checkpoint id */
  scr_checkpoint_id = checkpoint_id;

  /* apply redundancy scheme */
  double bytes_copied = 0.0;
  rc = scr_copy_files(scr_checkpoint_id, &bytes_copied);

  /* stop timer, compute bandwidth, and report performance */
  if (scr_my_rank_world == 0) {
    double time_end = MPI_Wtime();
    double time_diff = time_end - time_start;
    double bw = total_bytes / (1024.0 * 1024.0 * time_diff);
    scr_dbg(1, "scr_fetch_files: seconds %f, bytes %e, MB/sec %f, per proc MB/sec %f",
            time_diff, total_bytes, bw, bw/scr_ranks_world
    );

    /* log data on the fetch to the database */
    if (scr_log_enable) {
      time_t now = scr_log_seconds();
      scr_log_event("FETCH SUCCEEDED", fetch_dir_target, &checkpoint_id, &now, &time_diff);

      char ckpt_path[SCR_MAX_FILENAME];
      scr_checkpoint_dir(checkpoint_id, ckpt_path);
      scr_log_transfer("FETCH", fetch_dir_target, ckpt_path, &checkpoint_id, &timestamp_start, &time_diff, &total_bytes);
    }
  }

  return rc;
}

/* flushes file named in src_file to dst_dir and fills in meta based on flush, returns success of flush */
static int scr_flush_a_file(const char* src_file, const char* dst_dir, struct scr_meta* meta)
{
  int flushed = SCR_SUCCESS;
  int tmp_rc;

  /* get full path of file to copy */
  char file[SCR_MAX_FILENAME];
  strcpy(file, src_file);

  /* break file into path and name components */
  char path[SCR_MAX_FILENAME];
  char name[SCR_MAX_FILENAME];
  scr_split_path(file, path, name);

  /* fill in meta with source file info */
  if (scr_read_meta(file, meta) == SCR_SUCCESS) {
    /* if XOR file, read meta for source file */
    if (meta->filetype == SCR_FILE_XOR) {
      scr_build_path(file, path, meta->src_filename);
      if (scr_read_meta(file, meta) != SCR_SUCCESS) {
        /* TODO: print error */
      }
    }
  } else {
        /* TODO: print error */
  }

  /* get meta data file name for file */
  char metafile[SCR_MAX_FILENAME];
  scr_meta_name(metafile, file);

  /* copy file */
  uLong crc;
  char my_flushed_file[SCR_MAX_FILENAME];
  tmp_rc = scr_copy_to(file, dst_dir, my_flushed_file, &crc);
  if (tmp_rc != SCR_SUCCESS) { flushed = SCR_FAILURE; }
  scr_dbg(2, "scr_flush_a_file: Read and copied %s to %s with success code %d @ %s:%d",
          file, my_flushed_file, tmp_rc, __FILE__, __LINE__
         );

  /* if file has crc32, check it against the one computed during the copy, otherwise if scr_crc_on_flush is set, record crc32 */
  if (meta->crc32_computed) {
    if (crc != meta->crc32) { 
      meta->complete = 0;
      flushed = SCR_FAILURE;
      scr_err("scr_flush_a_file: CRC32 mismatch detected when flushing file %s to %s @ %s:%d",
              file, my_flushed_file, __FILE__, __LINE__
             );
      scr_write_meta(file, meta);
      /* TODO: would be good to log this, but right now only rank 0 can write log entries */
      /*
      if (scr_log_enable) {
        time_t now = scr_log_seconds();
        scr_log_event("CRC32 MISMATCH", my_flushed_file, NULL, &now, NULL);
      }
      */
    }
  } else if (scr_crc_on_flush) {
    meta->crc32_computed = 1;
    meta->crc32          = crc;
    scr_write_meta(file, meta);
  }

  /* copy corresponding .scr file */
  uLong crc_scr;
  char my_flushed_metafile[SCR_MAX_FILENAME];
  tmp_rc = scr_copy_to(metafile, dst_dir, my_flushed_metafile, &crc_scr);
    if (tmp_rc != SCR_SUCCESS) { flushed = SCR_FAILURE; }
    scr_dbg(2, "scr_flush_a_file: Read and copied %s to %s with success code %d @ %s:%d",
            metafile, my_flushed_metafile, tmp_rc, __FILE__, __LINE__
           );

  /* TODO: check that written filesize matches expected filesize */

  /* TODO: for xor schemes, also flush the xor segments */

  /* fill out meta data, set complete field based on flush success */
  /* (we don't update the meta file here, since perhaps the file in cache is ok and only the flush failed) */
  meta->complete     = (flushed == SCR_SUCCESS);
  meta->src_complete = (flushed == SCR_SUCCESS);

  return flushed;
}

/* flush files from cache to parallel file system under SCR_PREFIX */
static int scr_flush_files(int checkpoint_id)
{
  int flushed = SCR_SUCCESS;
  int tmp_rc;

  /* if user has disabled flush, return failure */
  if (scr_flush <= 0) { return SCR_FAILURE; }

  /* if we don't need a flush, return right away with success */
  if (!scr_bool_need_flush(checkpoint_id)) { return SCR_SUCCESS; }

  /* if scr_par_prefix is not set, return right away with an error */
  if (strcmp(scr_par_prefix, "") == 0) { return SCR_FAILURE; }

  /* this may take a while, so tell user what we're doing */
  if (scr_my_rank_world == 0) { scr_dbg(1, "scr_flush_files: Initiating flush of checkpoint %d", checkpoint_id); }

  /* make sure all processes make it this far before progressing */
  MPI_Barrier(scr_comm_world);

  /* start timer */
  time_t timestamp_start;
  double time_start;
  if (scr_my_rank_world == 0) {
    timestamp_start = scr_log_seconds();
    time_start = MPI_Wtime();
  }

  /* read in the filemap to get the checkpoint file names */
  struct scr_filemap* map = scr_filemap_new();
  int have_files = 1;
  if (scr_filemap_read(scr_map_file, map) != SCR_SUCCESS) {
    scr_err("scr_flush_files: Failed to read filemap @ %s:%d", __FILE__, __LINE__);
    have_files = 0;
  }
  if (have_files && (scr_check_files(map, checkpoint_id) != SCR_SUCCESS)) {
    scr_err("scr_flush_files: One or more files is missing @ %s:%d", __FILE__, __LINE__);
    have_files = 0;
  }
  if (!scr_alltrue(have_files)) {
    if (scr_my_rank_world == 0) {
      scr_err("scr_flush_files: One or more processes are missing their files @ %s:%d", __FILE__, __LINE__);
      if (scr_log_enable) {
        time_t now = scr_log_seconds();
        scr_log_event("FLUSH STARTED", NULL, &checkpoint_id, &now, NULL);
        scr_log_event("FLUSH FAILED", "Missing files in cache", &checkpoint_id, &now, NULL);
      }
    }
    scr_filemap_delete(map);
    return SCR_FAILURE;
  }

  /* have rank 0 create the checkpoint directory and broadcast the name */
  int dirsize = 0;
  char dir[SCR_MAX_FILENAME];
  char timestamp[SCR_MAX_FILENAME];
  if (scr_my_rank_world == 0) {
    /* build the checkpoint directory name */
    time_t now;
    now = time(NULL);
    strftime(timestamp, sizeof(timestamp), "%Y-%m-%d_%H:%M:%S", localtime(&now));
    sprintf(dir, "%s/scr.%s.%s.%d", scr_par_prefix, timestamp, scr_jobid, checkpoint_id);
    scr_dbg(1, "scr_flush_files: Flushing to %s", dir);
    if (scr_log_enable) {
      time_t now = scr_log_seconds();
      scr_log_event("FLUSH STARTED", dir, &checkpoint_id, &now, NULL);
    }

    /* create the directory, set dir to NULL string if mkdir fails */
    tmp_rc = scr_mkdir(dir, S_IRWXU);
    if (tmp_rc != SCR_SUCCESS) {
      scr_err("scr_flush_files: Failed to make checkpoint directory mkdir(%s) %m errno=%d @ %s:%d",
              dir, errno, __FILE__, __LINE__
      );
      strcpy(dir, "");
    }

    dirsize = strlen(dir) + 1;
  }
  MPI_Bcast(&dirsize, 1, MPI_INT, 0, scr_comm_world);
  MPI_Bcast(dir, dirsize, MPI_BYTE, 0, scr_comm_world);

  /* check whether directory was created ok, and bail out if not */
  if (strcmp(dir, "") == 0) {
    scr_filemap_delete(map);
    if (scr_my_rank_world == 0) {
      if (scr_log_enable) {
        time_t now = scr_log_seconds();
        scr_log_event("FLUSH FAILED", dir, &checkpoint_id, &now, NULL);
      }
    }
    return SCR_FAILURE;
  }

  /* record total number of bytes written */
  double total_bytes = 0;

  /* flow control the write among processes, and gather meta data to rank 0 */
  if (scr_my_rank_world == 0) {
    /* get count of number of files from each process */
    int my_num_files = scr_filemap_num_files(map, checkpoint_id, scr_my_rank_world);
    int* num_files = (int*) malloc(scr_ranks_world * sizeof(int));
    MPI_Gather(&my_num_files, 1, MPI_INT, num_files, 1, MPI_INT, 0, scr_comm_world);

    /* compute offsets to write data structures for each rank */
    int i;
    int* offset_files = (int*) malloc(scr_ranks_world * sizeof(int));
    offset_files[0] = 0;
    for (i=1; i < scr_ranks_world; i++) {
      offset_files[i] = offset_files[i-1] + num_files[i-1];
    }
    int total_files = offset_files[scr_ranks_world-1] + num_files[scr_ranks_world-1];

    /* allocate structure to fill in summary info */
    struct scr_meta* data = (struct scr_meta*) malloc(total_files * sizeof(struct scr_meta));
    if (data == NULL) {
      scr_err("scr_flush_files: Failed to malloc data structure to write out summary file @ file %s:%d", __FILE__, __LINE__);
      MPI_Abort(scr_comm_world, 0);
    }

    /* flush my file, fill in meta data structure, and to the byte count */
    struct scr_hash_elem* file_elem = scr_filemap_first_file(map, checkpoint_id, scr_my_rank_world);
    for (i=0; i < my_num_files; i++) {
      char* file = scr_hash_elem_key(file_elem);
      if (scr_flush_a_file(file, dir, &data[i]) != SCR_SUCCESS) { flushed = SCR_FAILURE; }
      total_bytes += (double) data[i].filesize;
      file_elem = scr_hash_elem_next(file_elem);
    }

    /* have a sliding window of w processes write simultaneously */
    int w = scr_flush_width;
    if (w > scr_ranks_world-1) { w = scr_ranks_world-1; }

    /* allocate MPI_Request arrays and an array of ints */
    int* ranks = (int*) malloc(w * sizeof(int));
    MPI_Request* req_recv = (MPI_Request*) malloc(w * sizeof(MPI_Request));
    MPI_Request* req_send = (MPI_Request*) malloc(w * sizeof(MPI_Request));
    MPI_Status status;

    i = 1;
    int outstanding = 0;
    int index = 0;
    while (i < scr_ranks_world || outstanding > 0) {
      /* issue up to w outstanding sends and receives */
      while (i < scr_ranks_world && outstanding < w) {
        /* record which rank we assign to this slot */
        ranks[index] = i;

        /* post a receive for the response message we'll get back when rank i is done */
        int num = num_files[i];
        int offset = offset_files[i];
        MPI_Irecv(&data[offset], num * sizeof(struct scr_meta), MPI_BYTE, i, 0, scr_comm_world, &req_recv[index]);

        /* post a send to tell rank i to start */
        int start = 1;
        MPI_Isend(&start, 1, MPI_INT, i, 0, scr_comm_world, &req_send[index]);

        /* update the number of outstanding requests */
        i++;
        outstanding++;
        index++;
      }

      /* wait to hear back from any rank */
      MPI_Waitany(w, req_recv, &index, &status);

      /* once we hear back from a rank, the send to that rank should also be done, so complete it */
      MPI_Wait(&req_send[index], &status);

      /* check whether this rank wrote its files successfully */
      int rank = ranks[index];
      int j;
      for (j=0; j < num_files[rank]; j++) {
        /* compute the offset for each file from this rank */
        int offset = offset_files[rank] + j;

        /* check that this file was written successfully */
        if (!data[offset].complete) { flushed = SCR_FAILURE; }

        /* add the number of bytes written to the total */
        total_bytes += (double) data[offset].filesize;

        scr_dbg(2, "scr_flush_files: Rank %d wrote %s with completeness code %d @ %s:%d",
                rank, data[offset].filename, data[offset].complete, __FILE__, __LINE__
        );
      }

      /* one less request outstanding now */
      outstanding--;
    }

    /* free the MPI_Request arrays */
    free(ranks);    ranks    = NULL;
    free(req_recv); req_recv = NULL;
    free(req_send); req_send = NULL;

    /* write out summary file */
    int wrote_summary = scr_write_summary(dir, total_files, data);
    if (wrote_summary != SCR_SUCCESS) { flushed = SCR_FAILURE; }

    /* free the summary data buffer */
    free(num_files);    num_files    = NULL;
    free(offset_files); offset_files = NULL;
    free(data);         data         = NULL;
  } else {
    /* send the number of files i have to rank 0 */
    int my_num_files = scr_filemap_num_files(map, checkpoint_id, scr_my_rank_world);
    MPI_Gather(&my_num_files, 1, MPI_INT, NULL, 1, MPI_INT, 0, scr_comm_world);

    /* receive signal to start */
    int start = 0;
    MPI_Status status;
    MPI_Recv(&start, 1, MPI_INT, 0, 0, scr_comm_world, &status);

    /* flush each of my files and fill in meta data structures */
    struct scr_meta* data = (struct scr_meta*) malloc(my_num_files * sizeof(struct scr_meta));
    struct scr_hash_elem* file_elem = scr_filemap_first_file(map, checkpoint_id, scr_my_rank_world);
    int i = 0;
    for (i=0; i < my_num_files; i++) {
      char* file = scr_hash_elem_key(file_elem);
      if (scr_flush_a_file(file, dir, &data[i]) != SCR_SUCCESS) { flushed = SCR_FAILURE; }
      file_elem = scr_hash_elem_next(file_elem);
    }

    /* send meta data to rank 0 */
    MPI_Send(data, my_num_files * sizeof(struct scr_meta), MPI_BYTE, 0, 0, scr_comm_world);

    /* free the data */
    free(data); data = NULL;
  }

  /* TODO: check that symlinks are created ok before we declare flush as success */

  /* determine whether everyone wrote their files ok */
  int write_succeeded = scr_alltrue((flushed == SCR_SUCCESS));

  /* if flush succeeded, update the scr.current/scr.old symlinks */
  if (write_succeeded && scr_my_rank_world == 0) {
    /* TODO: update 'flushed' depending on symlink update */

    /* file write succeeded, now update symlinks */
    char old[SCR_MAX_FILENAME];
    char current[SCR_MAX_FILENAME];
    scr_build_path(old,     scr_par_prefix, "scr.old");
    scr_build_path(current, scr_par_prefix, "scr.current");

    /* if old exists, unlink it */
    if (access(old, F_OK) == 0) { unlink(old); }

    /* if current exists, read it in, unlink it, and create old */
    if (access(current, F_OK) == 0) {
      char target[SCR_MAX_FILENAME];
      int len = readlink(current, target, sizeof(target));
      target[len] = '\0';
      unlink(current);

      /* make old point to current target */
      symlink(target, old);
    }

    /* create new current to point to new directory */
    char target_path[SCR_MAX_FILENAME];
    char target_name[SCR_MAX_FILENAME];
    scr_split_path(dir, target_path, target_name);
    symlink(target_name, current);
  }

  /* have rank 0 broadcast whether the entire flush and symlink update succeeded */
  MPI_Bcast(&flushed, 1, MPI_INT, 0, scr_comm_world);

  /* mark set as flushed to the parallel file system */
  if (flushed == SCR_SUCCESS) {
    scr_set_flush(checkpoint_id, SCR_FLUSH_PFS);
  }

  /* stop timer, compute bandwidth, and report performance */
  if (scr_my_rank_world == 0) {
    double time_end = MPI_Wtime();
    double time_diff = time_end - time_start;
    double bw = total_bytes / (1024.0 * 1024.0 * time_diff);
    scr_dbg(1, "scr_flush_files: seconds %f, bytes %e, MB/sec %f, per proc MB/sec %f",
            time_diff, total_bytes, bw, bw/scr_ranks_world
    );

    /* log messages about flush */
    if (flushed == SCR_SUCCESS) {
      /* the flush worked, print a debug message */
      scr_dbg(1, "scr_flush_files: Flush succeeded");

      /* log details of flush */
      if (scr_log_enable) {
        time_t now = scr_log_seconds();
        scr_log_event("FLUSH SUCCEEDED", dir, &checkpoint_id, &now, &time_diff);

        char ckpt_path[SCR_MAX_FILENAME];
        scr_checkpoint_dir(checkpoint_id, ckpt_path);
        scr_log_transfer("FLUSH", ckpt_path, dir, &checkpoint_id, &timestamp_start, &time_diff, &total_bytes);
      }
    } else {
      /* the flush failed, this is more serious so print an error message */
      scr_err("scr_flush_files: Flush failed");

      /* log details of flush */
      if (scr_log_enable) {
        time_t now = scr_log_seconds();
        scr_log_event("FLUSH FAILED", dir, &checkpoint_id, &now, &time_diff);
      }
    }
  }

  /* free map object */
  scr_filemap_delete(map);

  return flushed;
}

/* check whether a flush is needed, and execute flush if so */
static int scr_check_flush()
{
  /* check whether user has enabled SCR auto-flush feature */
  if (scr_flush > 0) {
    /* every scr_flush checkpoints, flush the checkpoint set */
    if (scr_checkpoint_id > 0 && scr_checkpoint_id % scr_flush == 0) {
      scr_flush_files(scr_checkpoint_id);
    }
  }
  return SCR_SUCCESS;
}

/*
=========================================
Halt logic
=========================================
*/

/* write out a nodes file which is used by restart logic */
static int scr_write_nodes(int nodes)
{
    /* open nodes file for writing */
  FILE* fs = fopen(scr_nodes_file, "w");
  if (fs == NULL) {
    scr_err("Opening min nodes file for write: fopen(%s, \"w\") errno=%d %m @ %s:%d",
            scr_nodes_file, errno, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* write number of nodes */
  fprintf(fs, "%d\n", nodes);

  /* close file and return */
  fclose(fs);

  return SCR_SUCCESS;
}

/* sets flag to indicate to signal handler whether it's safe to halt immediately or not */
int scr_signal_critical_section(int inside)
{
/*
    this barrier ensures all tasks have progressed through critical section before we set the flag
    MPI_Barrier(scr_comm_world);

    set the lock
    scr_inside_checkpoint = inside;
  
    this barrier ensures correctness in case the signal comes while setting inside_checkpoint
    MPI_Barrier(scr_comm_world);
*/
  return SCR_SUCCESS;
}

/* writes a halt file to indicate that the SCR should exit job at first opportunity */
static int scr_halt(const char* reason)
{
  /* copy in reason if one was given */
  if (reason != NULL) {
    strcpy(halt.exit_reason, reason);
  }

  /* log the halt condition */
  int* ckpt = NULL;
  if (scr_checkpoint_id > 0) {
    ckpt = &scr_checkpoint_id;
  }
  scr_log_halt(reason, ckpt);

  /* and write out the halt file */
  return scr_halt_sync_and_decrement(scr_halt_file, &halt, 0);
}

/* returns the number of seconds remaining in the time allocation */
static int scr_seconds_remaining()
{
  /* returning a negative number tells the caller this functionality is disabled */
  int secs = -1;

  /* call libyogrt if we have it */
  #ifdef HAVE_LIBYOGRT
  secs = yogrt_remaining();
  if (secs < 0) { secs = 0; }
  #endif /* HAVE_LIBYOGRT */

  return secs;
}

/* check whether we should halt the job */
static int scr_bool_check_halt(int halt_now, int decrement)
{
  /* TODO: maybe it'd be better to have all nodes check for a halt file?
   * if we do, we should probably have every node write a halt file, as well
   * ==> need an allreduce and barrier */

  /* only need one rank to check on halt, use rank 0 */
  int need_to_halt = 0;
  if (scr_my_rank_world == 0) {
    /* TODO: all epochs are stored in ints, should be in unsigned ints? */
    /* get current epoch seconds */
    struct timeval tv;
    gettimeofday(&tv, NULL);
    int now = tv.tv_sec;

    /* locks halt file, reads it to pick up new values, decrements the checkpoint counter, writes it out, and unlocks it */
    scr_halt_sync_and_decrement(scr_halt_file, &halt, decrement);
/*
printf("ER: %s\n", halt.exit_reason);
printf("CL: %d\n", halt.checkpoints_left);
printf("EA: %d\n", halt.exit_after);
printf("EB: %d\n", halt.exit_before);
printf("HS: %d\n", halt.halt_seconds);
*/

    /* set halt seconds */
    int halt_seconds = 0;
    if (halt.halt_seconds != -1) { halt_seconds = halt.halt_seconds; }

    if (halt_seconds > 0) {
      /* if halt secs enabled, check the remaining time */
      int remaining = scr_seconds_remaining();
      if (remaining >= 0 && remaining <= halt_seconds) {
        if (halt_now) {
          scr_dbg(0, "Job exiting: Reached time limit: (seconds remaining = %d) <= (SCR_HALT_SECONDS = %d).",
                  remaining, halt_seconds
          );
          scr_halt("TIME_LIMIT");
        }
        need_to_halt = 1;
      }
    }
    if (strcmp(halt.exit_reason, "") != 0) {
      /* check whether a reason has been specified */
      if (halt_now) {
        scr_dbg(0, "Job exiting: Reason: %s.", halt.exit_reason);
        scr_halt(halt.exit_reason);
      }
      need_to_halt = 1;
    }
    if (halt.checkpoints_left != -1 && halt.checkpoints_left == 0) {
      /* check whether we are out of checkpoints */
      if (halt_now) {
        scr_dbg(0, "Job exiting: No more checkpoints remaining.");
        scr_halt("NO_CHECKPOINTS_LEFT");
      }
      need_to_halt = 1;
    }
    if (halt.exit_before != -1 && now >= (halt.exit_before - halt_seconds)) {
      /* check whether we need to exit before a specified time */
      if (halt_now) {
        time_t time_now  = (time_t) now;
        time_t time_exit = (time_t) halt.exit_before - halt_seconds;
        char str_now[256];
        char str_exit[256];
        strftime(str_now,  sizeof(str_now),  "%c", localtime(&time_now));
        strftime(str_exit, sizeof(str_exit), "%c", localtime(&time_exit));
        scr_dbg(0, "Job exiting: Current time (%s) is past ExitBefore-HaltSeconds time (%s).", str_now, str_exit);
        scr_halt("EXIT_BEFORE_TIME");
      }
      need_to_halt = 1;
    }
    if (halt.exit_after != -1 && now >= halt.exit_after) {
      /* check whether we need to exit after a specified time */
      if (halt_now) {
        time_t time_now  = (time_t) now;
        time_t time_exit = (time_t) halt.exit_after;
        char str_now[256];
        char str_exit[256];
        strftime(str_now,  sizeof(str_now),  "%c", localtime(&time_now));
        strftime(str_exit, sizeof(str_exit), "%c", localtime(&time_exit));
        scr_dbg(0, "Job exiting: Current time (%s) is past ExitAfter time (%s).", str_now, str_exit);
        scr_halt("EXIT_AFTER_TIME");
      }
      need_to_halt = 1;
    }
  }

  MPI_Bcast(&need_to_halt, 1, MPI_INT, 0, scr_comm_world);
  if (need_to_halt && halt_now) {
    /* flush files if needed */
    scr_flush_files(scr_checkpoint_id);

    /* sync up tasks before exiting (don't want tasks to exit so early that runtime kills others after timeout) */
    MPI_Barrier(scr_comm_world);

    /* and exit the job */
    exit(0);
  }

  return need_to_halt;
}

/*
=========================================
Distribute and file rebuild functions
=========================================
*/

/* returns true if a an XOR file is found for this rank for the given checkpoint id, sets xor_file to full filename */
static int scr_bool_have_xor_file(int checkpoint_id, char* xor_file)
{
  int rc = 0;

  /* find the name of my xor chunk file: read filemap and check filetype of each file */
  struct scr_filemap* map = scr_filemap_new();
  scr_filemap_read(scr_map_file, map);
  struct scr_hash_elem* file_elem;
  for (file_elem = scr_filemap_first_file(map, checkpoint_id, scr_my_rank_world);
       file_elem != NULL;
       file_elem = scr_hash_elem_next(file_elem))
  {
    /* get the filename */
    char* file = scr_hash_elem_key(file_elem);

    /* read the meta for this file */
    struct scr_meta meta;
    scr_read_meta(file, &meta);

    /* if the filetype of this file is an XOR fule, copy the filename and bail out */
    if (meta.filetype == SCR_FILE_XOR) {
      strcpy(xor_file, file);
      rc = 1;
      break;
    }
  }
  scr_filemap_delete(map);

  return rc;
}

/* given a filename to my XOR file, a failed rank in my xor set,
 * rebuild file and return new filename and current checkpoint id to caller */
static int scr_rebuild_xor(int root, int checkpoint_id)
{
  int rc = SCR_SUCCESS;
  int tmp_rc;

  int fd_chunk;
  struct scr_copy_xor_header h;
  int i;
  int* fds = NULL;
  unsigned long* filesizes = NULL;

  char full_chunk_filename[SCR_MAX_FILENAME];
  char path[SCR_MAX_FILENAME] = "";
  char name[SCR_MAX_FILENAME] = "";
  unsigned long my_bytes;
  MPI_Status  status[2];
  if (root != scr_my_rank_xor) {
    /* find the name of my xor chunk file: read filemap and check filetype of each file */
    if (!scr_bool_have_xor_file(checkpoint_id, full_chunk_filename)) {
      /* TODO: need to throw an error if we didn't find the file */
    }

    /* read the meta file for our XOR chunk */
    struct scr_meta meta_chunk;
    scr_read_meta(full_chunk_filename, &meta_chunk);

    /* open our xor file for reading */
    fd_chunk = scr_open(full_chunk_filename, O_RDONLY);
    if (fd_chunk < 0) {
      /* TODO: try again? */
      scr_abort(-1, "Opening XOR file for reading in XOR rebuild: scr_open(%s, O_RDONLY) errno=%d %m @ %s:%d",
              full_chunk_filename, errno, __FILE__, __LINE__
      );
    }

    /* read in the xor chunk header */
    scr_copy_xor_header_read(fd_chunk, &h);

    /* read in the number of our files */
    if (h.my_nfiles > 0) {
      fds       = (int*) malloc(h.my_nfiles * sizeof(int));
      filesizes = (unsigned long*) malloc(h.my_nfiles * sizeof(unsigned long));
    }

    /* get path from chunk file */
    scr_split_path(full_chunk_filename, path, name);

    /* open each of our files */
    unsigned long my_bytes = 0;
    for (i=0; i < h.my_nfiles; i++) {
      /* create full path to the file */
      char full_file[SCR_MAX_FILENAME];
      scr_build_path(full_file, path, h.my_files[i].filename);

      /* read in the filesize */
      filesizes[i] = h.my_files[i].filesize;
      my_bytes = filesizes[i];

      /* read meta for our file */
      scr_read_meta(full_file, &(h.my_files[i]));

      /* open our file for reading */
      fds[i] = scr_open(full_file, O_RDONLY);
      if (fds[i] < 0) {
        /* TODO: try again? */
        scr_abort(-1, "Opening checkpoint file for reading in XOR rebuild: scr_open(%s, O_RDONLY) errno=%d %m @ %s:%d",
                  full_file, errno, __FILE__, __LINE__
        );
      }
    }

    /* if failed rank is to my left, i have the meta for his files, send it to him */
    if (root == scr_lhs_rank) {
      MPI_Send(&h.partner_nfiles, 1, MPI_INT, scr_lhs_rank, 0, scr_comm_xor);
      MPI_Send(h.partner_files, h.partner_nfiles * sizeof(struct scr_meta), MPI_BYTE, scr_lhs_rank, 0, scr_comm_xor);
      MPI_Send(&h.checkpoint_id, 1, MPI_INT, scr_lhs_rank, 0, scr_comm_xor);
      MPI_Send(&h.chunk_size, sizeof(unsigned long), MPI_BYTE, scr_lhs_rank, 0, scr_comm_xor);
    }

    /* if failed rank is to my right, send him my file count and meta data so he can write his XOR header */
    if (root == scr_rhs_rank) {
      MPI_Send(&h.my_nfiles, 1, MPI_INT, scr_rhs_rank, 0, scr_comm_xor);
      MPI_Send(h.my_files, h.my_nfiles * sizeof(struct scr_meta), MPI_BYTE, scr_rhs_rank, 0, scr_comm_xor);
    }
  } else {
    /* receive the number of files and meta data for my files, as well as, the checkpoint id and the chunk size from right-side partner */
    MPI_Recv(&h.my_nfiles, 1, MPI_INT, scr_rhs_rank, 0, scr_comm_xor, &status[0]);
    scr_copy_xor_header_set_ranks(&h, scr_comm_level, scr_comm_world);
    scr_copy_xor_header_alloc_my_files(&h, scr_my_rank_world, h.my_nfiles);
    if (h.my_nfiles > 0) {
      fds       = (int*) malloc(h.my_nfiles * sizeof(int));
      filesizes = (unsigned long*) malloc(h.my_nfiles * sizeof(unsigned long));
    }
    MPI_Recv(h.my_files, h.my_nfiles * sizeof(struct scr_meta), MPI_BYTE, scr_rhs_rank, 0, scr_comm_xor, &status[0]);
    MPI_Recv(&h.checkpoint_id, 1, MPI_INT, scr_rhs_rank, 0, scr_comm_xor, &status[0]);
    MPI_Recv(&h.chunk_size, sizeof(unsigned long), MPI_BYTE, scr_rhs_rank, 0, scr_comm_xor, &status[0]);

    /* set chunk filename of form:  <xor_rank+1>_of_<xorset_size>_in_<level_partion>x<xorset_size>.xor */
    char ckpt_path[SCR_MAX_FILENAME];
    scr_checkpoint_dir(checkpoint_id, ckpt_path);
    sprintf(full_chunk_filename, "%s/%d_of_%d_in_%dx%d.xor",
            ckpt_path, scr_my_rank_xor+1, scr_ranks_xor, scr_my_rank_local, scr_xor_set_id);

    /* open my chunk file for writing */
    fd_chunk = scr_open(full_chunk_filename, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
    if (fd_chunk < 0) {
      /* TODO: try again? */
      scr_abort(-1, "Opening XOR chunk file for writing in XOR rebuild: scr_open(%s) errno=%d %m @ %s:%d",
                full_chunk_filename, errno, __FILE__, __LINE__
      );
    }

    /* split file into path and name */
    scr_split_path(full_chunk_filename, path, name);

    /* open each of my files for writing */
    for (i=0; i < h.my_nfiles; i++) {
      /* get the filename */
      char full_file[SCR_MAX_FILENAME];
      scr_build_path(full_file, path, h.my_files[i].filename);

      filesizes[i] = h.my_files[i].filesize;
      my_bytes += filesizes[i];

      /* open my file for writing */
      fds[i] = scr_open(full_file, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
      if (fds[i] < 0) {
        /* TODO: try again? */
        scr_abort(-1, "Opening checkpoint file for writing in XOR rebuild: scr_open(%s) errno=%d %m @ %s:%d",
                  full_file, errno, __FILE__, __LINE__
        );
      }
    }

    /* receive number of files our left-side partner has and allocate an array of meta structures to store info */
    MPI_Recv(&h.partner_nfiles, 1, MPI_INT, scr_lhs_rank, 0, scr_comm_xor, &status[0]);
    scr_copy_xor_header_alloc_partner_files(&h, scr_lhs_rank_world, h.partner_nfiles);

    /* receive meta for our partner's files */
    MPI_Recv(h.partner_files, h.partner_nfiles * sizeof(struct scr_meta), MPI_BYTE, scr_lhs_rank, 0, scr_comm_xor, &status[0]);

    /* write XOR chunk file header */
    scr_copy_xor_header_write(fd_chunk, &h);
  }

  unsigned long chunk_size = h.chunk_size;
  int num_files = h.my_nfiles;

  /* allocate buffer to read a piece of my file */
  char* send_buf = (char*) malloc(scr_mpi_buf_size);
  if (send_buf == NULL) {
    scr_abort(-1, "Allocating memory for send buffer: malloc(%d) errno=%d %m @ %s:%d",
            scr_mpi_buf_size, errno, __FILE__, __LINE__
    );
  }

  /* allocate buffer to read a piece of the recevied chunk file */
  char* recv_buf = (char*) malloc(scr_mpi_buf_size);
  if (recv_buf == NULL) {
    scr_abort(-1, "Allocating memory for recv buffer: malloc(%d) errno=%d %m @ %s:%d",
            scr_mpi_buf_size, errno, __FILE__, __LINE__
    );
  }

  /* Pipelined XOR Reduce to root */
  unsigned long offset = 0;
  int chunk_id;
  for (chunk_id = 0; chunk_id < scr_ranks_xor; chunk_id++) {
    size_t nread = 0;
    while (nread < chunk_size) {
      size_t count = chunk_size - nread;
      if (count > scr_mpi_buf_size) { count = scr_mpi_buf_size; }

      if (root != scr_my_rank_xor) {
        /* read the next set of bytes for this chunk from my file into send_buf */
        if (chunk_id != scr_my_rank_xor) {
          scr_read_pad_n(num_files, fds, send_buf, count, offset, filesizes);
          offset += count;
        } else {
          scr_read(fd_chunk, send_buf, count);
        }

        /* if not start of pipeline, receive data from left and xor with my own */
        if (root != scr_lhs_rank) {
          int i;
          MPI_Recv(recv_buf, count, MPI_BYTE, scr_lhs_rank, 0, scr_comm_xor, &status[0]);
          for (i = 0; i < count; i++) { send_buf[i] ^= recv_buf[i]; }
        }

        /* send data to right-side partner */
        MPI_Send(send_buf, count, MPI_BYTE, scr_rhs_rank, 0, scr_comm_xor);
      } else {
        /* root of rebuild, just receive incoming chunks and write them out */
        MPI_Recv(recv_buf, count, MPI_BYTE, scr_lhs_rank, 0, scr_comm_xor, &status[0]);

        /* if this is not my xor chunk, write data to normal file, otherwise write to my xor chunk */
        if (chunk_id != scr_my_rank_xor) {
          scr_write_pad_n(num_files, fds, recv_buf, count, offset, filesizes);
          offset += count;
        } else {
          scr_write(fd_chunk, recv_buf, count);
        }
      }

      nread += count;
    }
  }

  /* close my chunkfile */
  scr_close(fd_chunk);

  /* close my checkpoint files */
  for (i=0; i < num_files; i++) { scr_close(fds[i]); }

  /* if i'm the rebuild rank, truncate my file, and complete my file and xor chunk */
  if (root == scr_my_rank_xor) {
    /* complete each of our files and add each to our filemap */
    struct scr_filemap* map = scr_filemap_new();
    tmp_rc = scr_filemap_read(scr_map_file, map);
    for (i=0; i < num_files; i++) {
      char full_file[SCR_MAX_FILENAME];
      scr_build_path(full_file, path, h.my_files[i].filename);

      /* TODO: need to check for errors, check that file is really valid */

      /* fill out meta info for our file and complete it */
      struct scr_meta meta;
      scr_set_meta(&meta, full_file, scr_my_rank_world, scr_ranks_world, h.checkpoint_id, SCR_FILE_FULL, 1);
      scr_complete(full_file, &meta);

      /* add this file to the filemap */
      scr_filemap_add_file(map, h.checkpoint_id, scr_my_rank_world, full_file);
      /* TODO: tag file based on its filetype? */
    }

    /* replace main meta info with chunk info and mark chunk as complete */
    struct scr_meta meta_chunk;
    scr_set_meta(&meta_chunk, full_chunk_filename, scr_my_rank_world, scr_ranks_world, h.checkpoint_id, SCR_FILE_XOR, 1);
    scr_complete(full_chunk_filename, &meta_chunk);

    /* add our chunk file to our filemap, set the expected number of files, and write out the filemap */
    scr_filemap_add_file(map, h.checkpoint_id, scr_my_rank_world, full_chunk_filename);
    /* TODO: tag file based on its filetype? */
    scr_filemap_set_expected_files(map, h.checkpoint_id, scr_my_rank_world, scr_filemap_num_files(map, h.checkpoint_id, scr_my_rank_world));
    scr_filemap_write(scr_map_file, map);
    scr_filemap_delete(map);

    if (tmp_rc != SCR_SUCCESS) { rc = tmp_rc; }
  }

  /* free the buffers */
  scr_copy_xor_header_free(&h);
  if (filesizes != NULL) { free(filesizes); filesizes = NULL; }
  if (fds       != NULL) { free(fds);       fds       = NULL; }
  free(send_buf);
  free(recv_buf);

  return rc;
}

/* given a checkpoint id, check whether files can be rebuilt via xor and execute the rebuild if needed */
static int scr_attempt_rebuild_xor(int checkpoint_id)
{
  int rc = SCR_SUCCESS;

  /* check whether we have our files */
  int have_my_files = scr_bool_have_files(checkpoint_id, scr_my_rank_world);

  /* check whether we have our XOR file */
  char xor_file[SCR_MAX_FILENAME];
  if (!scr_bool_have_xor_file(checkpoint_id, xor_file)) {
    have_my_files = 0;
  }

  /* TODO: check whether each of the files listed in our xor file exists? */

  /* check whether I have my full checkpoint file, assume I don't */
  int need_rebuild = 1;
  if (have_my_files) { need_rebuild = 0; }

  /* count how many in my xor set need to rebuild */
  int total_rebuild;
  MPI_Allreduce(&need_rebuild, &total_rebuild, 1, MPI_INT, MPI_SUM, scr_comm_xor); 

  /* check whether all sets can rebuild, if not, bail out */
  int set_can_rebuild = (total_rebuild <= 1);
  if (!scr_alltrue(set_can_rebuild)) {
    if (scr_my_rank_world == 0) {
      scr_err("Cannot rebuild missing files @ %s:%d", __FILE__, __LINE__);
    }
    return SCR_FAILURE;
  }

  /* it's possible to rebuild; rebuild if we need to */
  if (total_rebuild > 0) {
    /* someone in my set needs to rebuild, determine who */
    int tmp_rank = need_rebuild ? scr_my_rank_xor : -1;
    int rebuild_rank;
    MPI_Allreduce(&tmp_rank, &rebuild_rank, 1, MPI_INT, MPI_MAX, scr_comm_xor);

    /* rebuild */
    if (need_rebuild) { scr_dbg(1, "Rebuilding file from XOR segments"); }
    rc = scr_rebuild_xor(rebuild_rank, checkpoint_id);
  }

  return rc;
}

/* given a filemap, a checkpoint_id, and a rank, unlink those files and remove them from the map */
static int scr_unlink_rank_map(struct scr_filemap* map, int ckpt, int rank)
{
  struct scr_hash_elem* file_elem;
  for (file_elem = scr_filemap_first_file(map, ckpt, rank);
       file_elem != NULL;
       file_elem = scr_hash_elem_next(file_elem))
  {
    char* file = scr_hash_elem_key(file_elem);
    if (file != NULL) {
      scr_dbg(2, "Delete file Checkpoint %d, Rank %d, File %s", ckpt, rank, file);

      /* delete the file */
      unlink(file);

      /* delete the meta file */
      scr_incomplete(file);
    }
  }
  /* remove this rank from our filemap */
  scr_filemap_remove_rank_by_checkpoint(map, ckpt, rank);

  return SCR_SUCCESS;
}

int scr_filemap_send(struct scr_filemap* map, int rank, MPI_Comm comm)
{
  /* first get the size of the filemap hash */
  size_t size = scr_hash_get_pack_size(map->hash);
  
  /* tell rank how big the pack size is */
  MPI_Send(&size, sizeof(size), MPI_BYTE, rank, 0, comm);

  /* pack the hash and send it */
  if (size > 0) {
    /* allocate a buffer big enough to pack the filemap into */
    char* buf = (char*) malloc(size);
    if (buf != NULL) {
      /* pack the map and send it */
      scr_hash_pack(map->hash, buf);
      MPI_Send(buf, size, MPI_BYTE, rank, 0, comm);
      free(buf); buf = NULL;
    } else {
      scr_err("scr_filemap_send: Failed to malloc buffer to pack filemap @ %s:%d",
              __FILE__, __LINE__
      );
      exit(1);
    }
  }

  return SCR_SUCCESS;
}

struct scr_filemap* scr_filemap_recv(int rank, MPI_Comm comm)
{
  /* get a new filemap */
  struct scr_filemap* map = scr_filemap_new();

  /* get the size of the incoming filemap hash */
  MPI_Status status;
  size_t size = 0;
  MPI_Recv(&size, sizeof(size), MPI_BYTE, rank, 0, comm, &status);
  
  /* receive the hash and unpack it */
  if (size > 0) {
    /* allocate a buffer big enough to receive the packed filemap */
    char* buf = (char*) malloc(size);
    if (buf != NULL) {
      /* receive the hash and unpack it (need to delete existing hash from new map to avoid leak) */
      MPI_Recv(buf, size, MPI_BYTE, rank, 0, comm, &status);
      scr_hash_delete(map->hash);
      map->hash = scr_hash_new();
      scr_hash_unpack(buf, map->hash);
      free(buf); buf = NULL;
    } else {
      scr_err("scr_filemap_send: Failed to malloc buffer to received packged filemap @ %s:%d",
              __FILE__, __LINE__
      );
      exit(1);
    }
  }

  return map;
}

/* since on a restart we may end up with more or fewer ranks on a node than the previous run,
 * rely on the master to read in and distribute the filemap to other ranks on the node */
static int scr_distribute_filemaps()
{
  /* everyone, create an empty filemap */
  struct scr_filemap* my_map = scr_filemap_new();

  /* if i'm the master on this node, read in all filemaps */
  if (scr_my_rank_local == 0) {
    /* read in the master map */
    struct scr_hash* hash = scr_hash_new();
    scr_hash_read(scr_master_map_file, hash);

    /* create an empty filemap */
    struct scr_filemap* map = scr_filemap_new();

    /* for each filemap listed in the master map */
    struct scr_hash_elem* elem;
    for (elem = scr_hash_elem_first(scr_hash_get(hash, "Filemap"));
         elem != NULL;
         elem = scr_hash_elem_next(elem))
    {
      /* get the filename of this filemap */
      char* file = scr_hash_elem_key(elem);

      /* read in the filemap */
      struct scr_filemap* tmp_map = scr_filemap_new();
      scr_filemap_read(file, tmp_map);

      /* merge it with local 0 filemap */
      scr_filemap_merge(map, tmp_map);

      /* delete filemap */
      scr_filemap_delete(tmp_map);
      unlink(file);
    }

    /* TODO: write out new local 0 filemap? */
    if (scr_filemap_num_ranks(map) > 0) {
      scr_filemap_write(scr_map_file, map);
    }

    /* delete the hash object */
    scr_hash_delete(hash);

    /* translate local rank ids to global rank ids */
    int rank;
    int* ranks = (int*) malloc(scr_ranks_local * sizeof(int));
    MPI_Group group_local, group_world;
    MPI_Comm_group(scr_comm_local, &group_local);
    MPI_Comm_group(scr_comm_world, &group_world);
    int i;
    for (i=0; i < scr_ranks_local; i++) {
      MPI_Group_translate_ranks(group_local, 1, &i, group_world, &rank);
      ranks[i] = rank;
    }

    /* for each rank on this node, send them their own file data if we have it */
    int have_files = 1;
    MPI_Bcast(&have_files, 1, MPI_INT, 0, scr_comm_local);
    for (i=0; i < scr_ranks_local; i++) {
      rank = ranks[i];
      int got_map = 0;
      if (scr_filemap_have_rank(map, rank)) {
        got_map = 1;
        struct scr_filemap* tmp_map = scr_filemap_extract_rank(map, rank);
        if (rank == scr_my_rank_world) {
          /* merge this filemap into our own */
          scr_filemap_merge(my_map, tmp_map);
        } else {
          /* tell the rank we have a filemap for him and send it */
          MPI_Send(&got_map, 1, MPI_INT, i, 0, scr_comm_local);
          scr_filemap_send(tmp_map, i, scr_comm_local);
        }
        scr_filemap_delete(tmp_map);
      } else {
        /* tell this rank we don't have anything for him this time */
        if (rank != scr_my_rank_world) {
          MPI_Send(&got_map, 1, MPI_INT, i, 0, scr_comm_local);
        }
      }
    }

    /* now just round robin the remainder across the set (load balancing) */
    int num;
    int* remaining_ranks = NULL;
    scr_filemap_list_ranks(map, &num, &remaining_ranks);
    if (num > 0) {
      int j = 0;
      while (j < num) {
        have_files = 1;
        MPI_Bcast(&have_files, 1, MPI_INT, 0, scr_comm_local);
        for (i=0; i < scr_ranks_local; i++) {
          rank = ranks[i];
          int got_map = 0;
          if (j < num) {
            got_map = 1;
            struct scr_filemap* tmp_map = scr_filemap_extract_rank(map, remaining_ranks[j]);
            if (rank == scr_my_rank_world) {
              /* merge this filemap into our own */
              scr_filemap_merge(my_map, tmp_map);
            } else {
              /* tell the rank we have a filemap for him and send it */
              MPI_Send(&got_map, 1, MPI_INT, i, 0, scr_comm_local);
              scr_filemap_send(tmp_map, i, scr_comm_local);
            }
            scr_filemap_delete(tmp_map);
            j++;
          } else {
            /* tell this rank we don't have anything for him this time */
            if (rank != scr_my_rank_world) {
              MPI_Send(&got_map, 1, MPI_INT, i, 0, scr_comm_local);
            }
          }
        }
      }
    }
    if (remaining_ranks != NULL) { free(remaining_ranks); remaining_ranks = NULL; }

    /* now tell local tasks we're done distributing filemaps */
    have_files = 0;
    MPI_Bcast(&have_files, 1, MPI_INT, 0, scr_comm_local);

    /* delete the filemap */
    scr_filemap_delete(map);

    /* write out the new master filemap */
    hash = scr_hash_new();
    char file[SCR_MAX_FILENAME];
    for (i=0; i < scr_ranks_local; i++) {
      sprintf(file, "%s/filemap_%d.scrinfo", scr_cntl_prefix, i);
      scr_hash_set_kv(hash, "Filemap", file);
    }
    scr_hash_write(scr_master_map_file, hash);
    scr_hash_delete(hash);

    if (ranks != NULL) { free(ranks); ranks = NULL; }
  } else {
    /* receive filemaps from master */
    int have_files = 0;
    MPI_Bcast(&have_files, 1, MPI_INT, 0, scr_comm_local);
    while (have_files) {
      /* receive message to know whether we'll get a filemap */
      int recv_map = 0;
      MPI_Status status;
      MPI_Recv(&recv_map, 1, MPI_INT, 0, 0, scr_comm_local, &status);

      if (recv_map) {
        struct scr_filemap* tmp_map = scr_filemap_recv(0, scr_comm_local);
        scr_filemap_merge(my_map, tmp_map);
        scr_filemap_delete(tmp_map);
      }

      MPI_Bcast(&have_files, 1, MPI_INT, 0, scr_comm_local);
    }
  }

  /* write out our local filemap */
  if (scr_filemap_num_ranks(my_map) > 0) {
    scr_filemap_write(scr_map_file, my_map);
  }
  scr_filemap_delete(my_map);

  return SCR_SUCCESS;
}

/* TODO: for now it just handles the most recent checkpoint */
/* this moves all files in the cache to make them accessible to new rank mapping */
static int scr_distribute_files(int checkpoint_id)
{
  int i;
  int rc = SCR_SUCCESS;

  /* clean out any incomplete files before we start */
  scr_clean_files();

  /* TODO: since we may have our own files, but we may need to move them,
   * let's always fall through to the distribute portion */
  /* check whether anyone is missing any files */
/*
  int have_my_files = scr_bool_have_files(checkpoint_id, scr_my_rank_world);
  if (scr_alltrue(have_my_files)) {
  TODO: may want to delete partner files to make room in case we get a new partner?
    return SCR_SUCCESS;
  }
  if (!have_my_files) {
    scr_dbg(2, "Missing my files");
  }
*/

  /* read in the filemap */
  struct scr_filemap* map = scr_filemap_new();
  scr_filemap_read(scr_map_file, map);

  /* allocate enough space for each rank we have for this checkpoint */
  int* send_ranks = NULL;
  int send_nranks = scr_filemap_num_ranks_by_checkpoint(map, checkpoint_id);
  if (send_nranks > 0) {
    send_ranks = (int*) malloc(sizeof(int) * send_nranks);
  }

  /* TODO: would like to remove these allgather and alltoall calls for better scalability,
   * however, they work for now and should handle tens of thousands of processes without too much trouble */

  /* someone is missing files, mark which files we have and in which round we can send them */
  int* found_files = (int*) malloc(sizeof(int) * scr_ranks_world);
  int round = 1;
  for(i = 0; i < scr_ranks_world; i++) {
    int rel_rank = (scr_my_rank_world + i) % scr_ranks_world;
    /* we could just call scr_bool_have_files here, but since it involves a file open/read/close with each call,
     * let's avoid calling it too often here by checking with the filemap first */
    if (scr_filemap_have_rank_by_checkpoint(map, checkpoint_id, rel_rank)  && scr_bool_have_files(checkpoint_id, rel_rank)) {
      send_ranks[round-1] = rel_rank;
      found_files[rel_rank] = round;
      round++;
    } else {
      found_files[rel_rank] = 0; 
    }
  }

  /* tell everyone whether we have their files */
  int* has_my_files = (int*) malloc(sizeof(int) * scr_ranks_world);
  MPI_Alltoall(found_files, 1, MPI_INT, has_my_files, 1, MPI_INT, scr_comm_world);

  /* TODO: try to pick the closest node which has my files */
  /* identify the rank and round in which we'll fetch our files */
  int retrieve_rank  = -1;
  int retrieve_round = -1;
  for(i = 0; i < scr_ranks_world; i++) {
    /* pick the earliest round i can get my files from someone (round 1 may be ourselves) */
    int rel_rank = (scr_my_rank_world + i) % scr_ranks_world;
    if (has_my_files[rel_rank] > 0 && (has_my_files[rel_rank] < retrieve_round || retrieve_round < 0)) {
      retrieve_rank  = rel_rank;
      retrieve_round = has_my_files[rel_rank];
    }
  }

  /* for some redundancy schemes, we know at this point whether we can recover all files */
  int can_get_files = (retrieve_rank != -1);
  if (scr_copy_type != SCR_COPY_XOR && !scr_alltrue(can_get_files)) {
    free(send_ranks);
    free(has_my_files);
    free(found_files);
    if (!can_get_files) {
      scr_dbg(2, "Cannot find process that has my checkpoint files @ %s:%d", __FILE__, __LINE__);
    }
    scr_filemap_delete(map);
    return SCR_FAILURE;
  }

  /* get the maximum retrieve round */
  int max_rounds = 0;
  MPI_Allreduce(&retrieve_round, &max_rounds, 1, MPI_INT, MPI_MAX, scr_comm_world);

  /* tell everyone from which rank we intend to grab our files */
  int* retrieve_ranks = (int*) malloc(sizeof(int) * scr_ranks_world);
  MPI_Allgather(&retrieve_rank, 1, MPI_INT, retrieve_ranks, 1, MPI_INT, scr_comm_world);

  int tmp_rc = 0;

  /* run through rounds and exchange files */
  for (round = 1; round <= max_rounds; round++) {
    /* assume we don't need to send or receive any files this round */
    int send_rank = -1;
    int recv_rank = -1;
    int send_num  = 0;
    int recv_num  = 0;

    /* check whether I can potentially send to anyone in this round */
    if (round <= send_nranks) {
      /* have someone's files, check whether they are asking for them this round */
      int dst_rank = send_ranks[round-1];
      if (retrieve_ranks[dst_rank] == scr_my_rank_world) {
        /* need to send files this round, remember to whom and how many */
        send_rank = dst_rank;
        send_num  = scr_filemap_num_files(map, checkpoint_id, dst_rank);
      }
    }

    /* if I'm supposed to get my files this round, set the recv_rank */
    if (retrieve_round == round) { recv_rank = retrieve_rank; }

    /* TODO: another special case is to just move files if the processes are on the same node */
    /* if i'm sending to myself, just move (rename) each file */
    if (send_rank == scr_my_rank_world) {
      /* since we'll be adding and removing files to the same map for the same checkpoint and rank,
       * we can't just iterate through the files via hash_elem_next, so get a list of filename pointers */
      int nfiles = 0;
      char** files = NULL;
      scr_filemap_list_files(map, checkpoint_id, send_rank, &nfiles, &files);
      while (nfiles > 0) {
        nfiles--;

        /* get the existing filename and build the new one */
        char* file = files[nfiles];
        char path[SCR_MAX_FILENAME];
        char name[SCR_MAX_FILENAME];
        if (file != NULL) {
          scr_split_path(file, path, name);
        } else {
          strcpy(path, "UNKNOWN_PATH");
          strcpy(name, "UNKNOWN_NAME");
        }
        char newfile[SCR_MAX_FILENAME];
        char ckpt_path[SCR_MAX_FILENAME];
        scr_checkpoint_dir(checkpoint_id, ckpt_path);
        scr_build_path(newfile, ckpt_path, name);

        /* build the name of the existing and new meta files */
        char metafile[SCR_MAX_FILENAME];
        char newmetafile[SCR_MAX_FILENAME];
        scr_meta_name(metafile,    file);
        scr_meta_name(newmetafile, newfile);

        /* if the new file name is different from the old name, rename it */
        if (strcmp(file, newfile) != 0) {
          /* rename (move) the file */
          scr_dbg(2, "Round %d: rename(%s, %s)", round, file, newfile);
          tmp_rc = rename(file, newfile);
          if (tmp_rc != 0) {
            /* TODO: to cross mount points, if tmp_rc == EXDEV, open new file, copy, and delete orig */
            scr_err("Moving checkpoint file: rename(%s, %s) %m errno=%d @ %s:%d",
                    file, newfile, errno, __FILE__, __LINE__
                   );
            rc = SCR_FAILURE;
          }

          /* move the meta file */
          scr_dbg(2, "rename(%s, %s)", metafile, newmetafile);
          tmp_rc = rename(metafile, newmetafile);
          if (tmp_rc != 0) {
            /* TODO: to cross mount points, if tmp_rc == EXDEV, open new file, copy, and delete orig */
            scr_err("Moving checkpoint file: rename(%s, %s) %m errno=%d @ %s:%d",
                    metafile, newmetafile, errno, __FILE__, __LINE__
                   );
            rc = SCR_FAILURE;
          }

          /* add the new filename and remove the old name from our filemap */
          scr_filemap_add_file(map,    checkpoint_id, send_rank, newfile);
          /* TODO: tag file based on its filetype? */
          scr_filemap_remove_file(map, checkpoint_id, send_rank, file);
        }
      }
      /* free the list of filename pointers */
      if (files != NULL) { free(files); files = NULL; }
    } else {
      /* if we have files for this round, but the correspdonding rank doesn't need them, delete the files */
      if (round <= send_nranks && send_rank == -1) {
        int dst_rank = send_ranks[round-1];
        scr_unlink_rank_map(map, checkpoint_id, dst_rank);
      }

      /* sending to and/or recieving from another node */
      if (send_rank != -1 || recv_rank != -1) {
        /* remember the send rank for later, we'll set it to -1 eventually */
        int filemap_send_rank = send_rank;

        /* have someone to send to or receive from */
        int have_outgoing = 0;
        int have_incoming = 0;
        if (send_rank != -1) { have_outgoing = 1; }
        if (recv_rank != -1) { have_incoming = 1; }

        /* first, determine how many files I will be receiving and tell how many I will be sending */
        MPI_Request request[2];
        MPI_Status  status[2];
        int num_req = 0;
        if (have_incoming) {
          MPI_Irecv(&recv_num, 1, MPI_INT, recv_rank, 0, scr_comm_world, &request[num_req]);
          num_req++;
        }
        if (have_outgoing) {
          MPI_Isend(&send_num, 1, MPI_INT, send_rank, 0, scr_comm_world, &request[num_req]);
          num_req++;
        }
        MPI_Waitall(num_req, request, status);

        /* record how many files I will receive (need to distinguish between 0 files and not knowing) */
        if (have_incoming) {
          scr_filemap_set_expected_files(map, checkpoint_id, scr_my_rank_world, recv_num);
        }

        /* turn off send or receive flags if the file count is 0, nothing else to do */
        if (send_num == 0) {
          have_outgoing = 0;
          send_rank = -1;
        }
        if (recv_num == 0) {
          have_incoming = 0;
          recv_rank = -1;
        }

        /* TODO: would be best to order files according to file size */
        /* get a hash element of the first file to send if we have one */
        struct scr_hash_elem* file_elem = NULL;
        if (have_outgoing) {
          file_elem = scr_filemap_first_file(map, checkpoint_id, send_rank);
        }

        /* while we have a file to send or receive ... */
        while (have_incoming || have_outgoing) {
          /* get the filename and read its meta */
          struct scr_meta meta;
          char* file = NULL;
          if (have_outgoing) {
            file = scr_hash_elem_key(file_elem);
            scr_read_meta(file, &meta);
          }

          /* either sending or receiving a file this round, since we move files, it will be deleted or overwritten */
          char newfile[SCR_MAX_FILENAME];
          char ckpt_path[SCR_MAX_FILENAME];
          scr_checkpoint_dir(checkpoint_id, ckpt_path);
          scr_dbg(2, "Round %d: scr_swap_files(MOVE_FILES, %s, send=%d, %s, recv=%d)",
                  round, file, send_rank, ckpt_path, recv_rank
          );
          tmp_rc = scr_swap_files(MOVE_FILES, file, &meta, send_rank, ckpt_path, recv_rank, newfile, scr_comm_world);
          if (tmp_rc != SCR_SUCCESS) {
            scr_err("Swapping checkpoint files: scr_swap_files(..., %s, ..., %d, %s, %d, ...) @ %s:%d",
                    file, send_rank, ckpt_path, recv_rank, __FILE__, __LINE__
            );
            rc = SCR_FAILURE;
          }

          /* if we received a file, add it to the filemap and decrement our receive count */
          if (have_incoming) {
            scr_filemap_add_file(map, checkpoint_id, scr_my_rank_world, newfile);
            /* TODO: tag file based on its filetype? */
            recv_num--;
            if (recv_num == 0) {
              have_incoming = 0;
              recv_rank = -1;
            }
          }

          /* if we sent a file, get the next filename and decrement our send count */
          if (have_outgoing) {
            file_elem = scr_hash_elem_next(file_elem);
            send_num--;
            if (send_num == 0) {
              have_outgoing = 0;
              send_rank = -1;
            }
          }
        }

        /* if we sent to someone, remove those files from the filemap */
        if (filemap_send_rank != -1) {
          scr_filemap_remove_rank_by_checkpoint(map, checkpoint_id, filemap_send_rank);
        }
      }
    }
  }

  /* if we have more rounds than max rounds, delete the remainder of our files */
  for (round = max_rounds+1; round < send_nranks; round++) {
    /* have someone's files for this round, so delete them */
    int dst_rank = send_ranks[round-1];
    scr_unlink_rank_map(map, checkpoint_id, dst_rank);
  }

  free(send_ranks);
  free(retrieve_ranks);
  free(has_my_files);
  free(found_files);

  /* write out new filemap and free the memory resources */
  scr_filemap_write(scr_map_file, map);
  scr_filemap_delete(map);

  /* clean out any incomplete files before we start */
  scr_clean_files();

  /* TODO: if the exchange or redundancy rebuild failed, we should also delete any *good* files we received */

  /* return whether distribute succeeded, it does not ensure we have all of our files, only that the transfer complete without failure */
  return rc;
}

int scr_rebuild_files(int checkpoint_id)
{
  int rc = SCR_SUCCESS;

  /* TODO: to enable user to change redundancy scheme between runs,
   * we should inspect the cache to identify the redundancy scheme used for this cache */

  /* for xor, need to call rebuild_xor here */
  if (scr_copy_type == SCR_COPY_XOR) {
    rc = scr_attempt_rebuild_xor(checkpoint_id);
  }

  /* at this point, we should have all of our files, check that they're all here */

  /* check whether everyone has their files */
  int have_my_files = scr_bool_have_files(checkpoint_id, scr_my_rank_world);
  if (!scr_alltrue(have_my_files)) {
    if (scr_my_rank_world == 0) {
      scr_dbg(1, "Missing checkpoints files @ %s:%d",
              __FILE__, __LINE__
      );
    }
    return SCR_FAILURE;
  }

  /* for LOCAL and PARTNER, we need to apply the copy to complete the rebuild */
  if (scr_copy_type == SCR_COPY_LOCAL || scr_copy_type == SCR_COPY_PARTNER) {
    double bytes_copied = 0.0;
    rc = scr_copy_files(checkpoint_id, &bytes_copied);
  }

  return rc;
}

/* given a filename, return the full path to the file which the user should write to */
static int scr_route_file(int checkpoint_id, const char* file, char* newfile, int n)
{
  /* check that user's filename is not too long */
  if (strlen(file) >= SCR_MAX_FILENAME) {
    scr_abort(-1, "file name (%s) is longer than SCR_MAX_FILENAME (%d) @ %s:%d",
              file, SCR_MAX_FILENAME, __FILE__, __LINE__
    );
  }

  /* split user's filename into path and name components */
  char path[SCR_MAX_FILENAME];
  char name[SCR_MAX_FILENAME];
  scr_split_path(file, path, name);

  /* lookup the checkpoint directory */
  char ckpt_path[SCR_MAX_FILENAME];
  scr_checkpoint_dir(checkpoint_id, ckpt_path);

  /* check that new composed name does not overrun user buffer */
  if (strlen(ckpt_path) + 1 + strlen(name) + 1 > n) {
    scr_abort(-1, "file name (%s/%s) is longer than n (%d) @ %s:%d",
              ckpt_path, name, n, __FILE__, __LINE__
    );
  }

  /* build the composed name */
  scr_build_path(newfile, ckpt_path, name);

  return SCR_SUCCESS;
}

/* read in environment variables */
static int scr_get_params()
{
  char* value;

  /* user may want to disable SCR at runtime */
  if ((value = getenv("SCR_ENABLE")) != NULL) {
    scr_enabled = atoi(value);
  }

  /* bail out if not enabled -- nothing to do */
  if (! scr_enabled) {
    return SCR_SUCCESS;
  }

  /* read username from environment */
  if ((value = getenv("USER")) != NULL) {
    scr_username = strdup(value);
    if (scr_username == NULL) {
      scr_abort(-1, "Failed to allocate memory to record username (%s) @ %s:%d",
              value, __FILE__, __LINE__
      );
    }
  }

  /* read SLURM jobid from environment */
  if ((value = getenv("SLURM_JOBID")) != NULL) {
    scr_jobid = strdup(value);
    if (scr_jobid == NULL) {
      scr_abort(-1, "Failed to allocate memory to record jobid (%s) @ %s:%d",
              value, __FILE__, __LINE__
      );
    }
  }

  /* read jobname from environment */
  if ((value = getenv("SCR_JOB_NAME")) != NULL) {
    scr_jobname = strdup(value);
    if (scr_jobname == NULL) {
      scr_abort(-1, "Failed to allocate memory to record jobname (%s) @ %s:%d",
              value, __FILE__, __LINE__
      );
    }
  }

  /* read in our configuration parameters */
  scr_param_init();

  /* set debug verbosity level */
  if ((value = scr_param_get("SCR_DEBUG")) != NULL) {
    scr_debug = atoi(value);
  }

  /* set maximum number of checkpoints to keep in cache */
  if ((value = scr_param_get("SCR_CACHE_SIZE")) != NULL) {
    scr_cache_size = atoi(value);
  }

  /* select copy method */
  if ((value = scr_param_get("SCR_COPY_TYPE")) != NULL) {
    if (strcasecmp(value, "local") == 0) {
      scr_copy_type = SCR_COPY_LOCAL;
    } else if (strcasecmp(value, "partner") == 0) {
      scr_copy_type = SCR_COPY_PARTNER;
    } else if (strcasecmp(value, "xor") == 0) {
      scr_copy_type = SCR_COPY_XOR;
    } else {
      scr_abort(-1, "Please set SCR_COPY_TYPE to one of: LOCAL, PARTNER, XOR.");
    }
  }

  /* specify the number of tasks in xor set */
  if ((value = scr_param_get("SCR_XOR_SIZE")) != NULL) {
    scr_xor_size = atoi(value);
  }

  /* number of nodes between partners */
  if ((value = scr_param_get("SCR_PARTNER_DISTANCE")) != NULL) {
    scr_partner_distance = atoi(value);
  }

  /* if job has fewer than SCR_HALT_SECONDS remaining after completing a checkpoint, halt it */
  if ((value = scr_param_get("SCR_HALT_SECONDS")) != NULL) {
    scr_halt_seconds = atoi(value);
  }

  /* set MPI buffer size (file chunk size) */
  if ((value = scr_param_get("SCR_MPI_BUF_SIZE")) != NULL) {
    scr_mpi_buf_size = atoi(value);
  }

  /* whether to distribute files in filemap to ranks in SCR_Init */
  if ((value = scr_param_get("SCR_FETCH")) != NULL) {
    scr_fetch = atoi(value);
  }

  /* specify number of processes to read files simultaneously */
  if ((value = scr_param_get("SCR_FETCH_WIDTH")) != NULL) {
    scr_fetch_width = atoi(value);
  }

  /* whether to distribute files in filemap to ranks in SCR_Init */
  if ((value = scr_param_get("SCR_DISTRIBUTE")) != NULL) {
    scr_distribute = atoi(value);
  }

  /* specify how often we should flush files */
  if ((value = scr_param_get("SCR_FLUSH")) != NULL) {
    scr_flush = atoi(value);
  }

  /* specify number of processes to write files simultaneously */
  if ((value = scr_param_get("SCR_FLUSH_WIDTH")) != NULL) {
    scr_flush_width = atoi(value);
  }

  /* specify number of processes to write files simultaneously */
  if ((value = scr_param_get("SCR_FLUSH_ON_RESTART")) != NULL) {
    scr_flush_on_restart = atoi(value);
  }

  /* specify whether to compute CRC on redundancy copy */
  if ((value = scr_param_get("SCR_CRC_ON_COPY")) != NULL) {
    scr_crc_on_copy = atoi(value);
  }

  /* specify whether to compute CRC on fetch and flush */
  if ((value = scr_param_get("SCR_CRC_ON_FLUSH")) != NULL) {
    scr_crc_on_flush = atoi(value);
  }

  /* override default checkpoint frequency (number of times to call Need_checkpoint between checkpoints) */
  if ((value = scr_param_get("SCR_CHECKPOINT_FREQUENCY")) != NULL) {
    scr_checkpoint_frequency = atoi(value);
  }

  /* override default minimum number of seconds between checkpoints */
  if ((value = scr_param_get("SCR_CHECKPOINT_SECONDS")) != NULL) {
    scr_checkpoint_seconds = atoi(value);
  }

  /* override default maximum allowed checkpointing overhead */
  if ((value = scr_param_get("SCR_CHECKPOINT_OVERHEAD")) != NULL) {
    float overhead = 0.0;
    sscanf(value, "%f", &overhead);
    scr_checkpoint_overhead = (double) overhead;
  }

  /* override default base directory for checkpoint cache */
  if ((value = scr_param_get("SCR_CACHE_BASE")) != NULL) {
    strcpy(scr_cache_base, value);
  }

  /* override default scr_par_prefix (parallel file system prefix) */
  if ((value = scr_param_get("SCR_PREFIX")) != NULL) {
    strcpy(scr_par_prefix, value);
  }

  /* if user didn't set with SCR_PREFIX, pick up the current working directory as a default */
  /* TODO: wonder whether this convenience will cause more problems than its worth?
   * may lead to writing large checkpoint file sets to the executable directory, which may not be a parallel file system */
  if (strcmp(scr_par_prefix, "") == 0) {
    if (getcwd(scr_par_prefix, sizeof(scr_par_prefix)) == NULL) {
      scr_abort(-1, "Problem reading current working directory (getcwd() errno=%d %m) @ %s:%d",
              errno, __FILE__, __LINE__
      );
    }
  }

  /* done reading parameters, can release the data structures now */
  scr_param_finalize();

  return SCR_SUCCESS;
}

/*
=========================================
User interface functions
=========================================
*/

int SCR_Init()
{
  /* read in environment variables */
  scr_get_params();

  /* bail out if not enabled -- nothing to do */
  if (! scr_enabled) {
    return SCR_SUCCESS;
  }

  /* create a context for the library */
  MPI_Comm_dup(MPI_COMM_WORLD, &scr_comm_world);

  /* find our rank and the size of our world */
  MPI_Comm_rank(scr_comm_world, &scr_my_rank_world);
  MPI_Comm_size(scr_comm_world, &scr_ranks_world);

  /* get my hostname */
  if (gethostname(scr_my_hostname, sizeof(scr_my_hostname)) != 0) {
    scr_err("Call to gethostname failed @ %s:%d",
            __FILE__, __LINE__
    );
    MPI_Abort(scr_comm_world, 0);
  }

  /* create a scr_comm_local communicator to hold all tasks on the same node */
  /* TODO: maybe a better way to identify processes on the same node?
   * TODO: could improve scalability here using a bitonic sort and prefix scan
   * TODO: need something to work on systems with IPv6
   * Assumes: same int(IP) ==> same node 
   *   1. Get IP address as integer data type
   *   2. Allgather IP addresses from all processes
   *   3. Set color id to process with highest rank having the same IP */

  /* get IP address as integer data type */
  struct hostent *hostent;
  hostent = gethostbyname(scr_my_hostname);
  if (hostent == NULL) {
    scr_err("Fetching host information: gethostbyname(%s) @ %s:%d",
            scr_my_hostname, __FILE__, __LINE__
    );
    MPI_Abort(scr_comm_world, 0);
  }
  int host_id = ((struct in_addr *) hostent->h_addr_list[0])->s_addr;

  /* gather all host_id values */
  int* host_ids = (int*) malloc(scr_ranks_world * sizeof(int));
  if (host_ids == NULL) {
    scr_err("Can't allocate memory to determine which processes are on the same node @ %s:%d",
            __FILE__, __LINE__
    );
    MPI_Abort(scr_comm_world, 0);
  }
  MPI_Allgather(&host_id, 1, MPI_INT, host_ids, 1, MPI_INT, scr_comm_world);

  /* set host_index to the highest rank having the same host_id as we do */
  int i;
  int host_index = 0;
  for (i=0; i < scr_ranks_world; i++) {
    if (host_ids[i] == host_id) {
      host_index = i;
    }
  }
  free(host_ids);

  /* finally create the communicator holding all ranks on the same node */
  MPI_Comm_split(scr_comm_world, host_index, scr_my_rank_world, &scr_comm_local);

  /* find our position in the local communicator */
  MPI_Comm_rank(scr_comm_local, &scr_my_rank_local);
  MPI_Comm_size(scr_comm_local, &scr_ranks_local);

  /* Based on my local rank, create communicators consisting of all tasks at same local rank level */
  MPI_Comm_split(scr_comm_world, scr_my_rank_local, scr_my_rank_world, &scr_comm_level);

  /* find our position in the level communicator */
  MPI_Comm_rank(scr_comm_level, &scr_my_rank_level);
  MPI_Comm_size(scr_comm_level, &scr_ranks_level);

  /* split the scr_comm_level communicator based on xor set size to create our xor communicator */
  int rel_rank = scr_my_rank_level / scr_partner_distance;
  int mod_rank = scr_my_rank_level % scr_partner_distance;
  scr_xor_set_id = (rel_rank / scr_xor_size) * scr_partner_distance + mod_rank;
  MPI_Comm_split(scr_comm_level, scr_xor_set_id, scr_my_rank_world, &scr_comm_xor);

  /* find our position in the xor communicator */
  MPI_Comm_rank(scr_comm_xor, &scr_my_rank_xor);
  MPI_Comm_size(scr_comm_xor, &scr_ranks_xor);

  /* TODO: LOCAL needs no partner */
  /* find left and right-hand-side partners */
  if (scr_copy_type == SCR_COPY_LOCAL || scr_copy_type == SCR_COPY_PARTNER) {
    scr_set_partners(scr_comm_level, scr_partner_distance,
        &scr_lhs_rank, &scr_lhs_rank_world, scr_lhs_hostname,
        &scr_rhs_rank, &scr_rhs_rank_world, scr_rhs_hostname);
  } else if (scr_copy_type == SCR_COPY_XOR) {
    scr_set_partners(scr_comm_xor, 1,
        &scr_lhs_rank, &scr_lhs_rank_world, scr_lhs_hostname,
        &scr_rhs_rank, &scr_rhs_rank_world, scr_rhs_hostname);
  }
  scr_dbg(2, "LHS partner: %s (%d)  -->  My name: %s (%d)  -->  RHS partner: %s (%d)",
          scr_lhs_hostname, scr_lhs_rank_world, scr_my_hostname, scr_my_rank_world, scr_rhs_hostname, scr_rhs_rank_world
  );

  /* TODO: LOCAL needs no partner */
  /* check that we have a valid partner node */
  int have_partners = 1;
  if (scr_lhs_hostname[0] == '\0' ||
      scr_rhs_hostname[0] == '\0' ||
      strcmp(scr_lhs_hostname, scr_my_hostname) == 0 ||
      strcmp(scr_rhs_hostname, scr_my_hostname) == 0
     )
  {
    have_partners = 0;
  }

  /* TODO: LOCAL needs no partner */
  /* check that all tasks have valid partners */
  if (!scr_alltrue(have_partners)) {
    if (scr_my_rank_world == 0) {
      scr_err("No valid partner node detected (perhaps too few nodes).  Disabling SCR.");
    }
    scr_enabled = 0;
    return SCR_SUCCESS;
  }

  /* connect to the SCR log database if enabled */
  if (scr_my_rank_world == 0 && scr_log_enable) {
    /* initialize the logging support */
    if (scr_log_init() != SCR_SUCCESS) {
      scr_err("Failed to initialize SCR logging, disabling logging @ %s:%d",
              __FILE__, __LINE__
      );
      scr_log_enable = 0;
    }

    if (scr_log_enable && scr_username != NULL && scr_jobname != NULL) {
      /* register this job in the logging */
      time_t job_start = scr_log_seconds();
      if (scr_log_job(scr_username, scr_jobname, job_start) == SCR_SUCCESS) {
        /* record the start time for this run */
        scr_log_run(job_start);
      } else {
        scr_err("Failed to log job for username %s and jobname %s, disabling logging @ %s:%d",
                scr_username, scr_jobname, __FILE__, __LINE__
        );
        scr_log_enable = 0;
      }
    } else {
      scr_err("Failed to read username or jobname from environment, disabling logging @ %s:%d",
              __FILE__, __LINE__
      );
      scr_log_enable = 0;
    }
  }

  /* build the control directory name */
  if (strlen(scr_cntl_base) + 1 + strlen(scr_username) + strlen("/scr.") + strlen(scr_jobid) >= sizeof(scr_cntl_prefix)) {
    scr_abort(-1, "Control directory name (%s/%s%s%s) is too long for internal buffer of size %lu bytes @ %s:%d",
            scr_cntl_base, scr_username, "/scr.", scr_jobid, sizeof(scr_cntl_prefix), __FILE__, __LINE__
    );
  }
  sprintf(scr_cntl_prefix, "%s/%s/scr.%s", scr_cntl_base, scr_username, scr_jobid);

  /* build the cache directory name (unless it was set explicitly by the user) */
  if (strcmp(scr_cache_prefix, "") == 0) {
    if (strlen(scr_cache_base) + 1 + strlen(scr_username) + strlen("/scr.") + strlen(scr_jobid) >= sizeof(scr_cache_prefix)) {
      scr_abort(-1, "Cache directory name (%s/%s%s%s) is too long for internal buffer of size %lu bytes @ %s:%d",
              scr_cache_base, scr_username, "/scr.", scr_jobid, sizeof(scr_cache_prefix), __FILE__, __LINE__
      );
    }
    sprintf(scr_cache_prefix, "%s/%s/scr.%s", scr_cache_base, scr_username, scr_jobid);
  }

  /* TODO: should we check for required space in cache at this point? */

  /* the master on each node creates the control directory */
  if (scr_my_rank_local == 0) {
    scr_dbg(2, "Creating control directory: %s", scr_cntl_prefix);
    if (scr_mkdir(scr_cntl_prefix, S_IRWXU | S_IRWXG) != SCR_SUCCESS) {
      scr_abort(-1, "Failed to create control directory: %s @ %s:%d",
              scr_cntl_prefix, __FILE__, __LINE__
      );
    }
    /* TODO: open permissions to control directory so other users (admins) can halt the job? */
    /*
    mode_t mode = umask(0000);
    scr_mkdir(scr_cntl_prefix, S_IRWXU | S_IRWXG | S_IRWXO);
    umask(mode);
    */
  }

  /* the master on each node creates the cache directory */
  if (scr_my_rank_local == 0) {
    scr_dbg(2, "Creating cache directory: %s", scr_cache_prefix);
    if (scr_mkdir(scr_cache_prefix, S_IRWXU | S_IRWXG) != SCR_SUCCESS) {
      scr_abort(-1, "Failed to create cache directory: %s @ %s:%d",
              scr_cache_prefix, __FILE__, __LINE__
      );
    }
  }

  /* build the file names using the control directory prefix */
  scr_build_path(scr_halt_file,  scr_cntl_prefix, "halt.scrinfo");
  scr_build_path(scr_flush_file, scr_cntl_prefix, "flush.scrinfo");
  scr_build_path(scr_nodes_file, scr_cntl_prefix, "nodes.scrinfo");
  sprintf(scr_map_file, "%s/filemap_%d.scrinfo", scr_cntl_prefix, scr_my_rank_local);
  sprintf(scr_master_map_file, "%s/filemap_node.scrinfo", scr_cntl_prefix);

  /* record the number of nodes being used in this job to the nodes file */
  int num_nodes = 0;
  MPI_Allreduce(&scr_ranks_level, &num_nodes, 1, MPI_INT, MPI_MAX, scr_comm_world);
  if (scr_my_rank_local == 0) { scr_write_nodes(num_nodes); }

  /* initialize halt info before calling scr_bool_check_halt
   * and set the halt seconds in our halt data structure,
   * this will be overridden if a value is already set in the halt file */
  scr_halt_init(&halt);
  halt.halt_seconds = scr_halt_seconds;

  /* initialize the timestamp recording the last checkpoint time */
  if (scr_my_rank_world == 0) {
    scr_time_checkpoint_end = MPI_Wtime();
  }

  /* sync everyone up */
  MPI_Barrier(scr_comm_world);

  /* now all processes are initialized (careful when moving this line up or down) */
  scr_initialized = 1;

  /* TODO: if SCR_NEED_SPARE=1, perhaps move this after distribute but before fetch to rebuild in parallel? */
  /* exit right now if we need to halt */
  scr_bool_check_halt(1, 0);

  int rc = SCR_FAILURE;

  double time_start, time_end, time_diff;

  /* if scr_fetch or scr_flush is enabled, check that scr_par_prefix is set */
  if ((scr_fetch != 0 || scr_flush > 0) && strcmp(scr_par_prefix, "") == 0) {
    if (scr_my_rank_world == 0) {
      scr_err("SCR_PREFIX must be set to use SCR_FETCH or SCR_FLUSH");
      scr_halt("SCR_INIT_FAILED");
    }
    MPI_Barrier(scr_comm_world);
    exit(0);
  }

  /* TODO: need to protect from signal here? */
  /* attempt to distribute files for a restart */
  if (rc != SCR_SUCCESS && scr_distribute) {
    int distribute_attempted = 0;

    /* start timer */
    time_t now;
    if (scr_my_rank_world == 0) {
      now = scr_log_seconds();
      time_start = MPI_Wtime();
    }

    /* distribute the filemaps to other ranks on the node */
    scr_distribute_filemaps();

    /* start from most recent checkpoint and work backwards */
    int max_id;
    do {
      /* clean incomplete files from our cache */
      scr_clean_files();

      /* get the latest checkpoint id we have files for */
      int checkpoint_id = scr_latest_checkpoint();

      /* find the maximum checkpoint id across all ranks */
      MPI_Allreduce(&checkpoint_id, &max_id, 1, MPI_INT, MPI_MAX, scr_comm_world);

      if (max_id != -1) {
        distribute_attempted = 1;

        /* create a directory for this checkpoint */
        scr_create_checkpoint_dir(max_id);

        /* distribute the files for the this checkpoint */
        scr_distribute_files(max_id);

        /* attempt to rebuild redundancy for this checkpoint */
        rc = scr_rebuild_files(max_id);
        if (rc == SCR_SUCCESS) {
          /* rebuild succeeded, update scr_checkpoint_id to the latest checkpoint and set max_id to break the loop */
          scr_checkpoint_id = max_id;
          max_id = -1;
        } else {
          if (scr_my_rank_world == 0) {
            scr_dbg(1, "scr_distribute_files: Could not distribute / rebuild checkpoint %d", max_id);
            if (scr_log_enable) {
              scr_log_event("DISTRIBUTE FAILED", NULL, &max_id, &now, NULL);
            }
          }

          /* rebuild failed, delete any files I have for this checkpoint */
          scr_unlink_checkpoint(max_id);
          scr_remove_checkpoint_dir(max_id);
          scr_remove_checkpoint_flush(max_id);
        }
      }
    } while (max_id != -1);

    /* TODO: may want to keep cache_size sets around, but we need to rebuild each one of them */
    /* delete all checkpoints up to most recent */
    if (scr_checkpoint_id != 0) {
      /* find the maximum number of checkpoints across all ranks */
      int max_num_checkpoints = 0;
      int num_checkpoints = scr_num_checkpoints();
      MPI_Allreduce(&num_checkpoints, &max_num_checkpoints, 1, MPI_INT, MPI_MAX, scr_comm_world);

      /* while this maximum number is greater than 1, find the oldest checkpoint and delete it */
      while (max_num_checkpoints > 1) {
        /* find the oldest checkpoint across all ranks */
        int min_id = max_id;
        int checkpoint_id = scr_oldest_checkpoint();
        if (checkpoint_id == -1) {
          checkpoint_id = max_id;
        }
        MPI_Allreduce(&checkpoint_id, &min_id, 1, MPI_INT, MPI_MIN, scr_comm_world);

        /* if this oldest checkpoint is not also the latest (last one), delete it */
        if (min_id != max_id) {
          scr_unlink_checkpoint(min_id);
          scr_remove_checkpoint_dir(min_id);
          scr_remove_checkpoint_flush(min_id);
        }

        /* find the maximum number of checkpoints across all ranks again */
        max_num_checkpoints = 0;
        num_checkpoints = scr_num_checkpoints();
        MPI_Allreduce(&num_checkpoints, &max_num_checkpoints, 1, MPI_INT, MPI_MAX, scr_comm_world);
      }
    }

    /* stop timer and report performance */
    if (scr_my_rank_world == 0) {
      time_end = MPI_Wtime();
      time_diff = time_end - time_start;

      if (distribute_attempted) {
        if (rc == SCR_SUCCESS) {
          scr_dbg(1, "scr_distribute_files: succeess, checkpoint %d, seconds %f", scr_checkpoint_id, time_diff);
          if (scr_log_enable) {
            scr_log_event("DISTRIBUTE SUCCEEDED", NULL, &scr_checkpoint_id, &now, &time_diff);
          }
        } else {
          scr_dbg(1, "scr_distribute_files: failed, seconds %f", time_diff);
          if (scr_log_enable) {
            scr_log_event("DISTRIBUTE FAILED", NULL, NULL, &now, &time_diff);
          }
        }
      }
    }

    /* TODO: need to make the flushfile specific to each checkpoint */
    /* if distribute succeeds, check whether we should flush on restart */
    if (rc == SCR_SUCCESS) {
      if (scr_flush_on_restart) {
        /* always flush on restart if scr_flush_on_restart is set */
        scr_flush_files(scr_checkpoint_id);
      } else {
        /* otherwise, flush only if we need to flush */
        scr_check_flush();
      }
    }
  }

  /* TODO: there is some risk here of cleaning the cache when we shouldn't if given a badly placed nodeset
     for a restart job step within an allocation with lots of spares. */
  /* if the distribute fails, let's clear the cache */
  if (rc != SCR_SUCCESS) {
    scr_unlink_all();
  }

  /* attempt to fetch files from parallel file system into cache */
  if (rc != SCR_SUCCESS && scr_fetch) {
    /* and now try to fetch files from the parallel file system */
    if (scr_my_rank_world == 0) {
      time_start = MPI_Wtime();
    }

    /* first attempt to fetch files from current */
    char dir[SCR_MAX_FILENAME];
    scr_build_path(dir, scr_par_prefix, "scr.current");
    rc = scr_fetch_files(dir);
    if (rc != SCR_SUCCESS) {
      /* current failed, delete the symlink */
      unlink(dir);

      /* try old */
      scr_build_path(dir, scr_par_prefix, "scr.old");
      rc = scr_fetch_files(dir);

      if (rc != SCR_SUCCESS) {
        /* old failed, delete the symlink */
        unlink(dir);
      } else {
        /* old worked, delete old */
        char target[SCR_MAX_FILENAME];
        int len = readlink(dir, target, sizeof(target));
        target[len] = '\0';
        unlink(dir);

        /* create current and point to old target */
        scr_build_path(dir, scr_par_prefix, "scr.current");
        symlink(target, dir);
      }
    }
    if (scr_my_rank_world == 0) {
      time_end = MPI_Wtime();
      time_diff = time_end - time_start;
      scr_dbg(1, "scr_fetch_files: return code %d, seconds %f", rc, time_diff);
    }
  }

  /* TODO: there is some risk here of cleaning the cache when we shouldn't if given a badly placed nodeset
     for a restart job step within an allocation with lots of spares. */
  /* if the fetch fails, lets clear the cache */
  if (rc != SCR_SUCCESS) {
    scr_unlink_all();
  }

  /* both the distribute and the fetch failed, maybe SCR_FETCH=0? */
  if (rc != SCR_SUCCESS) {
    if (!scr_fetch) {
      /* if user told us explicitly not to fetch, assume he knows what he's doing */
      rc = SCR_SUCCESS;
    } else {
      /* otherwise, print a warning */
      if (scr_my_rank_world == 0) {
        scr_err("SCR_Init() failed to copy checkpoint set into cache @ %s:%d", __FILE__, __LINE__);
        scr_err("Perhaps SCR_FETCH is enabled and SCR_PREFIX is not set correctly,");
        scr_err("or perhaps there is no current checkpoint set?");
      }
      rc = SCR_SUCCESS;
    }
  }

  /* sync everyone before returning to ensure that subsequent calls to SCR functions are valid */
  MPI_Barrier(scr_comm_world);

  /* start the clock for measuring the compute time */
  if (scr_my_rank_world == 0) {
    scr_timestamp_compute_start = scr_log_seconds();
    scr_time_compute_start = MPI_Wtime();
  }

  /* all done, ready to go */
  return rc;
}

/* Close down and clean up */
int SCR_Finalize()
{ 
  /* bail out if not enabled -- nothing to do */
  if (! scr_enabled) {
    return SCR_SUCCESS;
  }

  /* bail out if not initialized -- will get bad results */
  if (! scr_initialized) {
    scr_err("SCR has not been initialized @ %s:%d", __FILE__, __LINE__);
    return SCR_FAILURE;
  }

  /* stop the clock for measuring the compute time */
  if (scr_my_rank_world == 0) {
    scr_time_compute_end = MPI_Wtime();
  }

  /* if we reach SCR_Finalize, assume that we should not restart the job */
  scr_halt("SCR_FINALIZE_CALLED");

  /* flush checkpoint set if we need to */
  if (scr_bool_need_flush(scr_checkpoint_id)) {
    scr_flush_files(scr_checkpoint_id);
  }

  /* disconnect from database */
  if (scr_my_rank_world == 0 && scr_log_enable) {
    scr_log_finalize();
  }

  /* free off the library's communicators */
  MPI_Comm_free(&scr_comm_xor);
  MPI_Comm_free(&scr_comm_level);
  MPI_Comm_free(&scr_comm_local);
  MPI_Comm_free(&scr_comm_world);

  /* free memory allocated for variables */
  if (scr_username) { free(scr_username);  scr_username = NULL; }
  if (scr_jobid)    { free(scr_jobid);     scr_jobid    = NULL; }
  if (scr_jobname)  { free(scr_jobname);   scr_jobname  = NULL; }

  return SCR_SUCCESS;
}

/* sets flag to 1 if a checkpoint should be taken, flag is set to 0 otherwise */
int SCR_Need_checkpoint(int* flag)
{
  /* always say yes if not enabled (maybe no is better?) */
  if (! scr_enabled) {
    *flag = 1;
    return SCR_SUCCESS;
  }

  /* say no if not initialized */
  if (! scr_initialized) {
    scr_err("SCR has not been initialized @ %s:%d", __FILE__, __LINE__);
    return SCR_FAILURE;
  }

  /* track the number of times a user has called SCR_Need_checkpoint */
  scr_need_checkpoint_id++;

  /* assume we don't need to checkpoint */
  *flag = 0;

  /* check whether a halt condition is active (don't halt, just be sure to return 1 in this case) */
  int need_halt = scr_bool_check_halt(0, 0);
  if (need_halt) {
    *flag = 1;
  }

  /* TODO: account for MTBF, time to flush, etc. */
  /* if we don't need to halt, check whether we can afford to checkpoint */
  if (scr_my_rank_world == 0) {
    /* if checkpoint frequency is set, check the current checkpoint id */
    if (scr_checkpoint_frequency > 0 && scr_need_checkpoint_id % scr_checkpoint_frequency == 0) {
      *flag = 1;
    }

    /* if checkpoint seconds is set, check the time since the last checkpoint */
    if (scr_checkpoint_seconds > 0) {
      double now_seconds = MPI_Wtime();
      if ((int)(now_seconds - scr_time_checkpoint_end) >= scr_checkpoint_seconds) {
        *flag = 1;
      }
    }

    /* check whether we can afford to checkpoint based on the max allowed checkpoint overhead, if set */
    if (scr_checkpoint_overhead > 0) {
      /* TODO: could init the cost estimate via environment variable or stats from previous run */
      if (scr_time_checkpoint_count == 0) {
        /* if we haven't taken a checkpoint, we need to take one in order to get a cost estimate */
        *flag = 1;
      } else if (scr_time_checkpoint_count > 0) {
        /* based on average time of checkpoint, current time, and time that last checkpoint ended,
         * determine overhead of checkpoint if we took one right now */
        double now = MPI_Wtime();
        double avg_cost = scr_time_checkpoint_total / (double) scr_time_checkpoint_count;
        double percent_cost = avg_cost / (now - scr_time_checkpoint_end + avg_cost) * 100.0;

        /* if our current percent cost is less than allowable overhead, indicate that it's time for a checkpoint */
        if (percent_cost < scr_checkpoint_overhead) {
          *flag = 1;
        }
      }
    }

    /* no way to determine whether we need to checkpoint, so always say yes */
    if (scr_checkpoint_frequency <= 0 && scr_checkpoint_seconds <= 0 && scr_checkpoint_overhead <= 0) {
      *flag = 1;
    }
  }

  /* rank 0 makes the decision */
  MPI_Bcast(flag, 1, MPI_INT, 0, scr_comm_world);

  return SCR_SUCCESS;
}

/* informs SCR that a fresh checkpoint set is about to start */
int SCR_Start_checkpoint()
{
  /* TODO: need to have SCR still make the directory in this case */
  /* if not enabled, we still need rank 0 to create the directory, but we can't use any SCR variables */
  if (! scr_enabled) {
/*
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    if (rank == 0) {
      char path[SCR_MAX_FILENAME];
      char name[SCR_MAX_FILENAME];
      scr_split_path(file, path, name);
      scr_mkdir(path, S_IRWXU);
    }
    MPI_Barrier(MPI_COMM_WORLD);
*/
    return SCR_SUCCESS;
  }
  
  /* bail out if not initialized -- will get bad results */
  if (! scr_initialized) {
    scr_err("SCR has not been initialized @ %s:%d", __FILE__, __LINE__);
    return SCR_FAILURE;
  }

  /* make sure everyone is ready to start before we overwrite the current checkpoint set */
  MPI_Barrier(scr_comm_world);

  if (scr_my_rank_world == 0) {
    /* stop the clock for measuring the compute time */
    scr_time_compute_end = MPI_Wtime();

    /* start the clock to record how long it takes to checkpoint */
    scr_timestamp_checkpoint_start = scr_log_seconds();
    scr_time_checkpoint_start = MPI_Wtime();
  }

  /* delete checkpoint sets to make room based on the cache size */
  struct scr_filemap* map = scr_filemap_new();
  scr_filemap_read(scr_map_file, map);
  int num = scr_filemap_num_checkpoints(map);
  scr_filemap_delete(map);
  while (num >= scr_cache_size) {
    /* delete the oldest checkpoint id */
    int oldest = scr_oldest_checkpoint();
    scr_unlink_checkpoint(oldest);
    scr_remove_checkpoint_dir(oldest);
    scr_remove_checkpoint_flush(oldest);
    num--;
  }

  /* increment our checkpoint counter */
  scr_checkpoint_id++;

  /* make directory in cache to store files for this checkpoint */
  scr_create_checkpoint_dir(scr_checkpoint_id);

  /* clear the file list for this process in this checkpoint */
  scr_hash_delete(scr_checkpoint_file_list);
  scr_checkpoint_file_list = scr_hash_new();

  /* print a message to differentiate some verbose debug messages */
  if (scr_my_rank_world == 0) {
    scr_dbg(1, "Starting checkpoint %d", scr_checkpoint_id);
  }

  return SCR_SUCCESS;
}

/* given a filename, return the full path to the file which the user should write to */
int SCR_Route_file(const char* file, char* newfile)
{
  int n = SCR_MAX_FILENAME;

  /* TODO: should we have SCR do anything at all in this case? */
  /* if not enabled, make straight copy of file into newfile and bail out */
  if (! scr_enabled) {
    strncpy(newfile, file, n);
    return SCR_SUCCESS;
  }
  
  /* bail out if not initialized -- will get bad results */
  if (! scr_initialized) {
    scr_err("SCR has not been initialized @ %s:%d", __FILE__, __LINE__);
    return SCR_FAILURE;
  }

  /* route the file */
  if (scr_route_file(scr_checkpoint_id, file, newfile, n) != SCR_SUCCESS) {
    return SCR_FAILURE;
  }

  /* add the routed file to our file list for this checkpoint */
  if (scr_hash_set_kv(scr_checkpoint_file_list, newfile, NULL) == NULL) {
    return SCR_FAILURE;
  }

  return SCR_SUCCESS;
}

/* completes the checkpoint set and marks it as valid or not */
int SCR_Complete_checkpoint(int valid)
{
  /* bail out if not enabled -- nothing to do */
  if (! scr_enabled) {
    return SCR_SUCCESS;
  }

  /* bail out if not initialized -- will get bad results */
  if (! scr_initialized) {
    scr_err("SCR has not been initialized @ %s:%d", __FILE__, __LINE__);
    return SCR_FAILURE;
  }

  /* read in our filemap */
  struct scr_filemap* map = scr_filemap_new();
  scr_filemap_read(scr_map_file, map);

  /* mark each file as complete and add each one to our filemap */
  struct scr_hash_elem* elem;
  for (elem = scr_hash_elem_first(scr_checkpoint_file_list);
       elem != NULL;
       elem = scr_hash_elem_next(elem))
  {
    /* get the filename */
    char* file = scr_hash_elem_key(elem);

    /* fill out meta info for our file */
    struct scr_meta meta;
    scr_set_meta(&meta, file, scr_my_rank_world, scr_ranks_world, scr_checkpoint_id, SCR_FILE_FULL, valid);

    /* mark the file as complete */
    scr_complete(file, &meta);

    /* add the file to our filemap */
    scr_filemap_add_file(map, scr_checkpoint_id, scr_my_rank_world, file);

    /* TODO: tag file based on its filetype? */
  }

  /* set the number of expected files, write the filemap, and delete the filemap object */
  scr_filemap_set_expected_files(map, scr_checkpoint_id, scr_my_rank_world,
    scr_filemap_num_files(map, scr_checkpoint_id, scr_my_rank_world));
  scr_filemap_write(scr_map_file, map);
  scr_filemap_delete(map);

  /* apply redundancy scheme */
  double bytes_copied = 0.0;
  int rc = scr_copy_files(scr_checkpoint_id, &bytes_copied);

  /* record the cost of the checkpoint */
  if (scr_my_rank_world == 0) {
    /* stop the clock for this checkpoint */
    scr_time_checkpoint_end = MPI_Wtime();

    /* compute and record the cost for this checkpoint */
    double cost = scr_time_checkpoint_end - scr_time_checkpoint_start;
    if (cost < 0) {
      scr_err("Checkpoint end time (%f) is less than start time (%f) @ %s:%d",
              scr_time_checkpoint_end, scr_time_checkpoint_start, __FILE__, __LINE__
      );
      cost = 0;
    }
    scr_time_checkpoint_total += cost;
    scr_time_checkpoint_count++;

    /* log data on the checkpoint in the database */
    if (scr_log_enable) {
      char ckpt_path[SCR_MAX_FILENAME];
      scr_checkpoint_dir(scr_checkpoint_id, ckpt_path);
      scr_log_transfer("CHECKPOINT", ckpt_path, ckpt_path, &scr_checkpoint_id, &scr_timestamp_checkpoint_start, &cost, &bytes_copied);
    }
  }

  /* print out a debug message with the result of the copy */
  if (scr_my_rank_world == 0) {
    scr_dbg(1, "Completed checkpoint %d with return code %d",
            scr_checkpoint_id, rc
    );
  }

  /* if copy is good, check whether we need to flush or halt, otherwise delete the checkpoint to conserve space */
  if (rc == SCR_SUCCESS) {
    scr_set_flush(scr_checkpoint_id, SCR_FLUSH_CACHE);
    scr_check_flush();
    scr_bool_check_halt(1, 1);
  } else {
    scr_unlink_checkpoint(scr_checkpoint_id);
    scr_remove_checkpoint_dir(scr_checkpoint_id);
    scr_remove_checkpoint_flush(scr_checkpoint_id);
  }

  /* make sure everyone is ready before we exit */
  MPI_Barrier(scr_comm_world);

  /* start the clock for measuring the compute time */
  if (scr_my_rank_world == 0) {
    scr_timestamp_compute_start = scr_log_seconds();
    scr_time_compute_start = MPI_Wtime();
  }

  /* TODO: stop timer to time cost of checkpoint (complete will stop timer) */

  return rc;
}
