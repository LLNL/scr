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

#include "scr_conf.h"
#include "scr.h"
#include "scr_err.h"
#include "scr_io.h"
#include "scr_util.h"
#include "scr_meta.h"
#include "scr_dataset.h"
#include "scr_halt.h"
#include "scr_log.h"
#include "scr_hash.h"
#include "scr_filemap.h"
#include "scr_param.h"
#include "scr_env.h"
#include "scr_index_api.h"

#ifdef HAVE_LIBGCS
#include "gcs.h"
#endif /* HAVE_LIBGCS */

/*
=========================================
Globals
=========================================
*/

#define SCR_TEST_AND_HALT (1)
#define SCR_TEST_BUT_DONT_HALT (2)

#define SCR_CURRENT_LINK "scr.current"

/* copy file operation flags: copy file vs. move file */
#define COPY_FILES 0
#define MOVE_FILES 1

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

static char* scr_cntl_prefix = NULL;
static char scr_par_prefix[SCR_MAX_FILENAME]   = "";

/* these files live in the control directory */
static char scr_master_map_file[SCR_MAX_FILENAME];
static char scr_map_file[SCR_MAX_FILENAME];
static char scr_transfer_file[SCR_MAX_FILENAME];

/* we keep the halt, flush, and nodes files in the prefix directory
 * so that the batch script and / or external commands can access them */
static char scr_halt_file[SCR_MAX_FILENAME];
static char scr_flush_file[SCR_MAX_FILENAME];
static char scr_nodes_file[SCR_MAX_FILENAME];

static scr_filemap* scr_map = NULL;
static scr_hash* scr_halt_hash = NULL;

static char* scr_username    = NULL;       /* username of owner for running job */
static char* scr_jobid       = NULL;       /* unique job id string of current job */
static char* scr_jobname     = NULL;       /* jobname string, used to tie different runs together */
static char* scr_clustername = NULL;       /* name of cluster running job */
static int scr_dataset_id    = 0;          /* keeps track of the dataset id */
static int scr_checkpoint_id = 0;          /* keeps track of the checkpoint id */
static int scr_in_output     = 0;          /* flag tracks whether we are between start and complete calls */
static int scr_initialized   = 0;          /* indicates whether the library has been initialized */
static int scr_enabled       = SCR_ENABLE; /* indicates whether the library is enabled */
static int scr_debug         = SCR_DEBUG;  /* set debug verbosity */
static int scr_log_enable    = SCR_LOG_ENABLE; /* whether to log SCR events */

static int scr_page_size = 0; /* records block size for aligning MPI and file buffers */

static int scr_cache_size       = SCR_CACHE_SIZE;       /* set number of checkpoints to keep at one time */
static int scr_copy_type        = SCR_COPY_TYPE;        /* select which redundancy algorithm to use */
static int scr_hop_distance     = SCR_HOP_DISTANCE;     /* number of nodes away to choose parnter */
static int scr_set_size         = SCR_SET_SIZE;         /* specify number of tasks in xor set */
static size_t scr_mpi_buf_size  = SCR_MPI_BUF_SIZE;     /* set MPI buffer size to chunk file transfer */

static int scr_halt_seconds     = SCR_HALT_SECONDS; /* secs remaining in allocation before job should be halted */

static int scr_distribute       = SCR_DISTRIBUTE;       /* whether to call scr_distribute_files during SCR_Init */
static int scr_fetch            = SCR_FETCH;            /* whether to call scr_fetch_files during SCR_Init */
static int scr_fetch_width      = SCR_FETCH_WIDTH;      /* specify number of processes to read files simultaneously */
static int scr_flush            = SCR_FLUSH;            /* how many checkpoints between flushes */
static int scr_flush_width      = SCR_FLUSH_WIDTH;      /* specify number of processes to write files simultaneously */
static int scr_flush_on_restart = SCR_FLUSH_ON_RESTART; /* specify whether to flush cache on restart */
static int scr_global_restart   = SCR_GLOBAL_RESTART;   /* set if code must be restarted from parallel file system */
static int scr_flush_async      = SCR_FLUSH_ASYNC;      /* whether to use asynchronous flush */
static double scr_flush_async_bw = SCR_FLUSH_ASYNC_BW;  /* bandwidth limit imposed during async flush */
static double scr_flush_async_percent = SCR_FLUSH_ASYNC_PERCENT;  /* runtime limit imposed during async flush */
static size_t scr_file_buf_size = SCR_FILE_BUF_SIZE;    /* set buffer size to chunk file copies to/from parallel file system */

static int scr_crc_on_copy   = SCR_CRC_ON_COPY;   /* whether to enable crc32 checks during scr_swap_files() */
static int scr_crc_on_flush  = SCR_CRC_ON_FLUSH;  /* whether to enable crc32 checks during flush and fetch */
static int scr_crc_on_delete = SCR_CRC_ON_DELETE; /* whether to enable crc32 checks when deleting checkpoints */

static int scr_preserve_user_directories = SCR_PRESERVE_USER_DIRECTORIES;
static int scr_use_containers            = SCR_USE_CONTAINERS;
static unsigned long scr_container_size  = SCR_CONTAINER_SIZE;

static int    scr_checkpoint_interval = SCR_CHECKPOINT_INTERVAL; /* times to call Need_checkpoint between checkpoints */
static int    scr_checkpoint_seconds   = SCR_CHECKPOINT_SECONDS;   /* min number of seconds between checkpoints */
static double scr_checkpoint_overhead  = SCR_CHECKPOINT_OVERHEAD;  /* max allowed overhead for checkpointing */
static int    scr_need_checkpoint_count = 0;    /* tracks the number of times Need_checkpoint has been called */
static double scr_time_checkpoint_total = 0.0; /* keeps a running total of the time spent to checkpoint */
static int    scr_time_checkpoint_count = 0;   /* keeps a running count of the number of checkpoints taken */

static time_t scr_timestamp_checkpoint_start;  /* record timestamp of start of checkpoint */
static double scr_time_checkpoint_start;       /* records the start time of the current checkpoint */
static double scr_time_checkpoint_end;         /* records the end time of the current checkpoint */

static time_t scr_timestamp_compute_start;     /* record timestamp of start of compute phase */
static double scr_time_compute_start;          /* records the start time of the current compute phase */
static double scr_time_compute_end;            /* records the end time of the current compute phase */

static MPI_Comm scr_comm_world = MPI_COMM_NULL; /* dup of MPI_COMM_WORLD */
static MPI_Comm scr_comm_local = MPI_COMM_NULL; /* contains all tasks local to the same node */
static MPI_Comm scr_comm_level = MPI_COMM_NULL; /* contains tasks across all nodes at the same local rank level */

static int scr_ranks_world = 0; /* number of ranks in the job */
static int scr_ranks_local = 0; /* number of ranks on my node */
static int scr_ranks_level = 0; /* number of ranks at my level (i.e., processes with same local rank across nodes) */

static int  scr_my_rank_world = MPI_PROC_NULL;  /* my rank in world */
static int  scr_my_rank_local = MPI_PROC_NULL;  /* my local rank on my node */
static int  scr_my_rank_level = MPI_PROC_NULL;  /* my rank in processes at my level */

static char scr_my_hostname[256] = "";

static scr_hash* scr_cachedesc_hash = NULL;
static scr_hash* scr_reddesc_hash   = NULL;

/*
=========================================
Define redundancy descriptor structure
=========================================
*/

struct scr_reddesc {
  int      enabled;
  int      index;
  int      interval;
  char*    base;
  char*    directory;
  int      copy_type;
  int      hop_distance;
  int      set_size;
  MPI_Comm comm;
  int      groups;
  int      group_id;
  int      ranks;
  int      my_rank;
  int      lhs_rank;
  int      lhs_rank_world;
  char     lhs_hostname[256];
  int      rhs_rank;
  int      rhs_rank_world;
  char     rhs_hostname[256];
};

static int scr_nreddescs = 0;
static struct scr_reddesc* scr_reddescs = NULL;

/*
=========================================
Error and Debug Messages
=========================================
*/

/* print error message to stdout */
void scr_err(const char *fmt, ...)
{
  va_list argp;
  fprintf(stdout, "SCR %s ERROR: rank %d on %s: ", SCR_ERR_VERSION, scr_my_rank_world, scr_my_hostname);
  va_start(argp, fmt);
  vfprintf(stdout, fmt, argp);
  va_end(argp);
  fprintf(stdout, "\n");
}

/* print warning message to stdout */
void scr_warn(const char *fmt, ...)
{
  va_list argp;
  fprintf(stdout, "SCR %s WARNING: rank %d on %s: ", SCR_ERR_VERSION, scr_my_rank_world, scr_my_hostname);
  va_start(argp, fmt);
  vfprintf(stdout, fmt, argp);
  va_end(argp);
  fprintf(stdout, "\n");
}

/* print message to stdout if scr_debug is set and it is >= level */
void scr_dbg(int level, const char *fmt, ...)
{
  va_list argp;
  if (level == 0 || (scr_debug > 0 && scr_debug >= level)) {
    fprintf(stdout, "SCR %s: rank %d on %s: ", SCR_ERR_VERSION, scr_my_rank_world, scr_my_hostname);
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
  fprintf(stderr, "SCR %s ABORT: rank %d on %s: ", SCR_ERR_VERSION, scr_my_rank_world, scr_my_hostname);
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

/* returns true (non-zero) if flag on each process in scr_comm_world is true */
static int scr_alltrue(int flag)
{
  int all_true = 0;
  MPI_Allreduce(&flag, &all_true, 1, MPI_INT, MPI_LAND, scr_comm_world);
  return all_true;
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
  while (dist > ranks) {
    dist -= ranks;
  }
  while (dist < 0) {
    dist += ranks;
  }

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
  MPI_Irecv(lhs_hostname,    sizeof(scr_my_hostname), MPI_CHAR, lhs, 0, comm, &request[0]);
  MPI_Isend(scr_my_hostname, sizeof(scr_my_hostname), MPI_CHAR, rhs, 0, comm, &request[1]);
  MPI_Waitall(2, request, status);

  /* shift hostnames to the left */
  MPI_Irecv(rhs_hostname,    sizeof(scr_my_hostname), MPI_CHAR, rhs, 0, comm, &request[0]);
  MPI_Isend(scr_my_hostname, sizeof(scr_my_hostname), MPI_CHAR, lhs, 0, comm, &request[1]);
  MPI_Waitall(2, request, status);

  /* shift rank in scr_comm_world to the right */
  MPI_Irecv(lhs_rank_world,     1, MPI_INT, lhs, 0, comm, &request[0]);
  MPI_Isend(&scr_my_rank_world, 1, MPI_INT, rhs, 0, comm, &request[1]);
  MPI_Waitall(2, request, status);

  /* shift rank in scr_comm_world to the left */
  MPI_Irecv(rhs_rank_world,     1, MPI_INT, rhs, 0, comm, &request[0]);
  MPI_Isend(&scr_my_rank_world, 1, MPI_INT, lhs, 0, comm, &request[1]);
  MPI_Waitall(2, request, status);

  return SCR_SUCCESS;
}

/*
=========================================
Configuration file
=========================================
*/

/* read parameters from config file and fill in hash (parallel) */
int scr_config_read(const char* file, scr_hash* hash)
{
  int rc = SCR_FAILURE;

  /* only rank 0 reads the file */
  if (scr_my_rank_world == 0) {
    rc = scr_config_read_serial(file, hash);
  }

  /* broadcast whether rank 0 read the file ok */
  MPI_Bcast(&rc, 1, MPI_INT, 0, scr_comm_world);

  /* if rank 0 read the file, broadcast the hash */
  if (rc == SCR_SUCCESS) {
    rc = scr_hash_bcast(hash, 0, scr_comm_world);
  }

  return rc;
}

/*
=========================================
Redundancy descriptor functions
=========================================
*/

/* initialize the specified redundancy descriptor struct */
static int scr_reddesc_init(struct scr_reddesc* c)
{
  /* check that we got a valid redundancy descriptor */
  if (c == NULL) {
    scr_err("No redundancy descriptor to fill from hash @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* initialize the descriptor */
  c->enabled        =  0;
  c->index          = -1;
  c->interval       = -1;
  c->base           = NULL;
  c->directory      = NULL;
  c->copy_type      = SCR_COPY_NULL;
  c->comm           = MPI_COMM_NULL;
  c->groups         =  0;
  c->group_id       = -1;
  c->ranks          =  0;
  c->my_rank        = MPI_PROC_NULL;
  c->lhs_rank       = MPI_PROC_NULL;
  c->lhs_rank_world = MPI_PROC_NULL;
  strcpy(c->lhs_hostname, "");
  c->rhs_rank       = MPI_PROC_NULL;
  c->rhs_rank_world = MPI_PROC_NULL;
  strcpy(c->rhs_hostname, "");

  return SCR_SUCCESS;
}

/* free any memory associated with the specified redundancy descriptor struct */
static int scr_reddesc_free(struct scr_reddesc* c)
{
  /* free the strings we strdup'd */
  if (c->base != NULL) {
    free(c->base);
    c->base = NULL;
  }

  if (c->directory != NULL) {
    free(c->directory);
    c->directory = NULL;
  }

  /* free the communicator we created */
  if (c->comm != MPI_COMM_NULL) {
    MPI_Comm_free(&c->comm);
  }

  return SCR_SUCCESS;
}

/* given a checkpoint id and a list of redundancy descriptor structs,
 * select and return a pointer to a descriptor for the specified checkpoint id */
static struct scr_reddesc* scr_ckptdesc_get(int id, int nckpts, struct scr_reddesc* ckpts)
{
  struct scr_reddesc* c = NULL;

  /* pick the redundancy descriptor that is:
   *   1) enabled
   *   2) has the highest interval that evenly divides id */
  int i;
  int interval = 0;
  for (i=0; i < nckpts; i++) {
    if (ckpts[i].enabled &&
        interval < ckpts[i].interval &&
        id % ckpts[i].interval == 0)
    {
      c = &ckpts[i];
      interval = ckpts[i].interval;
    }
  }

  return c;
}

/* convert the specified redundancy descritpor struct into a corresponding hash */
static int scr_reddesc_store_to_hash(const struct scr_reddesc* c, scr_hash* hash)
{
  /* check that we got a valid pointer to a redundancy descriptor and a hash */
  if (c == NULL || hash == NULL) {
    return SCR_FAILURE;
  }

  /* clear the hash */
  scr_hash_unset_all(hash);

  /* set the ENABLED key */
  scr_hash_set_kv_int(hash, SCR_CONFIG_KEY_ENABLED, c->enabled);

  /* set the INDEX key */
  scr_hash_set_kv_int(hash, SCR_CONFIG_KEY_INDEX, c->index);

  /* set the INTERVAL key */
  scr_hash_set_kv_int(hash, SCR_CONFIG_KEY_INTERVAL, c->interval);

  /* set the BASE key */
  if (c->base != NULL) {
    scr_hash_set_kv(hash, SCR_CONFIG_KEY_BASE, c->base);
  }

  /* set the DIRECTORY key */
  if (c->directory != NULL) {
    scr_hash_set_kv(hash, SCR_CONFIG_KEY_DIRECTORY, c->directory);
  }

  /* set the TYPE key */
  switch (c->copy_type) {
  case SCR_COPY_LOCAL:
    scr_hash_set_kv(hash, SCR_CONFIG_KEY_TYPE, "LOCAL");
    break;
  case SCR_COPY_PARTNER:
    scr_hash_set_kv(hash, SCR_CONFIG_KEY_TYPE, "PARTNER");
    break;
  case SCR_COPY_XOR:
    scr_hash_set_kv(hash, SCR_CONFIG_KEY_TYPE, "XOR");
    break;
  }

  /* set the GROUP_ID and GROUP_RANK keys */
  scr_hash_set_kv_int(hash, SCR_CONFIG_KEY_GROUPS,     c->groups);
  scr_hash_set_kv_int(hash, SCR_CONFIG_KEY_GROUP_ID,   c->group_id);
  scr_hash_set_kv_int(hash, SCR_CONFIG_KEY_GROUP_SIZE, c->ranks);
  scr_hash_set_kv_int(hash, SCR_CONFIG_KEY_GROUP_RANK, c->my_rank);

  /* set the DISTANCE and SIZE */
  scr_hash_set_kv_int(hash, SCR_CONFIG_KEY_HOP_DISTANCE, c->hop_distance);
  scr_hash_set_kv_int(hash, SCR_CONFIG_KEY_SET_SIZE,     c->set_size);

  return SCR_SUCCESS;
}

/* build a redundancy descriptor corresponding to the specified hash,
 * this function is collective, because it issues MPI calls */
static int scr_reddesc_create_from_hash(struct scr_reddesc* c, int index, const scr_hash* hash)
{
  int rc = SCR_SUCCESS;

  /* check that we got a valid redundancy descriptor */
  if (c == NULL) {
    scr_err("No redundancy descriptor to fill from hash @ %s:%d",
            __FILE__, __LINE__
    );
    rc = SCR_FAILURE;
  }

  /* check that we got a valid pointer to a hash */
  if (hash == NULL) {
    scr_err("No hash specified to build redundancy descriptor from @ %s:%d",
            __FILE__, __LINE__
    );
    rc = SCR_FAILURE;
  }

  /* check that everyone made it this far */
  if (! scr_alltrue(rc == SCR_SUCCESS)) {
    if (c != NULL) {
      c->enabled = 0;
    }
    return SCR_FAILURE;
  }

  /* initialize the descriptor */
  scr_reddesc_init(c);

  char* value = NULL;

  /* enable / disable the checkpoint */
  c->enabled = 1;
  value = scr_hash_elem_get_first_val(hash, SCR_CONFIG_KEY_ENABLED);
  if (value != NULL) {
    c->enabled = atoi(value);
  }

  /* index of the checkpoint */
  c->index = index;
  value = scr_hash_elem_get_first_val(hash, SCR_CONFIG_KEY_INDEX);
  if (value != NULL) {
    c->index = atoi(value);
  }

  /* set the checkpoint interval, default to 1 unless specified otherwise */
  c->interval = 1;
  value = scr_hash_elem_get_first_val(hash, SCR_CONFIG_KEY_INTERVAL);
  if (value != NULL) {
    c->interval = atoi(value);
  }

  /* set the base checkpoint directory */
  value = scr_hash_elem_get_first_val(hash, SCR_CONFIG_KEY_BASE);
  if (value != NULL) {
    c->base = strdup(value);
  }

  /* build the checkpoint directory name */
  value = scr_hash_elem_get_first_val(hash, SCR_CONFIG_KEY_DIRECTORY);
  if (value != NULL) {
    /* directory name already set, just copy it */
    c->directory = strdup(value);
  } else if (c->base != NULL) {
    /* directory name was not already set, so we need to build it */
    char str[100];
    sprintf(str, "%d", c->index);
    int dirname_size = strlen(c->base) + 1 + strlen(scr_username) + strlen("/scr.") + strlen(scr_jobid) +
                       strlen("/index.") + strlen(str) + 1;
    c->directory = (char*) malloc(dirname_size);
    sprintf(c->directory, "%s/%s/scr.%s/index.%s", c->base, scr_username, scr_jobid, str);
  }
    
  /* set the partner hop distance */
  c->hop_distance = scr_hop_distance;
  value = scr_hash_elem_get_first_val(hash, SCR_CONFIG_KEY_HOP_DISTANCE);
  if (value != NULL) {
    c->hop_distance = atoi(value);
  }

  /* set the xor set size */
  c->set_size = scr_set_size;
  value = scr_hash_elem_get_first_val(hash, SCR_CONFIG_KEY_SET_SIZE);
  if (value != NULL) {
    c->set_size = atoi(value);
  }

  /* read the checkpoint type from the hash, and build our checkpoint communicator */
  value = scr_hash_elem_get_first_val(hash, SCR_CONFIG_KEY_TYPE);
  if (value != NULL) {
    if (strcasecmp(value, "LOCAL") == 0) {
      c->copy_type = SCR_COPY_LOCAL;
    } else if (strcasecmp(value, "PARTNER") == 0) {
      c->copy_type = SCR_COPY_PARTNER;
    } else if (strcasecmp(value, "XOR") == 0) {
      c->copy_type = SCR_COPY_XOR;
    } else {
      c->enabled = 0;
      if (scr_my_rank_world == 0) {
        scr_warn("Unknown copy type %s in redundancy descriptor %d, disabling checkpoint @ %s:%d",
                value, c->index, __FILE__, __LINE__
        );
      }
    }

    /* CONVENIENCE: if all ranks are on the same node, change checkpoint type to LOCAL,
     * we do this so single-node jobs can run without requiring the user to change the copy type */
    if (scr_ranks_local == scr_ranks_world) {
      if (scr_my_rank_world == 0) {
        if (c->copy_type != SCR_COPY_LOCAL) {
          /* print a warning if we changed things on the user */
          scr_warn("Forcing copy type to LOCAL in redundancy descriptor %d @ %s:%d",
                  c->index, __FILE__, __LINE__
          );
        }
      }
      c->copy_type = SCR_COPY_LOCAL;
    }

    /* build the checkpoint communicator */
    char* group_id_str   = scr_hash_elem_get_first_val(hash, SCR_CONFIG_KEY_GROUP_ID);
    char* group_rank_str = scr_hash_elem_get_first_val(hash, SCR_CONFIG_KEY_GROUP_RANK);
    if (group_id_str != NULL && group_rank_str != NULL) {
      /* we already have a group id and rank, use that to rebuild the communicator */
      int group_id   = atoi(group_id_str);
      int group_rank = atoi(group_rank_str);
      MPI_Comm_split(scr_comm_world, group_id, group_rank, &c->comm);
    } else {
      /* otherwise, build the communicator based on the copy type and other parameters */
      int rel_rank, mod_rank, split_id;
      switch (c->copy_type) {
      case SCR_COPY_LOCAL:
        /* not going to communicate with anyone, so just dup COMM_SELF */
        MPI_Comm_dup(MPI_COMM_SELF, &c->comm);
        break;
      case SCR_COPY_PARTNER:
        /* dup the global level communicator */
        MPI_Comm_dup(scr_comm_level, &c->comm);
        break;
      case SCR_COPY_XOR:
        /* split the scr_comm_level communicator based on xor set size to create our xor communicator */
        rel_rank = scr_my_rank_level / c->hop_distance;
        mod_rank = scr_my_rank_level % c->hop_distance;
        split_id = (rel_rank / c->set_size) * c->hop_distance + mod_rank;
        MPI_Comm_split(scr_comm_level, split_id, scr_my_rank_world, &c->comm);
        break;
      }
    }

    /* find our position in the checkpoint communicator */
    MPI_Comm_rank(c->comm, &c->my_rank);
    MPI_Comm_size(c->comm, &c->ranks);

    /* for our group id, use the global rank of the rank 0 task in our checkpoint comm */
    c->group_id = scr_my_rank_world;
    MPI_Bcast(&c->group_id, 1, MPI_INT, 0, c->comm);

    /* count the number of groups */
    int group_master = (c->my_rank == 0) ? 1 : 0;
    MPI_Allreduce(&group_master, &c->groups, 1, MPI_INT, MPI_SUM, scr_comm_world);

    /* find left and right-hand-side partners (LOCAL needs no partner nodes) */
    if (c->copy_type == SCR_COPY_PARTNER) {
      scr_set_partners(c->comm, c->hop_distance,
          &c->lhs_rank, &c->lhs_rank_world, c->lhs_hostname,
          &c->rhs_rank, &c->rhs_rank_world, c->rhs_hostname
      );
    } else if (c->copy_type == SCR_COPY_XOR) {
      scr_set_partners(c->comm, 1,
          &c->lhs_rank, &c->lhs_rank_world, c->lhs_hostname,
          &c->rhs_rank, &c->rhs_rank_world, c->rhs_hostname
      );
    }

    /* check that we have a valid partner node (LOCAL needs no partner nodes) */
    if (c->copy_type == SCR_COPY_PARTNER || c->copy_type == SCR_COPY_XOR) {
      if (strcmp(c->lhs_hostname, "") == 0 ||
          strcmp(c->rhs_hostname, "") == 0 ||
          strcmp(c->lhs_hostname, scr_my_hostname) == 0 ||
          strcmp(c->rhs_hostname, scr_my_hostname) == 0)
      {
        c->enabled = 0;
        scr_warn("Failed to find partner processes for redundancy descriptor %d, disabling checkpoint, too few nodes? @ %s:%d",
                c->index, __FILE__, __LINE__
        );
      } else {
        scr_dbg(2, "LHS partner: %s (%d)  -->  My name: %s (%d)  -->  RHS partner: %s (%d)",
                c->lhs_hostname, c->lhs_rank_world, scr_my_hostname, scr_my_rank_world, c->rhs_hostname, c->rhs_rank_world
        );
      }
    }

    /* if anyone has disabled this checkpoint, everyone needs to */
    if (! scr_alltrue(c->enabled)) {
      c->enabled = 0;
    }
  }

  return SCR_SUCCESS;
}

/* many times we just need the directory for the checkpoint,
 * it's overkill to create the whole descriptor each time */
static char* scr_reddesc_val_from_filemap(scr_filemap* map, int ckpt, int rank, char* name)
{
  /* check that we have a pointer to a map and a character buffer */
  if (map == NULL || name == NULL) {
    return NULL;
  }

  /* create an empty hash to store the redundancy descriptor hash from the filemap */
  scr_hash* desc = scr_hash_new();
  if (desc == NULL) {
    return NULL;
  }

  /* get the redundancy descriptor hash from the filemap */
  if (scr_filemap_get_desc(map, ckpt, rank, desc) != SCR_SUCCESS) {
    scr_hash_delete(desc);
    return NULL;
  }

  /* copy the directory from the redundancy descriptor hash, if it's set */
  char* dup = NULL;
  char* val;
  if (scr_hash_util_get_str(desc, name, &val) == SCR_SUCCESS) {
    dup = strdup(val);
  }

  /* delete the hash object */
  scr_hash_delete(desc);

  return dup;
}

/* many times we just need the directory for the checkpoint,
 * it's overkill to create the whole descripter each time */
static char* scr_reddesc_base_from_filemap(scr_filemap* map, int ckpt, int rank)
{
  char* value = scr_reddesc_val_from_filemap(map, ckpt, rank, SCR_CONFIG_KEY_BASE);
  return value;
}

/* many times we just need the directory for the checkpoint,
 * it's overkill to create the whole descripter each time */
static char* scr_reddesc_dir_from_filemap(scr_filemap* map, int ckpt, int rank)
{
  char* value = scr_reddesc_val_from_filemap(map, ckpt, rank, SCR_CONFIG_KEY_DIRECTORY);
  return value;
}

/* build a redundancy descriptor struct from its corresponding hash stored in the filemap,
 * this function is collective */
static int scr_reddesc_create_from_filemap(scr_filemap* map, int id, int rank, struct scr_reddesc* c)
{
  /* check that we have a pointer to a map and a redundancy descriptor */
  if (map == NULL || c == NULL) {
    return SCR_FAILURE;
  }

  /* create an empty hash to store the redundancy descriptor hash from the filemap */
  scr_hash* desc = scr_hash_new();
  if (desc == NULL) {
    return SCR_FAILURE;
  }

  /* get the redundancy descriptor hash from the filemap */
  if (scr_filemap_get_desc(map, id, rank, desc) != SCR_SUCCESS) {
    scr_hash_delete(desc);
    return SCR_FAILURE;
  }

  /* fill in our redundancy descriptor */
  if (scr_reddesc_create_from_hash(c, -1, desc) != SCR_SUCCESS) {
    scr_hash_delete(desc);
    return SCR_FAILURE;
  }

  /* delete the hash object */
  scr_hash_delete(desc);

  return SCR_SUCCESS;
}

static int scr_reddesc_create_list()
{
  /* set the number of redundancy descriptors */
  scr_nreddescs = 0;
  scr_hash* tmp = scr_hash_get(scr_reddesc_hash, SCR_CONFIG_KEY_CKPTDESC);
  if (tmp != NULL) {
    scr_nreddescs = scr_hash_size(tmp);
  }

  /* allocate our redundancy descriptors */
  if (scr_nreddescs > 0) {
    scr_reddescs = (struct scr_reddesc*) malloc(scr_nreddescs * sizeof(struct scr_reddesc));
    /* TODO: check for errors */
  }

  int all_valid = 1;

  /* iterate over each of our checkpoints filling in each corresponding descriptor */
  int i;
  for (i=0; i < scr_nreddescs; i++) {
    /* get the info hash for this checkpoint */
    scr_hash* ckpt_hash = scr_hash_get_kv_int(scr_reddesc_hash, SCR_CONFIG_KEY_CKPTDESC, i);
    if (scr_reddesc_create_from_hash(&scr_reddescs[i], i, ckpt_hash) != SCR_SUCCESS) {
      all_valid = 0;
    }
  }

  /* determine whether everyone found a valid redundancy descriptor */
  if (!all_valid) {
    return SCR_FAILURE;
  }
  return SCR_SUCCESS;
}

static int scr_reddesc_free_list()
{
  /* iterate over and free each of our redundancy descriptors */
  if (scr_nreddescs > 0 && scr_reddescs != NULL) {
    int i;
    for (i=0; i < scr_nreddescs; i++) {
      scr_reddesc_free(&scr_reddescs[i]);
    }
  }

  /* set the count back to zero */
  scr_nreddescs = 0;

  /* and free off the memory allocated */
  if (scr_reddescs != NULL) {
    free(scr_reddescs);
    scr_reddescs = NULL;
  }

  return SCR_SUCCESS;
}

/*
=========================================
Checkpoint functions
=========================================
*/

/*
  GOALS: 
    - support different number of processes per node on
      a restart
    - support multiple files per rank per checkpoint
    - support multiple checkpoints at different cache levels
*/

/* searches through the cache descriptors and returns the size of the cache whose BASE
 * matches the specified base */
static int scr_cachedesc_size(const char* target)
{
  /* iterate over each of our cache descriptors */
  scr_hash* index = scr_hash_get(scr_cachedesc_hash, SCR_CONFIG_KEY_CACHEDESC);
  scr_hash_elem* elem;
  for (elem = scr_hash_elem_first(index);
       elem != NULL;
       elem = scr_hash_elem_next(elem))
  {
    /* get a reference to the hash for the current descriptor */
    scr_hash* h = scr_hash_elem_hash(elem);

    /* get the BASE value for this descriptor */
    char* base;
    if (scr_hash_util_get_str(h, SCR_CONFIG_KEY_BASE, &base) == SCR_SUCCESS) {
      /* if the BASE is set, and if it matches the specified base, lookup and return the size */
      if (strcmp(base, target) == 0) {
        int size;
        if (scr_hash_util_get_int(h, SCR_CONFIG_KEY_SIZE, &size) == SCR_SUCCESS) {
          return size;
        }

        /* found the base, but couldn't find the size, so return a size of 0 */
        return 0;
      }
    }
  }

  /* couldn't find the specified base, so return a size of 0 */
  return 0;
}

/* returns name of the cache directory for a given redundancy descriptor and dataset id */
static int scr_cache_dir_get(const struct scr_reddesc* c, int id, char* dir)
{
  /* fatal error if c or c->directory is not set */
  if (c == NULL || c->directory == NULL) {
    scr_abort(-1, "NULL redundancy descriptor or NULL dataset directory @ %s:%d",
            __FILE__, __LINE__
    );
  }

  /* now build the checkpoint directory name */
  sprintf(dir, "%s/dataset.%d", c->directory, id);
  return SCR_SUCCESS;
}

/* create a cache directory given a redundancy descriptor and dataset id,
 * waits for all tasks on the same node before returning */
static int scr_cache_dir_create(const struct scr_reddesc* c, int id)
{
  int rc = SCR_SUCCESS;

  /* have the master rank on each node create the directory */
  if (scr_my_rank_local == 0) {
    /* get the name of the checkpoint directory for the given id */
    char dir[SCR_MAX_FILENAME];
    scr_cache_dir_get(c, id, dir);

    /* create the directory */
    scr_dbg(2, "Creating dataset directory: %s", dir);
    rc = scr_mkdir(dir, S_IRWXU);

    /* check that we created the directory successfully, fatal error if not */
    if (rc != SCR_SUCCESS) {
      scr_abort(-1, "Failed to create dataset directory, aborting @ %s:%d",
                __FILE__, __LINE__
      );
    }
  }

  /* force all tasks on the same node to wait to ensure the directory is ready before returning */
  MPI_Barrier(scr_comm_local);

  return SCR_SUCCESS;
}

/* remove a cache directory given a redundancy descriptor and dataset id,
 * waits for all tasks on the same node before removing */
static int scr_cache_dir_delete(const char* prefix, int id)
{
  /* force all tasks on the same node to wait before we delete the directory */
  MPI_Barrier(scr_comm_local);

  /* have the master rank on each node remove the directory */
  if (scr_my_rank_local == 0) {
    char dir[SCR_MAX_FILENAME];
    sprintf(dir, "%s/dataset.%d", prefix, id);
    scr_dbg(2, "Removing dataset directory: %s", dir);
    rmdir(dir);
  }

  return SCR_SUCCESS;
}

/* removes entries in flush file for given dataset id */
static int scr_flush_file_dataset_remove(int id)
{
  /* only rank 0 needs to write the file */
  if (scr_my_rank_world == 0) {
    /* read the flush file into hash */
    scr_hash* hash = scr_hash_new();
    scr_hash_read(scr_flush_file, hash);

    /* delete this dataset id from the flush file */
    scr_hash_unset_kv_int(hash, SCR_FLUSH_KEY_DATASET, id);

    /* write the hash back to the flush file */
    scr_hash_write(scr_flush_file, hash);

    /* delete the hash */
    scr_hash_delete(hash);
  }
  return SCR_SUCCESS;
}

static int scr_compute_crc(scr_filemap* map, int id, int rank, const char* file)
{
  /* compute crc for the file */
  uLong crc_file;
  if (scr_crc32(file, &crc_file) != SCR_SUCCESS) {
    scr_err("Failed to compute crc for file %s @ %s:%d",
            file, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* allocate a new meta data object */
  scr_meta* meta = scr_meta_new();
  if (meta == NULL) {
    scr_abort(-1, "Failed to allocate meta data object @ %s:%d",
              __FILE__, __LINE__
    );
  }

  /* read meta data from filemap */
  if (scr_filemap_get_meta(map, id, rank, file, meta) != SCR_SUCCESS) {
    return SCR_FAILURE;
  }

  int rc = SCR_SUCCESS;

  /* read crc value from meta data */
  uLong crc_meta;
  if (scr_meta_get_crc32(meta, &crc_meta) == SCR_SUCCESS) {
    /* check that the values are the same */
    if (crc_file != crc_meta) {
      rc = SCR_FAILURE;
    }
  } else {
    /* record crc in filemap */
    scr_meta_set_crc32(meta, crc_file);
    scr_filemap_set_meta(map, id, rank, file, meta);
  }

  /* free our meta data object */
  scr_meta_delete(meta);

  return rc;
}

/* remove all files associated with specified dataset */
static int scr_cache_delete(scr_filemap* map, int id)
{
  /* print a debug messages */
  if (scr_my_rank_world == 0) {
    scr_dbg(1, "Deleting dataset %d from cache", id);
  }

  /* for each file of each rank we have for this dataset, delete the file */
  scr_hash_elem* rank_elem;
  for (rank_elem = scr_filemap_first_rank_by_dataset(map, id);
       rank_elem != NULL;
       rank_elem = scr_hash_elem_next(rank_elem))
  {
    /* get the rank id */
    int rank = scr_hash_elem_key_int(rank_elem);

    scr_hash_elem* file_elem;
    for (file_elem = scr_filemap_first_file(map, id, rank);
         file_elem != NULL;
         file_elem = scr_hash_elem_next(file_elem))
    {
      /* get the filename */
      char* file = scr_hash_elem_key(file_elem); 

      /* check file's crc value (monitor that cache hardware isn't corrupting files on us) */
      if (scr_crc_on_delete) {
        /* TODO: if corruption, need to log */
        if (scr_compute_crc(map, id, rank, file) != SCR_SUCCESS) {
          scr_err("Failed to verify CRC32 before deleting file %s, bad drive? @ %s:%d",
                  file, __FILE__, __LINE__
          );
        }
      }

      /* delete the file */
      unlink(file);
    }
  }

  /* remove the cache directory for this dataset */
  char* dir = scr_reddesc_dir_from_filemap(map, id, scr_my_rank_world);
  if (dir != NULL) {
    /* remove the dataset directory from cache */
    scr_cache_dir_delete(dir, id);
    free(dir);
  } else {
    /* TODO: abort! */
  }

  /* delete any entry in the flush file for this dataset */
  scr_flush_file_dataset_remove(id);

  /* TODO: remove data from transfer file for this dataset */

  /* remove this dataset from the filemap, and write new filemap to disk */
  scr_filemap_remove_dataset(map, id);
  scr_filemap_write(scr_map_file, map);

  return SCR_SUCCESS;
}

/* each process passes in an ordered list of dataset ids along with a current index,
 * this function identifies the next smallest id across all processes and returns
 * this id in current, it also updates index on processes as appropriate */
static int scr_next_dataset(int ndsets, const int* dsets, int* index, int* current)
{
  int dset_index = *index;

  /* get the next dataset we have in our list */
  int id = -1;
  if (dset_index < ndsets) {
    id = dsets[dset_index];
  }

  /* find the maximum dataset id across all ranks */
  int current_id;
  MPI_Allreduce(&id, &current_id, 1, MPI_INT, MPI_MAX, scr_comm_world);

  /* if any process has any dataset, identify the smallest */
  if (current_id != -1) {
    /* now find the minimum dataset id (that's not -1) */
    if (id == -1) {
      /* if we don't have a dataset, set our id to the max to avoid
       * picking -1 as the minimum */
      id = current_id;
    }
    MPI_Allreduce(&id, &current_id, 1, MPI_INT, MPI_MIN, scr_comm_world);

    /* if the current id matches our id, increment our index for the next iteration */
    if (current_id == id) {
      dset_index++;
    }
  }

  /* update caller's current index value and the dataset id we settled on */
  *index   = dset_index;
  *current = current_id;

  return SCR_SUCCESS;
}

/* remove all files recorded in filemap and the filemap itself */
static int scr_cache_purge(scr_filemap* map)
{
  /* TODO: put dataset selection logic into a function */

  /* get the list of datasets we have in our cache */
  int ndsets;
  int* dsets;
  scr_filemap_list_datasets(map, &ndsets, &dsets);

  /* TODO: also attempt to recover datasets which we were in the middle of flushing */
  int current_id;
  int dset_index = 0;
  do {
    /* get the smallest index across all processes (returned in current_id),
     * this also updates our dset_index value if appropriate */
    scr_next_dataset(ndsets, dsets, &dset_index, &current_id);
    
    /* if we found a dataset, delete it */
    if (current_id != -1) {
      /* remove this dataset from all tasks */
      scr_cache_delete(map, current_id);
    }
  } while (current_id != -1);

  /* now delete the filemap itself */
  unlink(scr_map_file);

  /* TODO: want to clear the map object here? */

  /* TODO: want to delete the master map file? */

  /* free our list of dataset ids */
  if (dsets != NULL) {
    free(dsets);
    dsets = NULL;
  }

  return 1;
}

/* checks whether specifed file exists, is readable, and is complete */
static int scr_bool_have_file(scr_filemap* map, int dset, int rank, const char* file, int ranks)
{
  /* if no filename is given return false */
  if (file == NULL || strcmp(file,"") == 0) {
    scr_dbg(2, "File name is null or the empty string @ %s:%d",
            __FILE__, __LINE__
    );
    return 0;
  }

  /* check that we can read the file */
  if (access(file, R_OK) < 0) {
    scr_dbg(2, "Do not have read access to file: %s @ %s:%d",
            file, __FILE__, __LINE__
    );
    return 0;
  }

  /* allocate object to read meta data into */
  scr_meta* meta = scr_meta_new();

  /* check that we can read meta file for the file */
  if (scr_filemap_get_meta(map, dset, rank, file, meta) != SCR_SUCCESS) {
    scr_dbg(2, "Failed to read meta data for file: %s @ %s:%d",
            file, __FILE__, __LINE__
    );
    scr_meta_delete(meta);
    return 0;
  }

  /* check that the file is complete */
  if (scr_meta_is_complete(meta) != SCR_SUCCESS) {
    scr_dbg(2, "File is marked as incomplete: %s @ %s:%d",
            file, __FILE__, __LINE__
    );
    scr_meta_delete(meta);
    return 0;
  }

  /* TODODSET: enable check for correct dataset / checkpoint id */

#if 0
  /* check that the file really belongs to the checkpoint id we think it does */
  int meta_dset = -1;
  if (scr_meta_get_dataset(meta, &meta_dset) != SCR_SUCCESS) {
    scr_dbg(2, "Failed to read dataset field in meta data: %s @ %s:%d",
            file, __FILE__, __LINE__
    );
    scr_meta_delete(meta);
    return 0;
  }
  if (dset != meta_dset) {
    scr_dbg(2, "File's dataset ID (%d) does not match id in meta data file (%d) for %s @ %s:%d",
            dset, meta_dset, file, __FILE__, __LINE__
    );
    scr_meta_delete(meta);
    return 0;
  }
#endif

#if 0
  /* check that the file really belongs to the rank we think it does */
  int meta_rank = -1;
  if (scr_meta_get_rank(meta, &meta_rank) != SCR_SUCCESS) {
    scr_dbg(2, "Failed to read rank field in meta data: %s @ %s:%d",
            file, __FILE__, __LINE__
    );
    scr_meta_delete(meta);
    return 0;
  }
  if (rank != meta_rank) {
    scr_dbg(2, "File's rank (%d) does not match rank in meta data (%d) for %s @ %s:%d",
            rank, meta_rank, file, __FILE__, __LINE__
    );
    scr_meta_delete(meta);
    return 0;
  }
#endif

#if 0
  /* check that the file really belongs to the rank we think it does */
  int meta_ranks = -1;
  if (scr_meta_get_ranks(meta, &meta_ranks) != SCR_SUCCESS) {
    scr_dbg(2, "Failed to read ranks field in meta data: %s @ %s:%d",
            file, __FILE__, __LINE__
    );
    scr_meta_delete(meta);
    return 0;
  }
  if (ranks != meta_ranks) {
    scr_dbg(2, "File's ranks (%d) does not match ranks in meta data file (%d) for %s @ %s:%d",
            ranks, meta_ranks, file, __FILE__, __LINE__
    );
    scr_meta_delete(meta);
    return 0;
  }
#endif

  /* check that the file size matches */
  unsigned long size = scr_filesize(file);
  unsigned long meta_size = 0;
  if (scr_meta_get_filesize(meta, &meta_size) != SCR_SUCCESS) {
    scr_dbg(2, "Failed to read filesize field in meta data: %s @ %s:%d",
            file, __FILE__, __LINE__
    );
    scr_meta_delete(meta);
    return 0;
  }
  if (size != meta_size) {
    scr_dbg(2, "Filesize is incorrect, currently %lu, expected %lu for %s @ %s:%d",
            size, meta_size, file, __FILE__, __LINE__
    );
    scr_meta_delete(meta);
    return 0;
  }

  /* TODO: check that crc32 match if set (this would be expensive) */

  /* free meta data object */
  scr_meta_delete(meta);

  /* if we made it here, assume the file is good */
  return 1;
}

/* check whether we have all files for a given rank of a given dataset */
static int scr_bool_have_files(scr_filemap* map, int id, int rank)
{
  /* check whether we have any files for the specified rank */
  if (! scr_filemap_have_rank_by_dataset(map, id, rank)) {
    return 0;
  }

  /* check whether we have all of the files we should */
  int expected_files = scr_filemap_get_expected_files(map, id, rank);
  int num_files = scr_filemap_num_files(map, id, rank);
  if (num_files != expected_files) {
    return 0;
  }

  /* check the integrity of each of the files */
  int missing_a_file = 0;
  scr_hash_elem* file_elem;
  for (file_elem = scr_filemap_first_file(map, id, rank);
       file_elem != NULL;
       file_elem = scr_hash_elem_next(file_elem))
  {
    char* file = scr_hash_elem_key(file_elem);
    if (! scr_bool_have_file(map, id, rank, file, scr_ranks_world)) {
      missing_a_file = 1;
    }
  }
  if (missing_a_file) {
    return 0;
  }

  /* if we make it here, we have all of our files */
  return 1;
}

/* opens the filemap, inspects that all listed files are readable and complete,
 * unlinks any that are not */
static int scr_cache_clean(scr_filemap* map)
{
  /* create a map to remember which files to keep */
  scr_filemap* keep_map = scr_filemap_new();

  /* scan each file for each rank of each checkpoint */
  scr_hash_elem* dset_elem;
  for (dset_elem = scr_filemap_first_dataset(map);
       dset_elem != NULL;
       dset_elem = scr_hash_elem_next(dset_elem))
  {
    /* get the dataset id */
    int dset = scr_hash_elem_key_int(dset_elem);

    scr_hash_elem* rank_elem;
    for (rank_elem = scr_filemap_first_rank_by_dataset(map, dset);
         rank_elem != NULL;
         rank_elem = scr_hash_elem_next(rank_elem))
    {
      /* get the rank id */
      int rank = scr_hash_elem_key_int(rank_elem);

      /* if we're missing any file for this rank in this checkpoint,
       * we'll delete them all */
      int missing_file = 0;

      /* first time through the file list, check that we have each file */
      scr_hash_elem* file_elem = NULL;
      for (file_elem = scr_filemap_first_file(map, dset, rank);
           file_elem != NULL;
           file_elem = scr_hash_elem_next(file_elem))
      {
        /* get filename */
        char* file = scr_hash_elem_key(file_elem);

        /* check whether we have it */
        if (! scr_bool_have_file(map, dset, rank, file, scr_ranks_world)) {
            missing_file = 1;
            scr_dbg(1, "File is unreadable or incomplete: Dataset %d, Rank %d, File: %s",
                    dset, rank, file
            );
        }
      }

      /* add redundancy descriptor to keep map, if one is set */
      scr_hash* desc = scr_hash_new();
      if (scr_filemap_get_desc(map, dset, rank, desc) == SCR_SUCCESS) {
        scr_filemap_set_desc(keep_map, dset, rank, desc);
      }
      scr_hash_delete(desc);

      /* add dataset descriptor to keep map, if one is set */
      scr_hash* dataset = scr_hash_new();
      if (scr_filemap_get_dataset(map, dset, rank, dataset) == SCR_SUCCESS) {
        scr_filemap_set_dataset(keep_map, dset, rank, dataset);
      }
      scr_hash_delete(dataset);

      /* check whether we have all the files we think we should */
      int expected_files = scr_filemap_get_expected_files(map, dset, rank);
      int num_files = scr_filemap_num_files(map, dset, rank);
      if (num_files != expected_files) {
        missing_file = 1;
      }

      /* if we have all the files, set the expected file number in the keep_map */
      if (! missing_file) {
        scr_filemap_set_expected_files(keep_map, dset, rank, expected_files);
      }

      /* second time through, either add all files to keep_map or delete them all */
      for (file_elem = scr_filemap_first_file(map, dset, rank);
           file_elem != NULL;
           file_elem = scr_hash_elem_next(file_elem))
      {
        /* get the filename */
        char* file = scr_hash_elem_key(file_elem);

        /* if we failed to read any file, delete them all 
         * otherwise add them all to the keep_map */
        if (missing_file) {
          /* inform user on what we're doing */
          scr_dbg(1, "Deleting file: Dataset %d, Rank %d, File: %s",
                  dset, rank, file
          );

          /* delete the file */
          unlink(file);
        } else {
          /* keep this file */
          scr_filemap_add_file(keep_map, dset, rank, file);

          /* copy the meta data */
          scr_meta* meta = scr_meta_new();
          if (scr_filemap_get_meta(map, dset, rank, file, meta) == SCR_SUCCESS) {
            scr_filemap_set_meta(keep_map, dset, rank, file, meta);
          }
          scr_meta_delete(meta);
        }
      }
    }
  }

  /* clear our current map, merge the keep_map into it, and write the map to disk */
  scr_filemap_clear(map);
  scr_filemap_merge(map, keep_map);
  scr_filemap_write(scr_map_file, map);

  /* free the keep_map object */
  scr_filemap_delete(keep_map);

  return SCR_SUCCESS;
}

/* returns true iff each file in the filemap can be read */
static int scr_cache_check_files(scr_filemap* map, int id)
{
  /* for each file of each rank for specified dataset id */
  int failed_read = 0;
  scr_hash_elem* rank_elem;
  for (rank_elem = scr_filemap_first_rank_by_dataset(map, id);
       rank_elem != NULL;
       rank_elem = scr_hash_elem_next(rank_elem))
  {
    /* get the rank id */
    int rank = scr_hash_elem_key_int(rank_elem);

    scr_hash_elem* file_elem;
    for (file_elem = scr_filemap_first_file(map, id, rank);
         file_elem != NULL;
         file_elem = scr_hash_elem_next(file_elem))
    {
      /* get the filename */
      char* file = scr_hash_elem_key(file_elem);

      /* check that we can read the file */
      if (access(file, R_OK) < 0) {
        failed_read = 1;
      }

      /* get meta data for this file */
      scr_meta* meta = scr_meta_new();
      if (scr_filemap_get_meta(map, id, rank, file, meta) != SCR_SUCCESS) {
        failed_read = 1;
      } else {
        /* check that the file is complete */
        if (scr_meta_is_complete(meta) != SCR_SUCCESS) {
          failed_read = 1;
        }
      }
      scr_meta_delete(meta);
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

static int scr_swap_file_names(const char* file_send, int rank_send,
                                     char* file_recv, size_t size_recv, int rank_recv,
                               const char* dir_recv, MPI_Comm comm)
{
  int rc = SCR_SUCCESS;

  /* determine whether we have a file to send */
  int have_outgoing = 0;
  if (rank_send != MPI_PROC_NULL &&
      file_send != NULL &&
      strcmp(file_send, "") != 0)
  {
    have_outgoing = 1;
  }

  /* determine whether we are expecting to receive a file */
  int have_incoming = 0;
  if (rank_recv != MPI_PROC_NULL &&
      dir_recv != NULL &&
      strcmp(dir_recv, "") != 0)
  {
    have_incoming = 1;
  }

  /* exchange file names with partners */
  char file_recv_orig[SCR_MAX_FILENAME] = "";
  int num_req = 0;;
  MPI_Request request[2];
  MPI_Status  status[2];
  if (have_incoming) {
    MPI_Irecv(file_recv_orig, SCR_MAX_FILENAME, MPI_CHAR, rank_recv, 0, comm, &request[num_req]);
    num_req++;
  }
  if (have_outgoing) {
    MPI_Isend((char*)file_send, strlen(file_send)+1, MPI_CHAR, rank_send, 0, comm, &request[num_req]);
    num_req++;
  }
  if (num_req > 0) {
    MPI_Waitall(num_req, request, status);
  }

  /* define the path to store our partner's file */
  if (have_incoming) {
    /* set full path to filename */
    char path[SCR_MAX_FILENAME] = "";
    char name[SCR_MAX_FILENAME] = "";
    scr_split_path(file_recv_orig, path, name);
    scr_build_path(file_recv, size_recv, dir_recv, name);
  }

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
static int scr_swap_files(
  int swap_type,
  const char* file_send, scr_meta* meta_send, int rank_send,
  const char* file_recv, scr_meta* meta_recv, int rank_recv,
  MPI_Comm comm)
{
  int rc = SCR_SUCCESS;
  MPI_Request request[2];
  MPI_Status  status[2];

  /* determine whether we have a file to send */
  int have_outgoing = 0;
  if (rank_send != MPI_PROC_NULL &&
      file_send != NULL &&
      strcmp(file_send, "") != 0)
  {
    have_outgoing = 1;
  }

  /* determine whether we are expecting to receive a file */
  int have_incoming = 0;
  if (rank_recv != MPI_PROC_NULL &&
      file_recv != NULL &&
      strcmp(file_recv, "") != 0)
  {
    have_incoming = 1;
  }

  /* exchange meta file info with partners */
  scr_hash_sendrecv(meta_send, rank_send, meta_recv, rank_recv, comm);

  /* allocate MPI send buffer */
  char *buf_send = NULL;
  if (have_outgoing) {
    buf_send = (char*) scr_align_malloc(scr_mpi_buf_size, scr_page_size);
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
    buf_recv = (char*) scr_align_malloc(scr_mpi_buf_size, scr_page_size);
    if (buf_recv == NULL) {
      scr_err("Allocating memory: malloc(%ld) errno=%d %m @ %s:%d",
              scr_mpi_buf_size, errno, __FILE__, __LINE__
      );
      if (buf_send != NULL) {
        scr_align_free(buf_send);
        buf_send = NULL;
      }
      return SCR_FAILURE;
    }
  }

  /* initialize crc values */
  uLong crc32_send = crc32(0L, Z_NULL, 0);
  uLong crc32_recv = crc32(0L, Z_NULL, 0);

  /* exchange files */
  if (swap_type == COPY_FILES) {
    /* open the file to send: read-only mode */
    int fd_send = -1;
    if (have_outgoing) {
      fd_send = scr_open(file_send, O_RDONLY);
      if (fd_send < 0) {
        scr_abort(-1, "Opening file for send: scr_open(%s, O_RDONLY) errno=%d %m @ %s:%d",
                file_send, errno, __FILE__, __LINE__
        );
      }
    }

    /* open the file to recv: truncate, write-only mode */
    int fd_recv = -1;
    if (have_incoming) {
      fd_recv = scr_open(file_recv, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
      if (fd_recv < 0) {
        scr_abort(-1, "Opening file for recv: scr_open(%s, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR) errno=%d %m @ %s:%d",
                file_recv, errno, __FILE__, __LINE__
        );
      }
    }

    /* exchange file chunks */
    int nread, nwrite;
    int sending = 0;
    if (have_outgoing) {
      sending = 1;
    }
    int receiving = 0;
    if (have_incoming) {
      receiving = 1;
    }
    while (sending || receiving) {
      /* if we are still receiving a file, post a receive */
      if (receiving) {
        MPI_Irecv(buf_recv, scr_mpi_buf_size, MPI_BYTE, rank_recv, 0, comm, &request[0]);
      }

      /* if we are still sending a file, read a chunk, send it, and wait */
      if (sending) {
        nread = scr_read(file_send, fd_send, buf_send, scr_mpi_buf_size);
        if (scr_crc_on_copy && nread > 0) {
          crc32_send = crc32(crc32_send, (const Bytef*) buf_send, (uInt) nread);
        }
        if (nread < 0) {
          nread = 0;
        }
        MPI_Isend(buf_send, nread, MPI_BYTE, rank_send, 0, comm, &request[1]);
        MPI_Wait(&request[1], &status[1]);
        if (nread < scr_mpi_buf_size) {
          sending = 0;
        }
      }

      /* if we are still receiving a file,
       * wait on our receive to complete and write the data */
      if (receiving) {
        MPI_Wait(&request[0], &status[0]);
        MPI_Get_count(&status[0], MPI_BYTE, &nwrite);
        if (scr_crc_on_copy && nwrite > 0) {
          crc32_recv = crc32(crc32_recv, (const Bytef*) buf_recv, (uInt) nwrite);
        }
        scr_write(file_recv, fd_recv, buf_recv, nwrite);
        if (nwrite < scr_mpi_buf_size) {
          receiving = 0;
        }
      }
    }

    /* close the files */
    if (have_outgoing) {
      scr_close(file_send, fd_send);
    }
    if (have_incoming) {
      scr_close(file_recv, fd_recv);
    }

    /* set crc field on our file if it hasn't been set already */
    if (scr_crc_on_copy && have_outgoing) {
      uLong meta_send_crc;
      if (scr_meta_get_crc32(meta_send, &meta_send_crc) != SCR_SUCCESS) {
        scr_meta_set_crc32(meta_send, crc32_send);
      } else {
        /* TODO: we could check that the crc on the sent file matches and take some action if not */
      }
    }
  } else if (swap_type == MOVE_FILES) {
    /* since we'll overwrite our send file in place with the recv file,
     * which may be larger, we need to keep track of how many bytes we've
     * sent and whether we've sent them all */
    unsigned long filesize_send = 0;

    /* open our file */
    int fd = -1;
    if (have_outgoing) {
      /* we'll overwrite our send file (or just read it if there is no incoming) */
      filesize_send = scr_filesize(file_send);
      fd = scr_open(file_send, O_RDWR);
      if (fd < 0) {
        /* TODO: skip writes and return error? */
        scr_abort(-1, "Opening file for send/recv: scr_open(%s, O_RDWR) errno=%d %m @ %s:%d",
                file_send, errno, __FILE__, __LINE__
        );
      }
    } else if (have_incoming) {
      /* if we're in this branch, then we only have an incoming file,
       * so we'll write our recv file from scratch */
      fd = scr_open(file_recv, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
      if (fd < 0) {
        /* TODO: skip writes and return error? */
        scr_abort(-1, "Opening file for recv: scr_open(%s, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR) errno=%d %m @ %s:%d",
                file_recv, errno, __FILE__, __LINE__
        );
      }
    }

    /* exchange file chunks */
    int sending = 0;
    if (have_outgoing) {
      sending = 1;
    }
    int receiving = 0;
    if (have_incoming) {
      receiving = 1;
    }
    int nread, nwrite;
    off_t read_pos = 0, write_pos = 0;
    while (sending || receiving) {
      if (receiving) {
        /* prepare a buffer to receive up to scr_mpi_buf_size bytes */
        MPI_Irecv(buf_recv, scr_mpi_buf_size, MPI_BYTE, rank_recv, 0, comm, &request[0]);
      }

      if (sending) {
        /* compute number of bytes to read */
        unsigned long count = filesize_send - read_pos;
        if (count > scr_mpi_buf_size) {
          count = scr_mpi_buf_size;
        }

        /* read a chunk of up to scr_mpi_buf_size bytes into buf_send */
        lseek(fd, read_pos, SEEK_SET); /* seek to read position */
        nread = scr_read(file_send, fd, buf_send, count);
        if (scr_crc_on_copy && nread > 0) {
          crc32_send = crc32(crc32_send, (const Bytef*) buf_send, (uInt) nread);
        }
        if (nread < 0) {
          nread = 0;
        }
        read_pos += (off_t) nread; /* update read pointer */

        /* send chunk (if nread is smaller than scr_mpi_buf_size,
         * then we've read the whole file) */
        MPI_Isend(buf_send, nread, MPI_BYTE, rank_send, 0, comm, &request[1]);
        MPI_Wait(&request[1], &status[1]);

        /* check whether we've read the whole file */
        if (filesize_send == read_pos && count < scr_mpi_buf_size) {
          sending = 0;
        }
      }

      if (receiving) {
        /* count the number of bytes received */
        MPI_Wait(&request[0], &status[0]);
        MPI_Get_count(&status[0], MPI_BYTE, &nwrite);
        if (scr_crc_on_copy && nwrite > 0) {
          crc32_recv = crc32(crc32_recv, (const Bytef*) buf_recv, (uInt) nwrite);
        }

        /* write those bytes to file (if nwrite is smaller than scr_mpi_buf_size,
         * then we've received the whole file) */
        lseek(fd, write_pos, SEEK_SET); /* seek to write position */
        scr_write(file_recv, fd, buf_recv, nwrite);
        write_pos += (off_t) nwrite; /* update write pointer */

        /* if nwrite is smaller than scr_mpi_buf_size,
         * then assume we've received the whole file */
        if (nwrite < scr_mpi_buf_size) {
          receiving = 0;
        }
      }
    }

    /* close file and cleanup */
    if (have_outgoing && have_incoming) {
      /* sent and received a file; close it, truncate it to corect size, rename it */
      scr_close(file_send, fd);
      truncate(file_send, write_pos);
      rename(file_send, file_recv);
    } else if (have_outgoing) {
      /* only sent a file; close it, delete it, and remove its completion marker */
      scr_close(file_send, fd);
      unlink(file_send);
    } else if (have_incoming) {
      /* only received a file; just need to close it */
      scr_close(file_recv, fd);
    }

    if (scr_crc_on_copy && have_outgoing) {
      uLong meta_send_crc;
      if (scr_meta_get_crc32(meta_send, &meta_send_crc) != SCR_SUCCESS) {
        /* we transfer this meta data across below,
         * so may as well update these fields so we can use them */
        scr_meta_set_crc32(meta_send, crc32_send);
        /* do not complete file send, we just deleted it above */
      } else {
        /* TODO: we could check that the crc on the sent file matches and take some action if not */
      }
    }
  } else {
    scr_err("Unknown file transfer type: %d @ %s:%d", swap_type, __FILE__, __LINE__);
    return SCR_FAILURE;
  } /* end file copy / move */

  /* free the MPI buffers */
  if (have_outgoing) {
    scr_align_free(buf_send);
    buf_send = NULL;
  }
  if (have_incoming) {
    scr_align_free(buf_recv);
    buf_recv = NULL;
  }

  /* mark received file as complete */
  if (have_incoming) {
    /* check that our written file is the correct size */
    unsigned long filesize_wrote = scr_filesize(file_recv);
    if (scr_meta_check_filesize(meta_recv, filesize_wrote) != SCR_SUCCESS) {
      scr_err("Received file does not match expected size %s @ %s:%d",
              file_recv, __FILE__, __LINE__
      );
      scr_meta_set_complete(meta_recv, 0);
      rc = SCR_FAILURE;
    }

    /* check that there was no corruption in receiving the file */
    if (scr_crc_on_copy) {
      /* if we computed crc during the copy, and crc is set in the received meta data
       * check that our computed value matches */
      uLong crc32_recv_meta;
      if (scr_meta_get_crc32(meta_recv, &crc32_recv_meta) == SCR_SUCCESS) {
        if (crc32_recv != crc32_recv_meta) {
          scr_err("CRC32 mismatch detected when receiving file %s @ %s:%d",
                  file_recv, __FILE__, __LINE__
          );
          scr_meta_set_complete(meta_recv, 0);
          rc = SCR_FAILURE;
        }
      }
    }
  }

  return rc;
}

/* copy files to a partner node */
static int scr_copy_partner(scr_filemap* map, const struct scr_reddesc* c, int id)
{
  int rc = SCR_SUCCESS;

  /* get a list of our files */
  int numfiles = 0;
  char** files = NULL;
  scr_filemap_list_files(map, id, scr_my_rank_world, &numfiles, &files);

  /* first, determine how many files we'll be sending and receiving with our partners */
  MPI_Status status;
  int send_num = numfiles;
  int recv_num = 0;
  MPI_Sendrecv(&send_num, 1, MPI_INT, c->rhs_rank, 0, &recv_num, 1, MPI_INT, c->lhs_rank, 0, c->comm, &status);

  /* record how many files our partner will send */
  scr_filemap_set_expected_files(map, id, c->lhs_rank_world, recv_num);

  /* remember which node our partner is on (needed for drain) */
  scr_filemap_set_tag(map, id, c->lhs_rank_world, SCR_FILEMAP_KEY_PARTNER, c->lhs_hostname);

  /* record partner's redundancy descriptor hash */
  scr_hash* lhs_desc_hash = scr_hash_new();
  scr_hash* my_desc_hash  = scr_hash_new();
  scr_reddesc_store_to_hash(c, my_desc_hash);
  scr_hash_sendrecv(my_desc_hash, c->rhs_rank, lhs_desc_hash, c->lhs_rank, c->comm);
  scr_filemap_set_desc(map, id, c->lhs_rank_world, lhs_desc_hash);
  scr_hash_delete(my_desc_hash);
  scr_hash_delete(lhs_desc_hash);

  /* store this info in our filemap before we receive any files */
  scr_filemap_write(scr_map_file, map);

  /* define directory to receive partner file in */
  char dir[SCR_MAX_FILENAME];
  scr_cache_dir_get(c, id, dir);

  /* for each potential file, step through a call to swap */
  while (send_num > 0 || recv_num > 0) {
    /* assume we won't send or receive in this step */
    int send_rank = MPI_PROC_NULL;
    int recv_rank = MPI_PROC_NULL;

    /* if we have a file left to send, get the filename and destination rank */
    char* file = NULL;
    if (send_num > 0) {
      int i = numfiles - send_num;
      file = files[i];
      send_rank = c->rhs_rank;
      send_num--;
    }

    /* if we have a file left to receive, get the rank */
    if (recv_num > 0) {
      recv_rank = c->lhs_rank;
      recv_num--;
    }

    /* exhange file names with partners */
    char file_partner[SCR_MAX_FILENAME];
    scr_swap_file_names(file, send_rank, file_partner, sizeof(file_partner), recv_rank, dir, c->comm);

    /* if we'll receive a file, record the name of our partner's file in the filemap */
    if (recv_rank != MPI_PROC_NULL) {
      scr_filemap_add_file(map, id, c->lhs_rank_world, file_partner);
      scr_filemap_write(scr_map_file, map);
    }

    /* get meta data of file we're sending */
    scr_meta* send_meta = scr_meta_new();
    scr_filemap_get_meta(map, id, scr_my_rank_world, file, send_meta);

    /* exhange files with partners */
    scr_meta* recv_meta = scr_meta_new();
    if (scr_swap_files(COPY_FILES, file, send_meta, send_rank, file_partner, recv_meta, recv_rank, c->comm) != SCR_SUCCESS) {
      rc = SCR_FAILURE;
    }
    scr_filemap_set_meta(map, id, c->lhs_rank_world, file_partner, recv_meta);

    /* free meta data for these files */
    scr_meta_delete(recv_meta);
    scr_meta_delete(send_meta);
  }

  /* write out the updated filemap */
  scr_filemap_write(scr_map_file, map);

  /* free our list of files */
  if (files != NULL) {
    free(files);
    files = NULL;
  }

  return rc;
}

/* set the ranks array in the header */
static int scr_copy_xor_header_set_ranks(scr_hash* header, MPI_Comm comm, MPI_Comm comm_world)
{
  scr_hash_unset(header, SCR_KEY_COPY_XOR_RANKS);
  scr_hash_unset(header, SCR_KEY_COPY_XOR_GROUP);

  /* record the total number of ranks in comm_world */
  int ranks_world;
  MPI_Comm_size(comm_world, &ranks_world);
  scr_hash_set_kv_int(header, SCR_KEY_COPY_XOR_RANKS, ranks_world);

  /* create a new empty hash to track group info for this xor set */
  scr_hash* hash = scr_hash_new();
  scr_hash_set(header, SCR_KEY_COPY_XOR_GROUP, hash);

  /* record the total number of ranks in the xor communicator */
  int ranks_comm;
  MPI_Comm_size(comm, &ranks_comm);
  scr_hash_set_kv_int(hash, SCR_KEY_COPY_XOR_GROUP_RANKS, ranks_comm);

  /* record mapping of rank in xor group to corresponding world rank */
  int i, rank;
  if (ranks_comm > 0) {
    /* map ranks in comm to ranks in scr_comm_world */
    MPI_Group group, group_world;
    MPI_Comm_group(comm, &group);
    MPI_Comm_group(comm_world, &group_world);
    for (i=0; i < ranks_comm; i++) {
      MPI_Group_translate_ranks(group, 1, &i, group_world, &rank);
      scr_hash_setf(hash, NULL, "%s %d %d", SCR_KEY_COPY_XOR_GROUP_RANK, i, rank);
    }
    MPI_Group_free(&group);
    MPI_Group_free(&group_world);
  }

  return SCR_SUCCESS;
}

/* apply XOR redundancy scheme to dataset files */
static int scr_copy_xor(scr_filemap* map, const struct scr_reddesc* c, int id)
{
  int rc = SCR_SUCCESS;
  int tmp_rc;
  int i;

  /* allocate buffer to read a piece of my file */
  char* send_buf = (char*) scr_align_malloc(scr_mpi_buf_size, scr_page_size);
  if (send_buf == NULL) {
    scr_abort(-1, "Allocating memory for send buffer: malloc(%d) errno=%d %m @ %s:%d",
            scr_mpi_buf_size, errno, __FILE__, __LINE__
    );
  }

  /* allocate buffer to read a piece of the recevied chunk file */
  char* recv_buf = (char*) scr_align_malloc(scr_mpi_buf_size, scr_page_size);
  if (recv_buf == NULL) {
    scr_abort(-1, "Allocating memory for recv buffer: malloc(%d) errno=%d %m @ %s:%d",
            scr_mpi_buf_size, errno, __FILE__, __LINE__
    );
  }

  /* count the number of files I have and allocate space in structures for each of them */
  int num_files = scr_filemap_num_files(map, id, scr_my_rank_world);
  int* fds = NULL;
  char** filenames = NULL;
  unsigned long* filesizes = NULL;
  if (num_files > 0) {
    fds       = (int*)           malloc(num_files * sizeof(int));
    filenames = (char**)         malloc(num_files * sizeof(char*));
    filesizes = (unsigned long*) malloc(num_files * sizeof(unsigned long));
    if (fds == NULL || filenames == NULL || filesizes == NULL) {
      scr_abort(-1, "Failed to allocate file arrays @ %s:%d",
                __FILE__, __LINE__
      );
    }
  }

  /* record partner's redundancy descriptor hash in our filemap */
  scr_hash* lhs_desc_hash = scr_hash_new();
  scr_hash* my_desc_hash  = scr_hash_new();
  scr_reddesc_store_to_hash(c, my_desc_hash);
  scr_hash_sendrecv(my_desc_hash, c->rhs_rank, lhs_desc_hash, c->lhs_rank, c->comm);
  scr_filemap_set_desc(map, id, c->lhs_rank_world, lhs_desc_hash);
  scr_hash_delete(my_desc_hash);
  scr_hash_delete(lhs_desc_hash);

  /* allocate a new xor file header hash, record the global ranks of the
   * processes in our xor group, and record the dataset id */
  scr_hash* header = scr_hash_new();
  scr_copy_xor_header_set_ranks(header, c->comm, scr_comm_world);

  /* record dataset in header */
  scr_hash* dataset = scr_hash_new();
  scr_filemap_get_dataset(map, id, scr_my_rank_world, dataset);
  scr_hash_set(header, SCR_KEY_COPY_XOR_DATASET, dataset);

  /* open each file, get the filesize of each, and read the meta data of each */
  scr_hash* current_files = scr_hash_new();
  int file_count = 0;
  unsigned long my_bytes = 0;
  scr_hash_elem* file_elem;
  for (file_elem = scr_filemap_first_file(map, id, scr_my_rank_world);
       file_elem != NULL;
       file_elem = scr_hash_elem_next(file_elem))
  {
    /* get the filename */
    filenames[file_count] = scr_hash_elem_key(file_elem);

    /* get the filesize of this file and add the byte count to the total */
    filesizes[file_count] = scr_filesize(filenames[file_count]);
    my_bytes += filesizes[file_count];

    /* read the meta data for this file and insert it into the current_files hash */
    scr_meta* file_hash = scr_meta_new();
    scr_filemap_get_meta(map, id, scr_my_rank_world, filenames[file_count], file_hash);
    scr_hash_setf(current_files, file_hash, "%d", file_count);

    /* open the file */
    fds[file_count]  = scr_open(filenames[file_count], O_RDONLY);
    if (fds[file_count] < 0) {
      /* TODO: try again? */
      scr_abort(-1, "Opening checkpoint file for copying: scr_open(%s, O_RDONLY) errno=%d %m @ %s:%d",
                filenames[file_count], errno, __FILE__, __LINE__
      );
    }

    file_count++;
  }

  /* set total number of files we have, plus our rank */
  scr_hash* current_hash = scr_hash_new();
  scr_hash_set_kv_int(current_hash, SCR_KEY_COPY_XOR_RANK,  scr_my_rank_world);
  scr_hash_set_kv_int(current_hash, SCR_KEY_COPY_XOR_FILES, file_count);
  scr_hash_set(current_hash, SCR_KEY_COPY_XOR_FILE, current_files);

  /* exchange file info with partners and add data to our header */
  scr_hash* partner_hash = scr_hash_new();
  scr_hash_sendrecv(current_hash, c->rhs_rank, partner_hash, c->lhs_rank, c->comm);
  scr_hash_set(header, SCR_KEY_COPY_XOR_CURRENT, current_hash);
  scr_hash_set(header, SCR_KEY_COPY_XOR_PARTNER, partner_hash);

  /* allreduce to get maximum filesize */
  unsigned long max_bytes;
  MPI_Allreduce(&my_bytes, &max_bytes, 1, MPI_UNSIGNED_LONG, MPI_MAX, c->comm);

  /* TODO: use unsigned long integer arithmetic (with proper byte padding) instead of char to speed things up */

  /* compute chunk size according to maximum file length and number of ranks in xor set */
  /* if filesize doesn't divide evenly, then add one byte to chunk_size */
  /* TODO: check that ranks > 1 for this divide to be safe (or at partner selection time) */
  size_t chunk_size = max_bytes / (unsigned long) (c->ranks - 1);
  if ((c->ranks - 1) * chunk_size < max_bytes) {
    chunk_size++;
  }

  /* TODO: need something like this to handle 0-byte files? */
  if (chunk_size == 0) {
    chunk_size++;
  }

  /* record the dataset id and the chunk size in the xor chunk header */
  scr_hash_util_set_bytecount(header, SCR_KEY_COPY_XOR_CHUNK, chunk_size);

  /* set chunk filenames of form:  <xor_rank+1>_of_<xor_ranks>_in_<group_id>.xor */
  char my_chunk_file[SCR_MAX_FILENAME];
  char dir[SCR_MAX_FILENAME];
  scr_cache_dir_get(c, id, dir);
  sprintf(my_chunk_file,  "%s/%d_of_%d_in_%d.xor", dir, c->my_rank+1, c->ranks, c->group_id);

  /* record chunk file in filemap before creating it */
  scr_filemap_add_file(map, id, scr_my_rank_world, my_chunk_file);
  scr_filemap_write(scr_map_file, map);

  /* open my chunk file */
  int fd_chunk = scr_open(my_chunk_file, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
  if (fd_chunk < 0) {
    /* TODO: try again? */
    scr_abort(-1, "Opening XOR chunk file for writing: scr_open(%s) errno=%d %m @ %s:%d",
            my_chunk_file, errno, __FILE__, __LINE__
    );
  }

  /* write out the xor chunk header */
  scr_hash_write_fd(my_chunk_file, fd_chunk, header);
  scr_hash_delete(header);

  MPI_Request request[2];
  MPI_Status  status[2];

  /* XOR Reduce_scatter */
  size_t nread = 0;
  while (nread < chunk_size) {
    size_t count = chunk_size - nread;
    if (count > scr_mpi_buf_size) {
      count = scr_mpi_buf_size;
    }

    int chunk_id;
    for(chunk_id = c->ranks-1; chunk_id >= 0; chunk_id--) {
      /* read the next set of bytes for this chunk from my file into send_buf */
      if (chunk_id > 0) {
        int chunk_id_rel = (c->my_rank + c->ranks + chunk_id) % c->ranks;
        if (chunk_id_rel > c->my_rank) {
          chunk_id_rel--;
        }
        unsigned long offset = chunk_size * (unsigned long) chunk_id_rel + nread;
        if (scr_read_pad_n(num_files, filenames, fds,
                           send_buf, count, offset, filesizes) != SCR_SUCCESS)
        {
          rc = SCR_FAILURE;
        }
      } else {
        memset(send_buf, 0, count);
      }

      /* TODO: XORing with unsigned long would be faster here (if chunk size is multiple of this size) */
      /* merge the blocks via xor operation */
      if (chunk_id < c->ranks-1) {
        for (i = 0; i < count; i++) {
          send_buf[i] ^= recv_buf[i];
        }
      }

      if (chunk_id > 0) {
        /* not our chunk to write, forward it on and get the next */
        MPI_Irecv(recv_buf, count, MPI_BYTE, c->lhs_rank, 0, c->comm, &request[0]);
        MPI_Isend(send_buf, count, MPI_BYTE, c->rhs_rank, 0, c->comm, &request[1]);
        MPI_Waitall(2, request, status);
      } else {
        /* write send block to send chunk file */
        if (scr_write_attempt(my_chunk_file, fd_chunk, send_buf, count) != count) {
          rc = SCR_FAILURE;
        }
      }
    }

    nread += count;
  }

  /* close my chunkfile, with fsync */
  if (scr_close(my_chunk_file, fd_chunk) != SCR_SUCCESS) {
    rc = SCR_FAILURE;
  }

  /* close my checkpoint files */
  for (i=0; i < num_files; i++) {
    scr_close(filenames[i], fds[i]);
  }

  /* free the buffers */
  if (filesizes != NULL) {
    free(filesizes);
    filesizes = NULL;
  }
  if (filenames != NULL) {
    /* in this case, we don't free each name, since we copied the pointer to the string in the filemap */
    free(filenames);
    filenames = NULL;
  }
  if (fds != NULL) {
    free(fds);
    fds = NULL;
  }
  scr_align_free(send_buf);
  scr_align_free(recv_buf);

  /* TODO: need to check for errors */
  /* write meta file for xor chunk */
  unsigned long my_chunk_file_size = scr_filesize(my_chunk_file);
  scr_meta* meta = scr_meta_new();
  scr_meta_set_filename(meta, my_chunk_file);
  scr_meta_set_filetype(meta, SCR_META_FILE_XOR);
  scr_meta_set_filesize(meta, my_chunk_file_size);
  scr_meta_set_complete(meta, 1);
  /* TODODSET: move the ranks field elsewhere, for now it's needed by scr_index.c */
  scr_meta_set_ranks(meta, scr_ranks_world);
  scr_filemap_set_meta(map, id, scr_my_rank_world, my_chunk_file, meta);
  scr_filemap_write(scr_map_file, map);
  scr_meta_delete(meta);

  /* if crc_on_copy is set, compute and store CRC32 value for chunk file */
  if (scr_crc_on_copy) {
    scr_compute_crc(map, id, scr_my_rank_world, my_chunk_file);
    /* TODO: would be nice to save this CRC in our partner's XOR file so we can check correctness on a rebuild */
  }

  return rc;
}

/* apply redundancy scheme to file and return number of bytes copied in bytes parameter */
int scr_copy_files(scr_filemap* map, const struct scr_reddesc* c, int id, double* bytes)
{
  /* initialize to 0 */
  *bytes = 0.0;

  /* step through each of my files for the specified dataset to scan for any incomplete files */
  int valid = 1;
  double my_bytes = 0.0;
  scr_hash_elem* file_elem;
  for (file_elem = scr_filemap_first_file(map, id, scr_my_rank_world);
       file_elem != NULL;
       file_elem = scr_hash_elem_next(file_elem))
  {
    /* get the filename */
    char* file = scr_hash_elem_key(file_elem);

    /* check the file */
    if (!scr_bool_have_file(map, id, scr_my_rank_world, file, scr_ranks_world)) {
      scr_dbg(2, "scr_copy_files: File determined to be invalid: %s", file);
      valid = 0;
    }

    /* add up the number of bytes on our way through */
    my_bytes += (double) scr_filesize(file);

    /* if crc_on_copy is set, compute crc and update meta file (PARTNER does this during the copy) */
    if (scr_crc_on_copy && c->copy_type != SCR_COPY_PARTNER) {
      scr_compute_crc(map, id, scr_my_rank_world, file);
    }
  }

  /* determine whether everyone's files are good */
  int all_valid = scr_alltrue(valid);
  if (! all_valid) {
    if (scr_my_rank_world == 0) {
      scr_dbg(1, "scr_copy_files: Exiting copy since one or more checkpoint files is invalid");
    }
    return SCR_FAILURE;
  }

  /* start timer */
  time_t timestamp_start;
  double time_start;
  if (scr_my_rank_world == 0) {
    timestamp_start = scr_log_seconds();
    time_start = MPI_Wtime();
  }

  /* apply the redundancy scheme */
  int rc = SCR_FAILURE;
  switch (c->copy_type) {
  case SCR_COPY_LOCAL:
    rc = SCR_SUCCESS;
    break;
  case SCR_COPY_PARTNER:
    rc = scr_copy_partner(map, c, id);
    break;
  case SCR_COPY_XOR:
    rc = scr_copy_xor(map, c, id);
    break;
  }

  /* record the number of files this task wrote during this dataset 
   * (need to remember when a task writes 0 files) */
  int num_files = scr_filemap_num_files(map, id, scr_my_rank_world);
  scr_filemap_set_expected_files(map, id, scr_my_rank_world, num_files);
  scr_filemap_write(scr_map_file, map);

  /* determine whether everyone succeeded in their copy */
  int valid_copy = (rc == SCR_SUCCESS);
  if (! valid_copy) {
    scr_err("scr_copy_files failed with return code %d @ %s:%d", rc, __FILE__, __LINE__);
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
    scr_dbg(1, "scr_copy_files: %f secs, %e bytes, %f MB/s, %f MB/s per proc",
            time_diff, *bytes, bw, bw/scr_ranks_world
    );

    /* log data on the copy in the database */
    if (scr_log_enable) {
      char dir[SCR_MAX_FILENAME];
      scr_cache_dir_get(c, id, dir);
      scr_log_transfer("COPY", c->base, dir, &id, &timestamp_start, &time_diff, bytes);
    }
  }

  return rc;
}

/*
=========================================
Flush and fetch functions
=========================================
*/

/* read in the summary file from dir assuming file is using version 4 format or earlier, convert to version 5 hash */
static int scr_summary_read_v4_to_v5(const char* dir, scr_hash* summary_hash)
{
  /* check that we have a pointer to a hash */
  if (summary_hash == NULL) {
    return SCR_FAILURE;
  }

  /* check whether we can read the summary file */
  char summary_file[SCR_MAX_FILENAME];
  if (scr_build_path(summary_file, sizeof(summary_file), dir, "scr_summary.txt") != SCR_SUCCESS) {
    scr_err("Failed to build full filename for summary file @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* check whether we can read the file before we actually try,
   * we take this step to avoid printing an error in scr_hash_read */
  if (access(summary_file, R_OK) < 0) {
    return SCR_FAILURE;
  }

  /* open the summary file */
  FILE* fs = fopen(summary_file, "r");
  if (fs == NULL) {
    scr_err("Opening summary file for read: fopen(%s, \"r\") errno=%d %m @ %s:%d",
            summary_file, errno, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* assume we have one file per rank */
  int num_records = scr_ranks_world;

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
      sscanf(line, "%s %d", field, &num_records);
    }
    fgets(line, sizeof(line), fs);
    linenum++;
  }

  /* now we know how many records we'll be reading, so allocate space for them */
  if (num_records <= 0) {
    scr_err("No file records found in summary file %s, perhaps it is corrupt or incomplete @ %s:%d",
            summary_file, __FILE__, __LINE__
    );
    fclose(fs);
    return SCR_FAILURE;
  }

  /* set the version number in the summary hash, initialize a pointer to the checkpoint hash */
  scr_hash_set_kv_int(summary_hash, SCR_SUMMARY_KEY_VERSION, SCR_SUMMARY_FILE_VERSION_5);
  scr_hash* ckpt_hash = NULL;

  /* read the record for each rank */
  int i;
  int bad_values        =  0;
  int all_complete      =  1;
  int all_ranks         = -1;
  int all_checkpoint_id = -1;
  for(i=0; i < num_records; i++) {
    int expected_n, n;
    int rank, scr, ranks, pattern, complete, match_filesize, checkpoint_id;
    char filename[SCR_MAX_FILENAME];
    unsigned long exp_filesize, filesize;
    int crc_computed = 0;
    uLong crc = 0UL;

    /* read a line from the file, parse depending on version */
    if (version == 1) {
      expected_n = 10;
      n = fscanf(fs, "%d\t%d\t%d\t%d\t%d\t%d\t%lu\t%d\t%lu\t%s\n",
                 &rank, &scr, &ranks, &pattern, &checkpoint_id, &complete,
                 &exp_filesize, &match_filesize, &filesize, filename
      );
      linenum++;
    } else {
      expected_n = 11;
      n = fscanf(fs, "%d\t%d\t%d\t%d\t%d\t%lu\t%d\t%lu\t%s\t%d\t0x%lx\n",
                 &rank, &scr, &ranks, &checkpoint_id, &complete,
                 &exp_filesize, &match_filesize, &filesize, filename,
                 &crc_computed, &crc
      );
      linenum++;
    }

    /* check the return code returned from the read */
    if (n == EOF) {
      scr_err("Early EOF in summary file %s at line %d.  Only read %d of %d expected records @ %s:%d",
              summary_file, linenum, i, num_records, __FILE__, __LINE__
      );
      fclose(fs);
      scr_hash_unset_all(summary_hash);
      return SCR_FAILURE;
    } else if (n != expected_n) {
      scr_err("Invalid read of record %d in %s at line %d @ %s:%d",
              i, summary_file, linenum, __FILE__, __LINE__
      );
      fclose(fs);
      scr_hash_unset_all(summary_hash);
      return SCR_FAILURE;
    }

    /* TODO: check whether all files are complete, match expected size, number of ranks, checkpoint_id, etc */
    if (rank < 0 || rank >= scr_ranks_world) {
      bad_values = 1;
      scr_err("Invalid rank detected (%d) in a job with %d tasks in %s at line %d @ %s:%d",
              rank, scr_my_rank_world, summary_file, linenum, __FILE__, __LINE__
      );
    }

    /* chop to basename of filename */
    char* base = basename(filename);

    /* set the pointer to the checkpoint hash, if we haven't already */
    if (ckpt_hash == NULL) {
      /* get a pointer to the checkpoint hash */
      ckpt_hash = scr_hash_set_kv_int(summary_hash, SCR_SUMMARY_5_KEY_CKPT, checkpoint_id);
    }

    /* get a pointer to the hash for this rank, and then to the file for this rank */
    scr_hash* rank_hash = scr_hash_set_kv_int(ckpt_hash, SCR_SUMMARY_5_KEY_RANK, rank);
    scr_hash* file_hash = scr_hash_set_kv(    rank_hash, SCR_SUMMARY_5_KEY_FILE, base);

    /* set the file size, and the crc32 value if it was computed */
    scr_hash_util_set_bytecount(file_hash, SCR_SUMMARY_5_KEY_SIZE, exp_filesize);
    if (crc_computed) {
      scr_hash_util_set_crc32(file_hash, SCR_SUMMARY_5_KEY_CRC, crc);
    }

    /* if the file is incomplete, set the incomplete field for this file */
    if (! complete) {
      all_complete = 0;
      scr_hash_set_kv_int(file_hash, SCR_SUMMARY_5_KEY_COMPLETE, 0);
    }

    /* check that the checkpoint id matches all other checkpoint ids in the file */
    if (checkpoint_id != all_checkpoint_id) {
      if (all_checkpoint_id == -1) {
        all_checkpoint_id = checkpoint_id;
      } else {
        bad_values = 1;
        scr_err("Checkpoint id %d on record %d does not match expected checkpoint id %d in %s at line %d @ %s:%d",
                checkpoint_id, i, all_checkpoint_id, summary_file, linenum, __FILE__, __LINE__
        );
      }
    }

    /* check that the number of ranks matches all the number of ranks specified by all other records in the file */
    if (ranks != all_ranks) {
      if (all_ranks == -1) {
        all_ranks = ranks;
      } else {
        bad_values = 1;
        scr_err("Number of ranks %d on record %d does not match expected number of ranks %d in %s at line %d @ %s:%d",
                ranks, i, all_ranks, summary_file, linenum, __FILE__, __LINE__
        );
      }
    }
  }

  /* we've read in all of the records, now set the values for the complete field and the number of ranks field */
  if (ckpt_hash != NULL) {
    scr_hash_set_kv_int(ckpt_hash, SCR_SUMMARY_5_KEY_COMPLETE, all_complete);
    scr_hash_set_kv_int(ckpt_hash, SCR_SUMMARY_5_KEY_RANKS, all_ranks);
  }

  /* close the file */
  fclose(fs);

  /* if we found any problems while reading the file, clear the hash and return with an error */
  if (bad_values) {
    /* clear the hash, since we may have set bad values */
    scr_hash_unset_all(summary_hash);
    return SCR_FAILURE;
  }

  /* otherwise, return success */
  return SCR_SUCCESS;
}

/* verify the hash is a valid hash for a version 5 summary file */
static int scr_summary_check_v5(scr_hash* hash)
{
  /* check that the summary file version is something we support */
  int version;
  if (scr_hash_util_get_int(hash, SCR_SUMMARY_KEY_VERSION, &version) != SCR_SUCCESS) {
    /* couldn't find version number */
    scr_err("Failed to read version number in summary file @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  if (version != SCR_SUMMARY_FILE_VERSION_5) {
    /* invalid version number */
    scr_err("Found version number %d when %d was expected in summary file @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* check that we have exactly one checkpoint */
  scr_hash* ckpt_hash = scr_hash_get(hash, SCR_SUMMARY_5_KEY_CKPT);
  if (scr_hash_size(ckpt_hash) != 1) {
    scr_err("More than one checkpoint found in summary file @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* get the first (and only) checkpoint id */
  char* ckpt_str = scr_hash_elem_get_first_val(hash, SCR_SUMMARY_5_KEY_CKPT);
  scr_hash* ckpt = scr_hash_get(ckpt_hash, ckpt_str);

  /* check that the complete string is set and is set to 1 */
  int complete;
  if (scr_hash_util_get_int(ckpt, SCR_SUMMARY_5_KEY_COMPLETE, &complete) != SCR_SUCCESS) {
    /* could not find complete value (assume it's incomplete) */
    return SCR_FAILURE;
  }
  if (complete != 1) {
    /* checkpoint is marked as incomplete */
    return SCR_FAILURE;
  }

  /* read in the the number of ranks for this checkpoint */
  int ranks;
  if (scr_hash_util_get_int(ckpt, SCR_SUMMARY_5_KEY_RANKS, &ranks) != SCR_SUCCESS) {
    scr_err("Failed to read number of ranks in summary file @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* check that the number of ranks matches the number we're currently running with */
  if (ranks != scr_ranks_world) {
    scr_err("Number of ranks %d that wrote checkpoint does not match current number of ranks %d @ %s:%d",
            ranks, scr_ranks_world, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  return SCR_SUCCESS;
}

/* read in the summary file from dir */
static int scr_summary_read_v5(const char* dir, scr_hash* summary_hash)
{
  /* check that we got a pointer to a hash */
  if (summary_hash == NULL) {
    return SCR_FAILURE;
  }

  /* build the filename for the summary file */
  char summary_file[SCR_MAX_FILENAME];
  if (scr_build_path(summary_file, sizeof(summary_file), dir, "summary.scr") != SCR_SUCCESS) {
    scr_err("Failed to build full filename for summary file @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* check whether we can read the file before we actually try,
   * we take this step to avoid printing an error in scr_hash_read */
  if (access(summary_file, R_OK) < 0) {
    return SCR_FAILURE;
  }

  /* read in the summary hash file */
  if (scr_hash_read(summary_file, summary_hash) != SCR_SUCCESS) {
    scr_err("Reading summary file %s @ %s:%d",
            summary_file, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* if we made it here, we successfully read the summary file as a hash */
  return SCR_SUCCESS;
}

/* read in the summary file from dir */
static int scr_summary_read_v6(const char* dir, scr_hash* summary_hash)
{
  /* check that we got a pointer to a hash */
  if (summary_hash == NULL) {
    return SCR_FAILURE;
  }

  /* build the filename for the summary file */
  char summary_file[SCR_MAX_FILENAME];
  if (scr_build_path(summary_file, sizeof(summary_file), dir, "summary.scr") != SCR_SUCCESS) {
    scr_err("Failed to build full filename for summary file @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* check whether we can read the file before we actually try,
   * we take this step to avoid printing an error in scr_hash_read */
  if (access(summary_file, R_OK) < 0) {
    return SCR_FAILURE;
  }

  /* read in the summary hash file */
  if (scr_hash_read(summary_file, summary_hash) != SCR_SUCCESS) {
    scr_err("Reading summary file %s @ %s:%d",
            summary_file, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* read the version from the summary hash */
  int version;
  if (scr_hash_util_get_int(summary_hash, SCR_SUMMARY_KEY_VERSION, &version) != SCR_SUCCESS) {
    scr_err("Failed to read version from summary file %s @ %s:%d",
            summary_file, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* check that the version number matches */
  if (version != SCR_SUMMARY_FILE_VERSION_6) {
    scr_err("Summary file %s is version %d instead of version %d @ %s:%d",
            summary_file, version, SCR_SUMMARY_FILE_VERSION_6, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* if we made it here, we successfully read the summary file as a hash */
  return SCR_SUCCESS;
}

static int scr_summary_convert_v5_to_v6(scr_hash* old, scr_hash* new)
{
  /* TODO: convert into a version 6 hash */

  return SCR_SUCCESS;
}

/* read in the summary file from dir */
static int scr_summary_read(const char* dir, scr_hash* summary_hash)
{
  /* check that we have pointers to a hash */
  if (summary_hash == NULL) {
    return SCR_FAILURE;
  }

  /* clear the hash */
  scr_hash_unset_all(summary_hash);

  /* attempt to read the summary file, assuming it is in version 5 format */
  if (scr_summary_read_v6(dir, summary_hash) != SCR_SUCCESS) {
    /* failed to read file as version 6 format, try to get a version 5 hash */
    scr_hash* summary_hash_v5 = scr_hash_new();
    if (scr_summary_read_v5(dir, summary_hash_v5) != SCR_SUCCESS) {
      /* failed to read the summary file, try again, but now assume an older format */
      if (scr_summary_read_v4_to_v5(dir, summary_hash_v5) != SCR_SUCCESS) {
        /* we still failed, don't report an error here, just return failure,
         * the read functions will report errors as needed */
        scr_err("Reading summary file in %s @ %s:%d",
                dir, __FILE__, __LINE__
        );
        scr_hash_delete(summary_hash_v5);
        return SCR_FAILURE;
      }
    }

    /* check that the hash looks like version 5 summary file */
    if (scr_summary_check_v5(summary_hash_v5) != SCR_SUCCESS) {
      /* hash failed version 5 check */
      scr_err("Invalid version 5 summary file in %s @ %s:%d",
              dir, __FILE__, __LINE__
      );
      scr_hash_delete(summary_hash_v5);
      return SCR_FAILURE;
    }

    /* convert version 5 summary file hash into version 6 hash */
    if (scr_summary_convert_v5_to_v6(summary_hash_v5, summary_hash) != SCR_SUCCESS) {
      /* failed to convert version 5 hash into version 6 format */
      scr_err("Invalid version 5 summary file in %s @ %s:%d",
              dir, __FILE__, __LINE__
      );
      scr_hash_delete(summary_hash_v5);
      return SCR_FAILURE;
    }
    scr_hash_delete(summary_hash_v5);
  }

  /* TODO: check that hash looks like a version 6 hash */

  return SCR_SUCCESS;
}

/* write out the summary file to dir */
static int scr_summary_write(const char* dir, const scr_dataset* dataset, int all_complete, scr_hash* data)
{
  /* get the dataset name */
  char* name;
  if (scr_dataset_get_name(dataset, &name) != SCR_SUCCESS) {
    scr_err("Failed to get dataset name for summary file @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* build the dataset directory */
  char dset_dir[SCR_MAX_FILENAME];
  if (scr_build_path(dset_dir, sizeof(dset_dir), dir, name) != SCR_SUCCESS) {
    scr_err("Failed to build full directory for summary file @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* build the summary filename */
  char file[SCR_MAX_FILENAME];
  if (scr_build_path(file, sizeof(file), dset_dir, "summary.scr") != SCR_SUCCESS) {
    scr_err("Failed to build full filename for summary file @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* create an empty hash to build our summary info */
  scr_hash* summary_hash = scr_hash_new();

  /* write the summary file version number */
  scr_hash_util_set_int(summary_hash, SCR_SUMMARY_KEY_VERSION, SCR_SUMMARY_FILE_VERSION_6);

  /* mark whether the flush is complete in the summary file */
  scr_hash_util_set_int(summary_hash, SCR_SUMMARY_6_KEY_COMPLETE, all_complete);

  /* write the dataset descriptor */
  scr_hash* dataset_hash = scr_hash_new();
  scr_hash_merge(dataset_hash, dataset);
  scr_hash_set(summary_hash, SCR_SUMMARY_6_KEY_DATASET, dataset_hash);

  /* for each file, insert hash listing filename, then file size, crc,
   * and incomplete flag under that */
  scr_hash_merge(summary_hash, data);

  /* write the number of ranks used to write this dataset */
  scr_hash* rank2file_hash = scr_hash_get(summary_hash, SCR_SUMMARY_6_KEY_RANK2FILE);
  scr_hash_util_set_int(rank2file_hash, SCR_SUMMARY_6_KEY_RANKS, scr_ranks_world);

  /* write the hash to a file */
  scr_hash_write(file, summary_hash);

  /* free the hash object */
  scr_hash_delete(summary_hash);

  /* mark the dataset as complete in the index file */
  scr_hash* index_hash = scr_hash_new();
  scr_index_read(scr_par_prefix, index_hash);
  scr_index_set_dataset(index_hash, dataset, all_complete);
  scr_index_write(scr_par_prefix, index_hash);
  scr_hash_delete(index_hash);

  return SCR_SUCCESS;
}

/* write out the summary file to dir */
static int scr_summary_write_old(const char* dir, int checkpoint_id, int all_complete, scr_hash* data)
{
  return SCR_FAILURE;
}

/* returns true if the given dataset id needs to be flushed */
static int scr_bool_need_flush(int id)
{
  int need_flush = 0;

  /* just have rank 0 read the file */
  if (scr_my_rank_world == 0) {
    /* read the flush file */
    scr_hash* hash = scr_hash_new();
    scr_hash_read(scr_flush_file, hash);

    /* if we have the dataset in cache, but not on the parallel file system,
     * then it needs to be flushed */
    scr_hash* dset_hash = scr_hash_get_kv_int(hash, SCR_FLUSH_KEY_DATASET, id);
    scr_hash* in_cache = scr_hash_get_kv(dset_hash, SCR_FLUSH_KEY_LOCATION, SCR_FLUSH_KEY_LOCATION_CACHE);
    scr_hash* in_pfs   = scr_hash_get_kv(dset_hash, SCR_FLUSH_KEY_LOCATION, SCR_FLUSH_KEY_LOCATION_PFS);
    if (in_cache != NULL && in_pfs == NULL) {
      need_flush = 1;
    }

    /* free the hash object */
    scr_hash_delete(hash);
  }
  MPI_Bcast(&need_flush, 1, MPI_INT, 0, scr_comm_world);

  return need_flush;
}

/* adds a location for the specified dataset id to the flush file */
static int scr_flush_file_location_set(int id, const char* location)
{
  /* only rank 0 updates the file */
  if (scr_my_rank_world == 0) {
    /* read the flush file into hash */
    scr_hash* hash = scr_hash_new();
    scr_hash_read(scr_flush_file, hash);

    /* set the location for this dataset */
    scr_hash* dset_hash = scr_hash_set_kv_int(hash, SCR_FLUSH_KEY_DATASET, id);
    scr_hash_set_kv(dset_hash, SCR_FLUSH_KEY_LOCATION, location);

    /* write the hash back to the flush file */
    scr_hash_write(scr_flush_file, hash);

    /* delete the hash */
    scr_hash_delete(hash);
  }
  return SCR_SUCCESS;
}

/* returns SCR_SUCCESS if specified dataset id is at specified location */
static int scr_flush_file_location_test(int id, const char* location)
{
  /* only rank 0 checks the status, bcasts the results to everyone else */
  int at_location = 0;
  if (scr_my_rank_world == 0) {
    /* read the flush file into hash */
    scr_hash* hash = scr_hash_new();
    scr_hash_read(scr_flush_file, hash);

    /* check the location for this dataset */
    scr_hash* dset_hash = scr_hash_get_kv_int(hash, SCR_FLUSH_KEY_DATASET, id);
    scr_hash* value     = scr_hash_get_kv(dset_hash, SCR_FLUSH_KEY_LOCATION, location);
    if (value != NULL) {
      at_location = 1;
    }

    /* delete the hash */
    scr_hash_delete(hash);
  }
  MPI_Bcast(&at_location, 1, MPI_INT, 0, scr_comm_world);

  if (! at_location) {
    return SCR_FAILURE;
  }
  return SCR_SUCCESS;
}

/* removes a location for the specified dataset id from the flush file */
static int scr_flush_file_location_unset(int id, const char* location)
{
  /* only rank 0 updates the file */
  if (scr_my_rank_world == 0) {
    /* read the flush file into hash */
    scr_hash* hash = scr_hash_new();
    scr_hash_read(scr_flush_file, hash);

    /* unset the location for this dataset */
    scr_hash* dset_hash = scr_hash_get_kv_int(hash, SCR_FLUSH_KEY_DATASET, id);
    scr_hash_unset_kv(dset_hash, SCR_FLUSH_KEY_LOCATION, location);

    /* write the hash back to the flush file */
    scr_hash_write(scr_flush_file, hash);

    /* delete the hash */
    scr_hash_delete(hash);
  }
  return SCR_SUCCESS;
}

/* remove any dataset ids from flush file which are not in cache,
 * and add any datasets in cache that are not in the flush file */
static int scr_flush_file_rebuild(const scr_filemap* map)
{
  if (scr_my_rank_world == 0) {
    /* read the flush file */
    scr_hash* hash = scr_hash_new();
    scr_hash_read(scr_flush_file, hash);

    /* get list of dataset ids in flush file */
    int flush_ndsets;
    int* flush_dsets;
    scr_hash* flush_dsets_hash = scr_hash_get(hash, SCR_FLUSH_KEY_DATASET);
    scr_hash_list_int(flush_dsets_hash, &flush_ndsets, &flush_dsets);

    /* get list of dataset ids in cache */
    int cache_ndsets;
    int* cache_dsets;
    scr_filemap_list_datasets(map, &cache_ndsets, &cache_dsets);

    int flush_index = 0;
    int cache_index = 0;
    while (flush_index < flush_ndsets && cache_index < cache_ndsets) {
      /* get next smallest index from flush file and cache */
      int flush_dset = flush_dsets[flush_index];
      int cache_dset = cache_dsets[cache_index];

      if (flush_dset < cache_dset) {
        /* dataset exists in flush file but not in cache,
         * delete it from the flush file */
        scr_hash_unset_kv_int(hash, SCR_FLUSH_KEY_DATASET, flush_dset);
        flush_index++;
      } else if (cache_dset < flush_dset) {
        /* dataset exists in cache but not flush file,
         * add it to the flush file */
        scr_hash* dset_hash = scr_hash_set_kv_int(hash, SCR_FLUSH_KEY_DATASET, cache_dset);
        scr_hash_set_kv(dset_hash, SCR_FLUSH_KEY_LOCATION, SCR_FLUSH_KEY_LOCATION_CACHE);
        cache_index++;
      } else {
        /* dataset exists in cache and the flush file,
         * ensure that it is listed as being in the cache */
        scr_hash* dset_hash = scr_hash_set_kv_int(hash, SCR_FLUSH_KEY_DATASET, cache_dset);
        scr_hash_unset_kv(dset_hash, SCR_FLUSH_KEY_LOCATION, SCR_FLUSH_KEY_LOCATION_CACHE);
        scr_hash_set_kv(dset_hash, SCR_FLUSH_KEY_LOCATION, SCR_FLUSH_KEY_LOCATION_CACHE);
        flush_index++;
        cache_index++;
      }
    }
    while (flush_index < flush_ndsets) {
      /* dataset exists in flush file but not in cache,
       * delete it from the flush file */
      int flush_dset = flush_dsets[flush_index];
      scr_hash_unset_kv_int(hash, SCR_FLUSH_KEY_DATASET, flush_dset);
      flush_index++;
    }
    while (cache_index < cache_ndsets) {
      /* dataset exists in cache but not flush file,
       * add it to the flush file */
      int cache_dset = cache_dsets[cache_index];
      scr_hash* dset_hash = scr_hash_set_kv_int(hash, SCR_FLUSH_KEY_DATASET, cache_dset);
      scr_hash_set_kv(dset_hash, SCR_FLUSH_KEY_LOCATION, SCR_FLUSH_KEY_LOCATION_CACHE);
      cache_index++;
    }

    /* free our list of cache dataset ids */
    if (cache_dsets != NULL) {
      free(cache_dsets);
      cache_dsets = NULL;
    }

    /* free our list of flush file dataset ids */
    if (flush_dsets != NULL) {
      free(flush_dsets);
      flush_dsets = NULL;
    }

    /* write the hash back to the flush file */
    scr_hash_write(scr_flush_file, hash);

    /* delete the hash */
    scr_hash_delete(hash);
  }
  return SCR_SUCCESS;
}

/* checks whether the specified dataset id is currently being flushed */
static int scr_bool_is_flushing(int id)
{
  /* assume we are not flushing this checkpoint */
  int is_flushing = 0;

  /* only rank 0 tests the file */
  if (scr_my_rank_world == 0) {
    /* read flush file into hash */
    scr_hash* hash = scr_hash_new();
    scr_hash_read(scr_flush_file, hash);

    /* attempt to look up the FLUSHING state for this checkpoint */
    scr_hash* dset_hash = scr_hash_get_kv_int(hash, SCR_FLUSH_KEY_DATASET, id);
    scr_hash* flushing_hash = scr_hash_get_kv(dset_hash, SCR_FLUSH_KEY_LOCATION, SCR_FLUSH_KEY_LOCATION_FLUSHING);
    if (flushing_hash != NULL) {
      is_flushing = 1;
    }

    /* delete the hash */
    scr_hash_delete(hash);
  }

  /* need every task to agree that this checkpoint is not being flushed */
  if (! scr_alltrue(is_flushing == 0)) {
    is_flushing = 1;
  }
  return is_flushing;
}

/* fetch file name in meta from dir and build new full path in newfile,
 * return whether operation succeeded */
static int scr_fetch_a_file(const char* src_dir, const scr_meta* meta, const char* dst_dir,
                            char* newfile, size_t newfile_size)
{
  int success = SCR_SUCCESS;

  /* get the filename from the meta data */
  char* meta_filename;
  if (scr_meta_get_filename(meta, &meta_filename) != SCR_SUCCESS) {
    scr_err("Failed to read filename from meta data @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* build full path to file */
  char filename[SCR_MAX_FILENAME];
  if (scr_build_path(filename, sizeof(filename), src_dir, meta_filename) != SCR_SUCCESS) {
    scr_err("Failed to build full file name of target file for fetch @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* fetch the file */
  uLong crc;
  uLong* crc_p = NULL;
  if (scr_crc_on_flush) {
    crc_p = &crc;
  }
  success = scr_copy_to(filename, dst_dir, scr_file_buf_size, newfile, newfile_size, crc_p);

  /* check that crc matches crc stored in meta */
  uLong meta_crc;
  if (scr_meta_get_crc32(meta, &meta_crc) == SCR_SUCCESS) {
    if (success == SCR_SUCCESS && scr_crc_on_flush && crc != meta_crc) {
      success = SCR_FAILURE;
      scr_err("CRC32 mismatch detected when fetching file from %s to %s @ %s:%d",
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
  }

  return success;
}

static int scr_container_get_name_size_offset_length(
  const scr_hash* segment, const scr_hash* containers,
  char** name, unsigned long* size, unsigned long* offset, unsigned long* length)
{
  /* check that our parameters are valid */
  if (segment == NULL || containers == NULL || name == NULL || size == NULL || offset == NULL || length == NULL) {
    return SCR_FAILURE;
  }

  /* lookup the segment length */
  if (scr_hash_util_get_bytecount(segment, SCR_SUMMARY_6_KEY_LENGTH, length) != SCR_SUCCESS) {
    return SCR_FAILURE;
  }

  /* get the container hash */
  scr_hash* container = scr_hash_get(segment, SCR_SUMMARY_6_KEY_CONTAINER);

  /* lookup id for container */
  int id;
  if (scr_hash_util_get_int(container, SCR_SUMMARY_6_KEY_ID, &id) != SCR_SUCCESS) {
    return SCR_FAILURE;
  }

  /* lookup the offset value */
  if (scr_hash_util_get_bytecount(container, SCR_SUMMARY_6_KEY_OFFSET, offset) != SCR_SUCCESS) {
    return SCR_FAILURE;
  }

  /* get container with matching id from containers list */
  scr_hash* info = scr_hash_getf(containers, "%d", id);

  /* get name of container */
  if (scr_hash_util_get_str(info, SCR_KEY_NAME, name) != SCR_SUCCESS) {
    return SCR_FAILURE;
  }

  /* get size of container */
  if (scr_hash_util_get_bytecount(info, SCR_KEY_SIZE, size) != SCR_SUCCESS) {
    return SCR_FAILURE;
  }

  return SCR_SUCCESS;
}

/* fetch file name in meta from dir and build new full path in newfile,
 * return whether operation succeeded */
static int scr_fetch_file_from_containers(const char* file, scr_meta* meta, scr_hash* segments, const scr_hash* containers)
{
  unsigned long buf_size = scr_file_buf_size;

  /* check that we got something for a source file */
  if (file == NULL || strcmp(file, "") == 0) {
    scr_err("Invalid source file @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* check that our other arguments are valid */
  if (meta == NULL || segments == NULL || containers == NULL) {
    scr_err("Invalid metadata, segments, or container @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* open the file for writing */
  int fd_src = scr_open(file, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
  if (fd_src < 0) {
    scr_err("Opening file to copy: scr_open(%s) errno=%d %m @ %s:%d",
            file, errno, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* TODO:
  posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED | POSIX_FADV_SEQUENTIAL)
  that tells the kernel that you don't ever need the pages
  from the file again, and it won't bother keeping them in the page cache.
  */
  posix_fadvise(fd_src, 0, 0, POSIX_FADV_DONTNEED | POSIX_FADV_SEQUENTIAL);

  /* allocate buffer to read in file chunks */
  char* buf = (char*) malloc(buf_size);
  if (buf == NULL) {
    scr_err("Allocating memory: malloc(%llu) errno=%d %m @ %s:%d",
            buf_size, errno, __FILE__, __LINE__
    );
    scr_close(file, fd_src);
    return SCR_FAILURE;
  }

  /* initialize crc value */
  uLong crc;
  if (scr_crc_on_flush) {
    crc = crc32(0L, Z_NULL, 0);
  }

  int rc = SCR_SUCCESS;

  /* read in each segment */
  scr_hash_sort_int(segments, SCR_HASH_SORT_ASCENDING);
  scr_hash_elem* elem;
  for (elem = scr_hash_elem_first(segments);
       elem != NULL;
       elem = scr_hash_elem_next(elem))
  {
    /* get the container info for this segment */
    scr_hash* hash = scr_hash_elem_hash(elem);

    /* get the offset into the container and the length of the segment (both in bytes) */
    char* container_name;
    unsigned long container_size, container_offset, segment_length;
    if (scr_container_get_name_size_offset_length(hash, containers,
      &container_name, &container_size, &container_offset, &segment_length) != SCR_SUCCESS)
    {
      scr_err("Failed to get segment offset and length @ %s:%d",
              __FILE__, __LINE__
      );
      rc = SCR_FAILURE;
      break;
    }

    /* open container file for reading */
    int fd_container = scr_open(container_name, O_RDONLY);
    if (fd_container < 0) {
      scr_err("Opening file for reading: scr_open(%s) errno=%d %m @ %s:%d",
              container_name, errno, __FILE__, __LINE__
      );
      rc = SCR_FAILURE;
      break;
    }

    /* TODO:
    posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED | POSIX_FADV_SEQUENTIAL)
    that tells the kernel that you don't ever need the pages
    from the file again, and it won't bother keeping them in the page cache.
    */
    posix_fadvise(fd_container, 0, 0, POSIX_FADV_DONTNEED | POSIX_FADV_SEQUENTIAL);

    /* seek to offset within container */
    off_t pos = (off_t) container_offset;
    if (lseek(fd_container, pos, SEEK_SET) == (off_t)-1) {
      /* our seek failed, return an error */
      scr_err("Failed to seek to byte %lu in %s @ %s:%d",
              pos, container_name, __FILE__, __LINE__
      );
      rc = SCR_FAILURE;
      break;
    }
    
    /* copy data from container into file in chunks */
    unsigned long remaining = segment_length;
    while (remaining > 0) {
      /* read / write up to buf_size bytes at a time from file */
      unsigned long count = remaining;
      if (count > buf_size) {
        count = buf_size;
      }

      /* attempt to read buf_size bytes from container */
      int nread = scr_read_attempt(container_name, fd_container, buf, count);

      /* if we read some bytes, write them out */
      if (nread > 0) {
        /* optionally compute crc value as we go */
        if (scr_crc_on_flush) {
          crc = crc32(crc, (const Bytef*) buf, (uInt) nread);
        }

        /* write our nread bytes out */
        int nwrite = scr_write_attempt(file, fd_src, buf, nread);

        /* check for a write error or a short write */
        if (nwrite != nread) {
          /* write had a problem, stop copying and return an error */
          rc = SCR_FAILURE;
          break;
        }

        /* subtract the bytes we've processed from the number remaining */
        remaining -= (unsigned long) nread;
      }

      /* assume a short read is an error */
      if (nread < count) {
        /* read had a problem, stop copying and return an error */
        rc = SCR_FAILURE;
        break;
      }

      /* check for a read error, stop copying and return an error */
      if (nread < 0) {
        /* read had a problem, stop copying and return an error */
        rc = SCR_FAILURE;
        break;
      }
    }

    /* close container */
    if (scr_close(container_name, fd_container) != SCR_SUCCESS) {
      rc = SCR_FAILURE;
    }
  }

  /* close the source file */
  if (scr_close(file, fd_src) != SCR_SUCCESS) {
    rc = SCR_FAILURE;
  }

  /* free buffer */
  if (buf != NULL) {
    free(buf);
    buf = NULL;
  }

  /* verify crc value */
  if (rc == SCR_SUCCESS) {
    uLong crc2;
    if (scr_crc_on_flush) {
      if (scr_meta_get_crc32(meta, &crc2) == SCR_SUCCESS) {
        /* if a crc is already set in the meta data, check that we computed the same value */
        if (crc != crc2) {
          scr_err("CRC32 mismatch detected when fetching file %s @ %s:%d",
                  file, __FILE__, __LINE__
          );
          rc = SCR_FAILURE;
        }
      }
    }
  }

  return rc;
}

/* fetch files listed in hash into specified cache directory,
 * update filemap and fill in total number of bytes fetched,
 * returns SCR_SUCCESS if successful */
static int scr_fetch_files_list(const scr_hash* file_list, const char* dir, scr_filemap* map)
{
  /* assume we'll succeed in fetching our files */
  int rc = SCR_SUCCESS;

  /* assume we don't have any files to fetch */
  int my_num_files = 0;

  /* get dataset id */
  int id;
  scr_dataset* dataset = scr_hash_get(file_list, SCR_KEY_DATASET);
  scr_dataset_get_id(dataset, &id);

  /* get pointer to containers hash and copy into summary info if one exists */
  scr_hash* containers = scr_hash_get(file_list, SCR_SUMMARY_6_KEY_CONTAINER);

  /* now iterate through the file list and fetch each file */
  scr_hash_elem* file_elem = NULL;
  scr_hash* files = scr_hash_get(file_list, SCR_KEY_FILE);
  for (file_elem = scr_hash_elem_first(files);
       file_elem != NULL;
       file_elem = scr_hash_elem_next(file_elem))
  {
    /* get the filename */
    char* file = scr_hash_elem_key(file_elem);

    /* get a pointer to the hash for this file */
    scr_hash* hash = scr_hash_elem_hash(file_elem);

    /* check whether we are supposed to fetch this file */
    /* TODO: this is a hacky way to avoid reading a redundancy file back in
     * assuming that it's an original file, which breaks our redundancy computation
     * due to a name conflict on the file names */
    scr_hash_elem* no_fetch_hash = scr_hash_elem_get(hash, SCR_SUMMARY_6_KEY_NOFETCH);
    if (no_fetch_hash != NULL) {
      continue;
    }

    /* increment our file count */
    my_num_files++;

    /* split filename into path and name components */
    char path[SCR_MAX_FILENAME];
    char name[SCR_MAX_FILENAME];
    scr_split_path(file, path, name);

    /* build the destination file name */
    char newfile[SCR_MAX_FILENAME];
    scr_build_path(newfile, sizeof(newfile), dir, name);
      
    /* add the file to our filemap and write it to disk before creating the file */
    scr_filemap_add_file(map, id, scr_my_rank_world, newfile);
    scr_filemap_write(scr_map_file, map);

    /* get the file size */
    unsigned long filesize = 0;
    if (scr_hash_util_get_unsigned_long(hash, SCR_KEY_SIZE, &filesize) != SCR_SUCCESS) {
      scr_err("Failed to read file size from summary data @ %s:%d",
              __FILE__, __LINE__
      );
      rc = SCR_FAILURE;
      break;
    }

    /* check for a complete flag */
    int complete = 1;
    if (scr_hash_util_get_int(hash, SCR_KEY_COMPLETE, &complete) != SCR_SUCCESS) {
      /* in summary file, the absence of a complete flag on a file implies the file is complete */
      complete = 1;
    }

    /* create a new meta data object for this file */
    scr_meta* meta = scr_meta_new();

    /* set the meta data */
    scr_meta_set_filename(meta, newfile);
    scr_meta_set_filetype(meta, SCR_META_FILE_FULL);
    scr_meta_set_filesize(meta, filesize);
    scr_meta_set_complete(meta, 1);
    /* TODODSET: move the ranks field elsewhere, for now it's needed by scr_index.c */
    scr_meta_set_ranks(meta, scr_ranks_world);

    /* get the crc, if set, and add it to the meta data */
    uLong crc;
    if (scr_hash_util_get_crc32(hash, SCR_KEY_CRC, &crc) == SCR_SUCCESS) {
      scr_meta_set_crc32(meta, crc);
    }

    /* fetch file from containers if they are defined, otherwise fetch the native file */
    if (containers != NULL) {
      /* lookup segments hash for this file */
      scr_hash* segments = scr_hash_get(hash, SCR_SUMMARY_6_KEY_SEGMENT);

      /* fetch file from containers */
      if (scr_fetch_file_from_containers(newfile, meta, segments, containers) != SCR_SUCCESS) {
        /* failed to fetch file, mark it as incomplete */
        scr_meta_set_complete(meta, 0);
        rc = SCR_FAILURE;
      }
    } else {
      /* fetch native file, lookup directory for this file */
      char* from_dir;
      if (scr_hash_util_get_str(hash, SCR_KEY_PATH, &from_dir) == SCR_SUCCESS) {
        if (scr_fetch_a_file(from_dir, meta, dir, newfile, sizeof(newfile)) != SCR_SUCCESS) {
          /* failed to fetch file, mark it as incomplete */
          scr_meta_set_complete(meta, 0);
          rc = SCR_FAILURE;
        }
      } else {
        /* failed to read source directory, mark file as incomplete */
        scr_meta_set_complete(meta, 0);
        rc = SCR_FAILURE;
      }
    }

    /* TODODSET: want to write out filemap before we start to fetch each file? */

    /* mark the file as complete */
    scr_filemap_set_meta(map, id, scr_my_rank_world, newfile, meta);

    /* free the meta data object */
    scr_meta_delete(meta);
  }

  /* set the expected number of files for this dataset */
  scr_filemap_set_expected_files(map, id, scr_my_rank_world, my_num_files);
  scr_filemap_write(scr_map_file, map);

  return rc;
}

/* read contents of summary file */
static int scr_fetch_summary(const char* dir, scr_hash* file_list)
{
  int fetched = SCR_FAILURE;

  /* get a new hash to read summary data into */
  scr_hash* summary_hash = scr_hash_new();

  /* have rank 0 read summary file, if it exists */
  if (scr_my_rank_world == 0) {
    /* check that we can access the directory */
    if (access(dir, R_OK) == 0) {
      /* read data from the summary file */
      fetched = scr_summary_read(dir, summary_hash);
    } else {
      scr_err("Failed to access directory %s @ %s:%d", dir, __FILE__, __LINE__);
    }
  }

  /* broadcast success code from rank 0 */
  MPI_Bcast(&fetched, 1, MPI_INT, 0, scr_comm_world);

  /* scatter data from summary file to other ranks */
  if (fetched == SCR_SUCCESS) {
    /* broadcast the dataset information */
    scr_hash* dataset_hash = scr_hash_new();
    if (scr_my_rank_world == 0) {
      scr_dataset* dataset = scr_hash_get(summary_hash, SCR_SUMMARY_6_KEY_DATASET);
      scr_hash_merge(dataset_hash, dataset);
    }
    scr_hash_bcast(dataset_hash, 0, scr_comm_world);
    scr_hash_set(file_list, SCR_SUMMARY_6_KEY_DATASET, dataset_hash);

    /* TODO: it's overkill to bcast info for all containers, each proc only really needs to know
     * about the containers that contain its files */

    /* broadcast the container file information if we have any */
    scr_hash* container_hash = scr_hash_new();
    if (scr_my_rank_world == 0) {
      scr_dataset* container = scr_hash_get(summary_hash, SCR_SUMMARY_6_KEY_CONTAINER);
      scr_hash_merge(container_hash, container);
    }
    scr_hash_bcast(container_hash, 0, scr_comm_world);
    if (scr_hash_size(container_hash) > 0) {
      scr_hash_set(file_list, SCR_SUMMARY_6_KEY_CONTAINER, container_hash);
    }

    scr_hash_elem* elem;

    /* scatter out file information for each rank */
    scr_hash* send_hash = NULL;
    if (scr_my_rank_world == 0) {
      scr_hash* rank2file_hash = scr_hash_get(summary_hash, SCR_SUMMARY_6_KEY_RANK2FILE);
      send_hash = scr_hash_get(rank2file_hash, SCR_SUMMARY_6_KEY_RANK);
    }
    scr_hash* recv_hash = scr_hash_new();
    scr_hash_exchange(send_hash, recv_hash, scr_comm_world);

    /* iterate over the ranks that sent data to us, and set up our list of files */
    for (elem = scr_hash_elem_first(recv_hash);
         elem != NULL;
         elem = scr_hash_elem_next(elem))
    {
      /* get the file hash from the current rank */
      scr_hash* elem_hash = scr_hash_elem_hash(elem);
      scr_hash* file_hash = scr_hash_get(elem_hash, SCR_SUMMARY_6_KEY_FILE);

      /* copy the file hash */
      scr_hash* tmp_hash = scr_hash_new();
      scr_hash_merge(tmp_hash, file_hash);
      scr_hash_set(file_list, SCR_KEY_FILE, tmp_hash);
    }
    scr_hash_delete(recv_hash);

    /* if we're not using containers, add PATH entry for each of our files */
    scr_hash* files = scr_hash_get(file_list, SCR_KEY_FILE);
    for (elem = scr_hash_elem_first(files);
         elem != NULL;
         elem = scr_hash_elem_next(elem))
    {
      /* set the source path of each file */
      scr_hash* hash = scr_hash_elem_hash(elem);
      scr_hash_util_set_str(hash, SCR_KEY_PATH, dir);
    }
  }

  /* delete the summary hash object */
  scr_hash_delete(summary_hash);

  return fetched;
}

static int scr_fetch_data(const scr_hash* file_list, const char* ckpt_dir, scr_filemap* map)
{
  int success = SCR_SUCCESS;

  /* flow control rate of file reads from rank 0 by scattering file names to processes */
  if (scr_my_rank_world == 0) {
    /* fetch these files into the checkpoint directory */
    if (scr_fetch_files_list(file_list, ckpt_dir, map) != SCR_SUCCESS) {
      success = SCR_FAILURE;
    }

    /* now, have a sliding window of w processes read simultaneously */
    int w = scr_fetch_width;
    if (w > scr_ranks_world-1) {
      w = scr_ranks_world-1;
    }

    /* allocate MPI_Request arrays and an array of ints */
    int* flags       = (int*)         malloc(2 * w * sizeof(int));
    MPI_Request* req = (MPI_Request*) malloc(2 * w * sizeof(MPI_Request));
    MPI_Status status;
    if (flags == NULL || req == NULL) {
      scr_abort(-1, "Failed to allocate memory for flow control @ %s:%d",
                __FILE__, __LINE__
      );
    }

    int outstanding = 0;
    int index = 0;
    int i = 1;
    while (i < scr_ranks_world || outstanding > 0) {
      /* issue up to w outstanding sends and receives */
      while (i < scr_ranks_world && outstanding < w) {
        /* post a receive for the response message we'll get back when rank i is done */
        MPI_Irecv(&flags[index + w], 1, MPI_INT, i, 0, scr_comm_world, &req[index + w]);

        /* send a start signal to this rank */
        flags[index] = success;
        MPI_Isend(&flags[index], 1, MPI_INT, i, 0, scr_comm_world, &req[index]);

        /* update the number of outstanding requests */
        outstanding++;
        index++;
        i++;
      }

      /* wait to hear back from any rank */
      MPI_Waitany(w, &req[w], &index, &status);

      /* the corresponding send must be complete */
      MPI_Wait(&req[index], &status);

      /* check success code from process */
      if (flags[index + w] != SCR_SUCCESS) {
        success = SCR_FAILURE;
      }

      /* one less request outstanding now */
      outstanding--;
    }

    /* free the MPI_Request arrays */
    if (req != NULL) {
      free(req);
      req = NULL;
    }
    if (flags != NULL) {
      free(flags);
      flags = NULL;
    }
  } else {
    /* wait for start signal from rank 0 */
    MPI_Status status;
    MPI_Recv(&success, 1, MPI_INT, 0, 0, scr_comm_world, &status);

    /* if rank 0 hasn't seen a failure, try to read in our files */
    if (success == SCR_SUCCESS) {
      /* fetch these files into the checkpoint directory */
      if (scr_fetch_files_list(file_list, ckpt_dir, map) != SCR_SUCCESS) {
        success = SCR_FAILURE;
      }
    }

    /* tell rank 0 that we're done and send him our success code */
    MPI_Send(&success, 1, MPI_INT, 0, 0, scr_comm_world);
  }

  /* determine whether all processes successfully read their files */
  if (scr_alltrue(success == SCR_SUCCESS)) {
    return SCR_SUCCESS;
  }
  return SCR_FAILURE;
}

/* fetch files from parallel file system */
static int scr_fetch_files(scr_filemap* map, char* fetch_dir, int* dataset_id, int* checkpoint_id)
{
  /* this may take a while, so tell user what we're doing */
  if (scr_my_rank_world == 0) {
    scr_dbg(1, "Attempting fetch from %s", fetch_dir);
  }

  /* make sure all processes make it this far before progressing */
  MPI_Barrier(scr_comm_world);

  /* start timer */
  time_t timestamp_start;
  double time_start;
  if (scr_my_rank_world == 0) {
    timestamp_start = scr_log_seconds();
    time_start = MPI_Wtime();
  }

  /* broadcast fetch directory */
  int dirsize = 0;
  if (scr_my_rank_world == 0) {
    dirsize = strlen(fetch_dir) + 1;
  }
  MPI_Bcast(&dirsize, 1, MPI_INT, 0, scr_comm_world);
  MPI_Bcast(fetch_dir, dirsize, MPI_CHAR, 0, scr_comm_world);

  /* if there is no directory, bail out with failure */
  if (strcmp(fetch_dir, "") == 0) {
    return SCR_FAILURE;
  }

  /* log the fetch attempt */
  if (scr_my_rank_world == 0) {
    if (scr_log_enable) {
      time_t now = scr_log_seconds();
      scr_log_event("FETCH STARTED", fetch_dir, NULL, &now, NULL);
    }
  }

  /* allocate a new hash to get a list of files to fetch */
  scr_hash* file_list = scr_hash_new();

  /* read the summary file */
  if (scr_fetch_summary(fetch_dir, file_list) != SCR_SUCCESS) {
    if (scr_my_rank_world == 0) {
      scr_dbg(1, "Failed to read summary file @ %s:%d", __FILE__, __LINE__);
      if (scr_log_enable) {
        double time_end = MPI_Wtime();
        double time_diff = time_end - time_start;
        time_t now = scr_log_seconds();
        scr_log_event("FETCH FAILED", fetch_dir, NULL, &now, &time_diff);
      }
    }
    scr_hash_delete(file_list);
    return SCR_FAILURE;
  }

  /* get a pointer to the dataset */
  scr_dataset* dataset = scr_hash_get(file_list, SCR_KEY_DATASET);

  /* get the dataset id */
  int id;
  if (scr_dataset_get_id(dataset, &id) != SCR_SUCCESS) {
    if (scr_my_rank_world == 0) {
      scr_dbg(1, "Invalid id in summary file @ %s:%d", __FILE__, __LINE__);
      if (scr_log_enable) {
        double time_end = MPI_Wtime();
        double time_diff = time_end - time_start;
        time_t now = scr_log_seconds();
        scr_log_event("FETCH FAILED", fetch_dir, NULL, &now, &time_diff);
      }
    }
    scr_hash_delete(file_list);
    return SCR_FAILURE;
  }

  /* get the checkpoint id for this dataset */
  int ckpt_id;
  if (scr_dataset_get_ckpt(dataset, &ckpt_id) != SCR_SUCCESS) {
    /* eventually, we'll support reading of non-checkpoint datasets, but we don't yet */
    scr_err("Failed to read checkpoint id from dataset @ %s:%d",
            __FILE__, __LINE__
    );
    scr_hash_delete(file_list);
    return SCR_FAILURE;
  }

  /* delete any existing files for this dataset id (do this before filemap_read) */
  scr_cache_delete(map, id);

  /* get the redundancy descriptor for this id */
  struct scr_reddesc* c = scr_ckptdesc_get(ckpt_id, scr_nreddescs, scr_reddescs);

  /* store our redundancy descriptor hash in the filemap */
  scr_hash* my_desc_hash = scr_hash_new();
  scr_reddesc_store_to_hash(c, my_desc_hash);
  scr_filemap_set_desc(map, id, scr_my_rank_world, my_desc_hash);
  scr_hash_delete(my_desc_hash);

  /* write the filemap out before creating the directory */
  scr_filemap_write(scr_map_file, map);

  /* create the cache directory */
  scr_cache_dir_create(c, id);

  /* get the cache directory */
  char cache_dir[SCR_MAX_FILENAME];
  scr_cache_dir_get(c, id, cache_dir);

  /* now we can finally fetch the actual files */
  int success = 1;
  if (scr_fetch_data(file_list, cache_dir, map) != SCR_SUCCESS) {
    success = 0;
  }

  /* free the hash holding the summary file data */
  scr_hash_delete(file_list);

  /* check that all processes copied their file successfully */
  if (! scr_alltrue(success)) {
    /* someone failed, so let's delete the partial checkpoint */
    scr_cache_delete(map, id);

    if (scr_my_rank_world == 0) {
      scr_dbg(1, "One or more processes failed to read its files @ %s:%d",
              __FILE__, __LINE__
      );
      if (scr_log_enable) {
        double time_end = MPI_Wtime();
        double time_diff = time_end - time_start;
        time_t now = scr_log_seconds();
        scr_log_event("FETCH FAILED", fetch_dir, &id, &now, &time_diff);
      }
    }
    return SCR_FAILURE;
  }

  /* apply redundancy scheme */
  double bytes_copied = 0.0;
  int rc = scr_copy_files(map, c, id, &bytes_copied);
  if (rc == SCR_SUCCESS) {
    /* record dataset and checkpoint ids */
    *dataset_id = id;
    *checkpoint_id = ckpt_id;

    /* update our flush file to indicate this checkpoint is in cache as well as the parallel file system */
    /* TODO: should we place SCR_FLUSH_KEY_LOCATION_PFS before scr_copy_files? */
    scr_flush_file_location_set(id, SCR_FLUSH_KEY_LOCATION_CACHE);
    scr_flush_file_location_set(id, SCR_FLUSH_KEY_LOCATION_PFS);
    scr_flush_file_location_unset(id, SCR_FLUSH_KEY_LOCATION_FLUSHING);
  } else {
    /* something went wrong, so delete this checkpoint from the cache */
    scr_cache_delete(scr_map, id);
  }

  /* stop timer, compute bandwidth, and report performance */
  double total_bytes = bytes_copied;
  if (scr_my_rank_world == 0) {
    double time_end = MPI_Wtime();
    double time_diff = time_end - time_start;
    double bw = total_bytes / (1024.0 * 1024.0 * time_diff);
    scr_dbg(1, "scr_fetch_files: %f secs, %e bytes, %f MB/s, %f MB/s per proc",
            time_diff, total_bytes, bw, bw/scr_ranks_world
    );

    /* log data on the fetch to the database */
    if (scr_log_enable) {
      time_t now = scr_log_seconds();
      if (rc == SCR_SUCCESS) {
        scr_log_event("FETCH SUCCEEDED", fetch_dir, &id, &now, &time_diff);
      } else {
        scr_log_event("FETCH FAILED", fetch_dir, &id, &now, &time_diff);
      }

      char cache_dir[SCR_MAX_FILENAME];
      scr_cache_dir_get(c, id, cache_dir);
      scr_log_transfer("FETCH", fetch_dir, cache_dir, &id,
                       &timestamp_start, &time_diff, &total_bytes
      );
    }
  }

  return rc;
}

/* returns true if the named file needs to be flushed, 0 otherwise */
static int scr_bool_flush_file(const scr_filemap* map, int dset, int rank, const char* file)
{
  /* assume we need to flush this file */
  int flush = 1;

  /* read meta info for file */
  scr_meta* meta = scr_meta_new();
  if (scr_filemap_get_meta(map, dset, rank, file, meta) == SCR_SUCCESS) {
    /* don't flush XOR files */
    if (scr_meta_check_filetype(meta, SCR_META_FILE_XOR) == SCR_SUCCESS) {
      flush = 0;
    }
  } else {
    /* TODO: print error */
  }
  scr_meta_delete(meta);

  return flush;
}

/* fills in hash with a list of filenames and associated meta data that should be flushed for specified dataset id */
static int scr_flush_build_list(scr_filemap* map, int id, scr_hash* file_list)
{
  int rc = SCR_SUCCESS;

  /* lookup dataset info and make a copy in the file list */
  scr_dataset* dataset = scr_hash_new();
  scr_filemap_get_dataset(map, id, scr_my_rank_world, dataset);
  scr_hash_set(file_list, SCR_KEY_DATASET, dataset);

  /* identify which files we need to flush as part of the specified dataset id */
  scr_hash_elem* elem = NULL;
  for (elem = scr_filemap_first_file(map, id, scr_my_rank_world);
       elem != NULL;
       elem = scr_hash_elem_next(elem))
  {
    /* get the filename */
    char* file = scr_hash_elem_key(elem);

    /* read meta data for file and attach it to file list */
    scr_meta* meta = scr_meta_new();
    if (scr_filemap_get_meta(map, id, scr_my_rank_world, file, meta) == SCR_SUCCESS) {
      /* don't flush XOR files */
      int flush = 1;
      if (scr_meta_check_filetype(meta, SCR_META_FILE_XOR) == SCR_SUCCESS) {
        flush = 0;
      }

      /* if we need to flush this file, add it to the list and attach its meta data */
      if (flush) {
        scr_hash* file_hash = scr_hash_set_kv(file_list, SCR_FLUSH_KEY_FILE, file);
        scr_hash* meta_hash = scr_hash_set(file_hash, SCR_FLUSH_KEY_META, meta);
        meta = NULL;
      }
    } else {
      /* TODO: print error */
      rc = SCR_FAILURE;
    }

    /* if we didn't attach the meta data, we need to delete it */
    if (meta != NULL) {
      scr_meta_delete(meta);
    }
  }

  return rc;
}

static int scr_dataset_build_name(int id, int64_t usecs, char* name, int n)
{
#if 0
  /* format timestamp */
  char timestamp[SCR_MAX_FILENAME];
  time_t now = (time_t) (usecs / (int64_t) 1000000);
  strftime(timestamp, sizeof(timestamp), "%Y-%m-%d_%H:%M:%S", localtime(&now));

  /* build the directory name */
  char dirname[SCR_MAX_FILENAME];
  snprintf(name, n, "scr.%s.%s.%d", timestamp, scr_jobid, id);
#endif

  /* build the directory name */
  snprintf(name, n, "scr.dataset.%d", id);

  return SCR_SUCCESS;
}

/* create and return the name of a subdirectory under the prefix directory for the specified dataset */
static int scr_flush_dir_create(const scr_dataset* dataset, char* dir, int n)
{
  /* set dir to an empty string to indicate failure */
  strcpy(dir, "");
  int dirsize = 0;

  /* have rank 0 create the checkpoint directory */
  if (scr_my_rank_world == 0) {
    /* get the id of the dataset */
    int id;
    if (scr_dataset_get_id(dataset, &id) == SCR_SUCCESS) {
      /* get the name of the dataset */
      char* name;
      if (scr_dataset_get_name(dataset, &name) == SCR_SUCCESS) {
        /* build the directory name */
        if (scr_build_path(dir, n, scr_par_prefix, name) == SCR_SUCCESS) {
          /* add the directory to our index file, and record the flush timestamp */
          scr_hash* index_hash = scr_hash_new();
          scr_index_read(scr_par_prefix, index_hash);
          scr_index_set_dataset(index_hash, dataset, 0);
          scr_index_add_dir(index_hash, id, name);
          scr_index_mark_flushed(index_hash, id, name);
          scr_index_write(scr_par_prefix, index_hash);
          scr_hash_delete(index_hash);

          /* create the directory, set dir to an empty string if mkdir fails */
          if (scr_mkdir(dir, S_IRWXU) == SCR_SUCCESS) {
            scr_dbg(1, "Flushing to %s", dir);
          } else {
            /* failed to create the directory */
            scr_err("Failed to make checkpoint directory mkdir(%s) %m errno=%d @ %s:%d",
                    dir, errno, __FILE__, __LINE__
            );

            /* set dir to an empty string to indicate failure */
            strcpy(dir, "");
          }
        } else {
          /* failed to build directory name, set dir to an empty string to indicate failure */
          strcpy(dir, "");
        }

        /* compute the size of the directory string, including the terminating NULL character */
        dirsize = strlen(dir) + 1;
      }
    }
  }

  /* broadcast the directory name from rank 0 */
  MPI_Bcast(&dirsize, 1, MPI_INT, 0, scr_comm_world);
  MPI_Bcast(dir, dirsize, MPI_CHAR, 0, scr_comm_world);

  /* check whether directory was created ok, and bail out if not */
  if (strcmp(dir, "") == 0) {
    return SCR_FAILURE;
  }

  /* otherwise, the directory was created, and we're good to go */
  return SCR_SUCCESS;
}

/* create and return the name of a subdirectory under the prefix directory for the specified checkpoint id */
static int scr_flush_dir_create_old(int checkpoint_id, char* dir)
{
  /* have rank 0 create the checkpoint directory */
  int dirsize = 0;
  if (scr_my_rank_world == 0) {
    /* get the current time */
    time_t now;
    now = time(NULL);

    /* format timestamp */
    char timestamp[SCR_MAX_FILENAME];
    strftime(timestamp, sizeof(timestamp), "%Y-%m-%d_%H:%M:%S", localtime(&now));

    /* build the directory name */
    char dirname[SCR_MAX_FILENAME];
    sprintf(dirname, "scr.%s.%s.%d", timestamp, scr_jobid, checkpoint_id);

    /* add the directory to our index file, and record the flush timestamp */
    scr_hash* index_hash = scr_hash_new();
    scr_index_read(scr_par_prefix, index_hash);
    scr_index_add_dir(index_hash, checkpoint_id, dirname);
    scr_index_mark_flushed(index_hash, checkpoint_id, dirname);
    scr_index_write(scr_par_prefix, index_hash);
    scr_hash_delete(index_hash);

    /* create the directory, set dir to an empty string if mkdir fails */
    sprintf(dir, "%s/%s", scr_par_prefix, dirname);
    if (scr_mkdir(dir, S_IRWXU) != SCR_SUCCESS) {
      /* failed to create the directory */
      scr_err("Failed to make checkpoint directory mkdir(%s) %m errno=%d @ %s:%d",
              dir, errno, __FILE__, __LINE__
      );

      /* set dir to an empty string to indicate failure */
      strcpy(dir, "");
    }

    /* compute the size of the directory string, including the terminating NULL character */
    dirsize = strlen(dir) + 1;
  }

  /* broadcast the directory name from rank 0 */
  MPI_Bcast(&dirsize, 1, MPI_INT, 0, scr_comm_world);
  MPI_Bcast(dir, dirsize, MPI_CHAR, 0, scr_comm_world);

  /* check whether directory was created ok, and bail out if not */
  if (strcmp(dir, "") == 0) {
    return SCR_FAILURE;
  }

  /* otherwise, the directory was created, and we're good to go */
  return SCR_SUCCESS;
}

/* create all directories needed for file list */
static int scr_flush_create_dirs(const scr_hash* file_list)
{
  /* TODO: if preserving user-defined directories, we need to create the directories here */
  if (scr_preserve_user_directories) {
#ifdef HAVE_LIBGCS
    /* count the number of files that we need to flush */
    int count = scr_hash_size(file_list);

    /* allocate buffers to hold the directory needed for each file */
    char** dirs = NULL;
    int* leaders = NULL;
    if (count > 0) {
      dirs    = (char**) malloc(sizeof(char*) * count);
      leaders = (int*)   malloc(sizeof(int)   * count);
      /* TODO: check for allocation error */
    }

    /* lookup directory from meta data for each file */
    int i = 0;
    scr_hash_elem* elem = NULL;
    scr_hash* files = scr_hash_get(file_list, SCR_FLUSH_KEY_FILE);
    for (elem = scr_hash_elem_first(files);
         elem != NULL;
         elem = scr_hash_elem_next(elem))
    {
      /* get meta data for this file */
      scr_hash* hash = scr_hash_elem_hash(elem);
      scr_meta* meta = scr_hash_get(hash, SCR_FLUSH_KEY_META);

      dirs[i] = NULL;
      char* dir;
      if (scr_meta_get_origpath(meta, &dir) == SCR_SUCCESS) {
        dirs[i] = dir;
      } else {
        /* TODO: error */
      }

      /* add one to our file count */
      i++;
    }

    /* select leaders */
    GCS_Select_leaders_strings((void*) dirs, count, leaders, scr_comm_world);

    /* have leaders issue mkdir */
    int success = 1;
    for (i = 0; i < count; i++) {
      if (leaders[i]) {
        if (scr_mkdir(dirs[i], S_IRWXU) != SCR_SUCCESS) {
          success = 0;
        }
      }
    }

    /* free buffers */
    if (leaders != NULL) {
      free(leaders);
      leaders = NULL;
    }
    if (dirs != NULL) {
      free(dirs);
      dirs = NULL;
    }

    /* TODO: need to track directory names in summary file so we can delete them later */

    /* determine whether all leaders successfully created their directories */
    if (! scr_alltrue((success == 1))) {
      return SCR_FAILURE;
    }
    return SCR_SUCCESS;
#else
    return SCR_FAILURE;
#endif
  } else {
    /* get the dataset for this list of files */
    scr_dataset* dataset = scr_hash_get(file_list, SCR_KEY_DATASET);

    /* create the dataset directory */
    char dir[SCR_MAX_FILENAME];
    if (scr_flush_dir_create(dataset, dir, sizeof(dir)) != SCR_SUCCESS) {
      return SCR_FAILURE;
    }

    /* add the flush directory to each file in the list */
    scr_hash_elem* elem = NULL;
    scr_hash* files = scr_hash_get(file_list, SCR_FLUSH_KEY_FILE);
    for (elem = scr_hash_elem_first(files);
         elem != NULL;
         elem = scr_hash_elem_next(elem))
    {
      /* get meta data for this file */
      scr_hash* hash = scr_hash_elem_hash(elem);
      scr_hash_util_set_str(hash, SCR_FLUSH_KEY_PATH, dir);
    }

    return SCR_SUCCESS;
  }
}

static int scr_container_construct_name(const scr_dataset* dataset, int id, char* file, int len)
{
  /* check that we have a dataset and a buffer to write the name to */
  if (dataset == NULL || file == NULL) {
    return SCR_FAILURE;
  }

  /* get the name of the dataset */
  char* name;
  if (scr_dataset_get_name(dataset, &name) != SCR_SUCCESS) {
    return SCR_FAILURE;
  }

  /* build the name and check that it's not truncated */
  int n = snprintf(file, len, "%s/%s/ctr.%d.scr", scr_par_prefix, name, id);
  if (n >= len) {
    /* we truncated the container name */
    return SCR_FAILURE;
  }

  return SCR_SUCCESS;
}

/* identify the container to write each file to */
static int scr_flush_identify_containers(scr_hash* file_list)
{
  int rc = SCR_SUCCESS;

  /* get the maximum container size */
  unsigned long container_size = scr_container_size;

  /* get the dataset for the file list */
  scr_dataset* dataset = scr_hash_get(file_list, SCR_KEY_DATASET);

  /* compute total number of bytes we'll flush on this process */
  unsigned long my_bytes = 0;
  scr_hash_elem* elem = NULL;
  scr_hash* files = scr_hash_get(file_list, SCR_FLUSH_KEY_FILE);
  for (elem = scr_hash_elem_first(files);
       elem != NULL;
       elem = scr_hash_elem_next(elem))
  {
    /* get meta data for this file */
    scr_hash* hash = scr_hash_elem_hash(elem);
    scr_meta* meta = scr_hash_get(hash, SCR_FLUSH_KEY_META);

    /* get filesize from meta data */
    unsigned long filesize;
    if (scr_meta_get_filesize(meta, &filesize) == SCR_SUCCESS) {
      my_bytes += filesize;
    } else {
       /* TODO: error */
      rc = SCR_FAILURE;
    }
  }

  /* compute total number of bytes we need to write across all processes */
  unsigned long total_bytes;
  MPI_Allreduce(&my_bytes, &total_bytes, 1, MPI_UNSIGNED_LONG, MPI_SUM, scr_comm_world);

  /* compute total number of bytes we need to write on the node */
  unsigned long local_bytes;
  MPI_Reduce(&my_bytes, &local_bytes, 1, MPI_UNSIGNED_LONG, MPI_SUM, 0, scr_comm_local);

  /* compute offset for each node */
  unsigned long local_offset = 0;
  if (scr_my_rank_local == 0) {
    MPI_Scan(&local_bytes, &local_offset, 1, MPI_UNSIGNED_LONG, MPI_SUM, scr_comm_level);
    local_offset -= local_bytes;
  }

  /* compute offset for each process,
   * note that local_offset == 0 for all procs on the node except for scr_my_rank_local == 0,
   * which contains the offset for the node */
  unsigned long my_offset = 0;
  local_offset += my_bytes;
  MPI_Scan(&local_offset, &my_offset, 1, MPI_UNSIGNED_LONG, MPI_SUM, scr_comm_local);
  my_offset -= my_bytes;

  /* compute offset for each file on this process */
  unsigned long file_offset = my_offset;
  for (elem = scr_hash_elem_first(files);
       elem != NULL;
       elem = scr_hash_elem_next(elem))
  {
    /* get meta data for this file */
    scr_hash* hash = scr_hash_elem_hash(elem);
    scr_meta* meta = scr_hash_get(hash, SCR_FLUSH_KEY_META);

    /* get filesize from meta data */
    unsigned long filesize;
    if (scr_meta_get_filesize(meta, &filesize) == SCR_SUCCESS) {
      /* compute container id, offset, and length */
      int file_segment = 0;
      unsigned long remaining = filesize;
      while (remaining > 0) {
        /* compute container id, offset, and length */
        int container_id = file_offset / container_size;
        unsigned long container_offset = file_offset - (container_id * container_size);
        unsigned long container_length = container_size - container_offset;
        if (container_length > remaining) {
          container_length = remaining;
        }

        /* store segment length, container id, and container offset under new file segment */
        scr_hash* segment_hash = scr_hash_set_kv_int(hash, SCR_SUMMARY_6_KEY_SEGMENT, file_segment);
        scr_hash_util_set_bytecount(segment_hash, SCR_SUMMARY_6_KEY_LENGTH, container_length);
        scr_hash* container_hash = scr_hash_new();
        scr_hash_util_set_int(container_hash, SCR_SUMMARY_6_KEY_ID, container_id);
        scr_hash_util_set_bytecount(container_hash, SCR_SUMMARY_6_KEY_OFFSET, container_offset);
        scr_hash_set(segment_hash, SCR_SUMMARY_6_KEY_CONTAINER, container_hash);

        /* add entry for container name in the file list */
        scr_hash* details = scr_hash_set_kv_int(file_list, SCR_SUMMARY_6_KEY_CONTAINER, container_id);

        /* compute name of container */
        char container_name[SCR_MAX_FILENAME];
        scr_container_construct_name(dataset, container_id, container_name, sizeof(container_name));
        scr_hash_util_set_str(details, SCR_KEY_NAME, container_name);

        /* compute size of container */
        unsigned long size = container_size;
        if ((container_id+1) * container_size > total_bytes) {
          size = total_bytes - (container_id * container_size);
        }
        scr_hash_util_set_bytecount(details, SCR_SUMMARY_6_KEY_SIZE, size);

        /* move on to the next file segment */
        remaining   -= container_length;
        file_offset += container_length;
        file_segment++;
      }
    } else {
      /* TODO: error */
      rc = SCR_FAILURE;
    }
  }

  /* determine whether all processes successfully computed their containers */
  if (! scr_alltrue((rc == SCR_SUCCESS))) {
    return SCR_FAILURE;
  }
  return SCR_SUCCESS;
}

/* flushes file named in src_file to dst_dir and fills in meta based on flush,
 * returns success of flush */
static int scr_flush_a_file(const char* file, const char* dir, scr_meta* meta)
{
  int flushed = SCR_SUCCESS;
  int tmp_rc;

  /* break file into path and name components */
  char path[SCR_MAX_FILENAME];
  char name[SCR_MAX_FILENAME];
  scr_split_path(file, path, name);

  /* copy file */
  int crc_valid = 0;
  uLong crc;
  uLong* crc_p = NULL;
  if (scr_crc_on_flush) {
    crc_valid = 1;
    crc_p = &crc;
  }
  char my_flushed_file[SCR_MAX_FILENAME];
  tmp_rc = scr_copy_to(file, dir, scr_file_buf_size,
                       my_flushed_file, sizeof(my_flushed_file), crc_p
  );
  if (tmp_rc != SCR_SUCCESS) {
    crc_valid = 0;
    flushed = SCR_FAILURE;
  }
  scr_dbg(2, "scr_flush_a_file: Read and copied %s to %s with success code %d @ %s:%d",
          file, my_flushed_file, tmp_rc, __FILE__, __LINE__
  );

  /* if file has crc32, check it against the one computed during the copy,
   * otherwise if scr_crc_on_flush is set, record crc32 */
  if (crc_valid) {
    uLong crc_meta;
    if (scr_meta_get_crc32(meta, &crc_meta) == SCR_SUCCESS) {
      if (crc != crc_meta) { 
        /* detected a crc mismatch during the copy */

        /* TODO: unlink the copied file */
        /* unlink(my_flushed_file); */

        /* mark the file as invalid */
        scr_meta_set_complete(meta, 0);

        flushed = SCR_FAILURE;
        scr_err("scr_flush_a_file: CRC32 mismatch detected when flushing file %s to %s @ %s:%d",
                file, my_flushed_file, __FILE__, __LINE__
        );

        /* TODO: would be good to log this, but right now only rank 0 can write log entries */
        /*
        if (scr_log_enable) {
          time_t now = scr_log_seconds();
          scr_log_event("CRC32 MISMATCH", my_flushed_file, NULL, &now, NULL);
        }
        */
      }
    } else {
      /* the crc was not already in the metafile, but we just computed it, so set it */
      scr_meta_set_crc32(meta, crc);
    }
  }

  /* TODO: check that written filesize matches expected filesize */

  /* fill out meta data, set complete field based on flush success */
  /* (we don't update the meta file here, since perhaps the file in cache is ok and only the flush failed) */
  int complete = (flushed == SCR_SUCCESS);
  scr_meta_set_complete(meta, complete);

  return flushed;
}

static int    scr_flush_async_in_progress = 0;       /* tracks whether an async flush is currently underway */
static int    scr_flush_async_checkpoint_id = -1;    /* tracks the id of the checkpoint being flushed */
static time_t scr_flush_async_timestamp_start;       /* records the time the async flush started */
static double scr_flush_async_time_start;            /* records the time the async flush started */
static char   scr_flush_async_dir[SCR_MAX_FILENAME]; /* records the directory the async flush is writing to */
static scr_hash* scr_flush_async_hash = NULL; /* tracks list of files written with flush */
static double scr_flush_async_bytes = 0.0;           /* records the total number of bytes to be flushed */
static int    scr_flush_async_num_files = 0;         /* records the number of files this process must flush */

/* queues file to flushed to dst_dir in hash, returns size of file in bytes */
static int scr_flush_async_file_enqueue(scr_hash* hash, const char* file, const char* dst_dir, double* bytes)
{
#if 0
  /* start with 0 bytes written for this file */
  *bytes = 0.0;

  /* break file into path and name components */
  char path[SCR_MAX_FILENAME];
  char name[SCR_MAX_FILENAME];
  scr_split_path(file, path, name);

  /* create dest_file using dest_dir and name */
  char dest_file[SCR_MAX_FILENAME];
  scr_build_path(dest_file, sizeof(dest_file), dst_dir, name);

  /* look up the filesize of the file */
  unsigned long filesize = scr_filesize(file);

  /* add this file to the hash, and add its filesize to the number of bytes written */
  scr_hash* file_hash = scr_hash_set_kv(hash, SCR_TRANSFER_KEY_FILES, file);
  if (file_hash != NULL) {
    scr_hash_util_set_str(file_hash, "DESTINATION", dest_file);
    scr_hash_util_set_bytecount(file_hash, "SIZE",    filesize);
    scr_hash_util_set_bytecount(file_hash, "WRITTEN", 0);
  }
  *bytes += (double) filesize;
 
  /* get meta data file name for file */
  char metafile[SCR_MAX_FILENAME];
  scr_meta_name(metafile, file);

  /* look up the filesize of the metafile */
  unsigned long metasize = scr_filesize(metafile);

  /* break file into path and name components */
  char metapath[SCR_MAX_FILENAME];
  char metaname[SCR_MAX_FILENAME];
  scr_split_path(metafile, metapath, metaname);

  /* create dest_file using dest_dir and name */
  char dest_metafile[SCR_MAX_FILENAME];
  scr_build_path(dest_metafile, sizeof(dest_metafile), dst_dir, metaname);

  /* add the metafile to the transfer hash, and add its filesize to the number of bytes written */
  file_hash = scr_hash_set_kv(hash, SCR_TRANSFER_KEY_FILES, metafile);
  if (file_hash != NULL) {
    scr_hash_util_set_str(file_hash, "DESTINATION", dest_metafile);
    scr_hash_util_set_bytecount(file_hash, "SIZE",    metasize);
    scr_hash_util_set_bytecount(file_hash, "WRITTEN", 0);
  }
  *bytes += (double) metasize;
#endif
 
  return SCR_SUCCESS;
}

/* given a hash, test whether the files in that hash have completed their flush */
static int scr_flush_async_file_test(const scr_hash* hash, double* bytes)
{
#if 0
  /* initialize bytes to 0 */
  *bytes = 0.0;

  /* get the FILES hash */
  scr_hash* files_hash = scr_hash_get(hash, SCR_TRANSFER_KEY_FILES);
  if (files_hash == NULL) {
    /* can't tell whether this flush has completed */
    return SCR_FAILURE;
  }

  /* assume we're done, look for a file that says we're not */
  int transfer_complete = 1;

  /* for each file, check whether the WRITTEN field matches the SIZE field,
   * which indicates the file has completed its transfer */
  scr_hash_elem* elem;
  for (elem = scr_hash_elem_first(files_hash);
       elem != NULL;
       elem = scr_hash_elem_next(elem))
  {
    /* get the file name */
    char* file = scr_hash_elem_key(elem);

    /* get the hash for this file */
    scr_hash* file_hash = scr_hash_elem_hash(elem);
    if (file_hash == NULL) {
      transfer_complete = 0;
      continue;
    }

    /* check whether this file is complete */
    /* lookup the strings for the size and bytes written */
    char* size    = scr_hash_elem_get_first_val(file_hash, "SIZE");
    char* written = scr_hash_elem_get_first_val(file_hash, "WRITTEN");
    if (size == NULL || written == NULL) {
      transfer_complete = 0;
      continue;
    }
  
    /* convert the size and bytes written strings to numbers */
    off_t size_count    = strtoul(size,    NULL, 0);
    off_t written_count = strtoul(written, NULL, 0);
    if (written_count < size_count) {
      transfer_complete = 0;
    }

    /* add up number of bytes written */
    *bytes += (double) written_count;
  }

  /* return our decision */
  if (transfer_complete) {
    return SCR_SUCCESS;
  }
  return SCR_FAILURE;
#endif
  return SCR_SUCCESS;
}

/* dequeues files listed in hash2 from hash1 */
static int scr_flush_async_file_dequeue(scr_hash* hash1, scr_hash* hash2)
{
#if 0
  /* for each file listed in hash2, remove it from hash1 */
  scr_hash* file_hash = scr_hash_get(hash2, SCR_TRANSFER_KEY_FILES);
  if (file_hash != NULL) {
    scr_hash_elem* elem;
    for (elem = scr_hash_elem_first(file_hash);
         elem != NULL;
         elem = scr_hash_elem_next(elem))
    {
      /* get the filename, and dequeue it */
      char* file = scr_hash_elem_key(elem);
      scr_hash_unset_kv(hash1, SCR_TRANSFER_KEY_FILES, file);

      /* get meta data file name for file, and dequeue it */
      char metafile[SCR_MAX_FILENAME];
      scr_meta_name(metafile, file);
      scr_hash_unset_kv(hash1, SCR_TRANSFER_KEY_FILES, metafile);
    }
  }
#endif
  return SCR_SUCCESS;
}

/* start an asynchronous flush from cache to parallel file system under SCR_PREFIX */
static int scr_flush_async_start(scr_filemap* map, int id)
{
#if 0
  int tmp_rc;

  /* if user has disabled flush, return failure */
  if (scr_flush <= 0) {
    return SCR_FAILURE;
  }

  /* if we don't need a flush, return right away with success */
  if (! scr_bool_need_flush(id)) {
    return SCR_SUCCESS;
  }

  /* if scr_par_prefix is not set, return right away with an error */
  if (strcmp(scr_par_prefix, "") == 0) {
    return SCR_FAILURE;
  }

  /* this may take a while, so tell user what we're doing */
  if (scr_my_rank_world == 0) {
    scr_dbg(1, "scr_flush_async_start: Initiating flush of dataset %d", id);
  }

  /* make sure all processes make it this far before progressing */
  MPI_Barrier(scr_comm_world);

  /* start timer */
  if (scr_my_rank_world == 0) {
    scr_flush_async_timestamp_start = scr_log_seconds();
    scr_flush_async_time_start = MPI_Wtime();

    /* log the start of the flush */
    if (scr_log_enable) {
      scr_log_event("ASYNC FLUSH STARTED", NULL, &id, &scr_flush_async_timestamp_start, NULL);
    }
  }

  /* mark that we've started a flush */
  scr_flush_async_in_progress = 1;
  scr_flush_async_checkpoint_id = id;
  scr_flush_file_location_set(id, SCR_FLUSH_KEY_LOCATION_FLUSHING);

  /* get a new hash to record our file list */
  scr_flush_async_hash = scr_hash_new();
  scr_flush_async_num_files = 0;
  scr_flush_async_bytes = 0.0;

  /* read in the filemap to get the dataset file names */
  int have_files = 1;
  if (have_files && (scr_cache_check_files(map, id) != SCR_SUCCESS)) {
    scr_err("scr_flush_async_start: One or more files is missing @ %s:%d", __FILE__, __LINE__);
    have_files = 0;
  }
  if (!scr_alltrue(have_files)) {
    if (scr_my_rank_world == 0) {
      scr_err("scr_flush_async_start: One or more processes are missing their files @ %s:%d",
              __FILE__, __LINE__
      );
      if (scr_log_enable) {
        double time_end = MPI_Wtime();
        double time_diff = time_end - scr_flush_async_time_start;
        time_t now = scr_log_seconds();
        scr_log_event("ASYNC FLUSH FAILED", "Missing files in cache", &id, &now, &time_diff);
      }
    }

    /* TODO: could lead to a memory leak in scr_flush_async_hash */

    return SCR_FAILURE;
  }

  /* create the dataset directory */
  if (scr_flush_dir_create_old(id, scr_flush_async_dir) != SCR_SUCCESS) {
    if (scr_my_rank_world == 0) {
      scr_err("scr_flush_async_start: Failed to create dataset directory @ %s:%d",
              __FILE__, __LINE__
      );
      if (scr_log_enable) {
        double time_end = MPI_Wtime();
        double time_diff = time_end - scr_flush_async_time_start;
        time_t now = scr_log_seconds();
        scr_log_event("ASYNC FLUSH FAILED", "Failed to create directory", &id, &now, &time_diff);
      }
    }

    /* TODO: could lead to a memory leak in scr_flush_async_hash */

    return SCR_FAILURE;
  }
  if (scr_my_rank_world == 0) {
    scr_dbg(1, "scr_flush_async_start: Flushing to %s", scr_flush_async_dir);
  }

  /* add each of my files to the transfer file list */
  double my_bytes = 0.0;
  scr_hash_elem* elem;
  for (elem = scr_filemap_first_file(map, id, scr_my_rank_world);
       elem != NULL;
       elem = scr_hash_elem_next(elem))
  {
    /* get the filename */
    char* file = scr_hash_elem_key(elem);

    /* enqueue this file, and add its byte count to the total */
    if (scr_bool_flush_file(map, id, scr_my_rank_world, file)) {
      double file_bytes = 0.0;
      scr_flush_async_file_enqueue(scr_flush_async_hash, file, scr_flush_async_dir, &file_bytes);
      my_bytes += file_bytes;
      scr_flush_async_num_files++;
    }
  }

  /* have master on each node write the transfer file, everyone else sends data to him */
  if (scr_my_rank_local == 0) {
    /* receive hash data from other processes on the same node and merge with our data */
    int i;
    for (i=1; i < scr_ranks_local; i++) {
      scr_hash* h = scr_hash_new();
      scr_hash_recv(h, i, scr_comm_local);
      scr_hash_merge(scr_flush_async_hash, h);
      scr_hash_delete(h);
    }

    /* get a hash to store file data */
    scr_hash* hash = scr_hash_new();

    /* open transfer file with lock */
    int fd = -1;
    scr_hash_lock_open_read(scr_transfer_file, &fd, hash);

    /* merge our data to the file data */
    scr_hash_merge(hash, scr_flush_async_hash);

    char* value = NULL;

    /* set BW if it's not already set */
    value = scr_hash_elem_get_first_val(hash, SCR_TRANSFER_KEY_BW);
    if (value == NULL) {
      double bw = (double) scr_flush_async_bw / (double) scr_ranks_level;
      scr_hash_unset(hash, SCR_TRANSFER_KEY_BW);
      scr_hash_setf(hash, NULL, "%s %f", SCR_TRANSFER_KEY_BW, bw);
    }

    /* set PERCENT if it's not already set */
    value = scr_hash_elem_get_first_val(hash, SCR_TRANSFER_KEY_PERCENT);
    if (value == NULL) {
      scr_hash_unset(hash, SCR_TRANSFER_KEY_PERCENT);
      scr_hash_setf(hash, NULL, "%s %f", SCR_TRANSFER_KEY_PERCENT, scr_flush_async_percent);
    }

    /* set the RUN command */
    scr_hash_unset(hash, SCR_TRANSFER_KEY_COMMAND);
    scr_hash_set_kv(hash, SCR_TRANSFER_KEY_COMMAND, SCR_TRANSFER_KEY_COMMAND_RUN);

    /* unset the DONE flag */
    scr_hash_unset_kv(hash, SCR_TRANSFER_KEY_FLAG, SCR_TRANSFER_KEY_FLAG_DONE);

    /* close the transfer file and release the lock */
    scr_hash_write_close_unlock(scr_transfer_file, &fd, hash);

    /* delete the hash */
    scr_hash_delete(hash);
  } else {
    /* send our transfer hash data to the master on this node */
    scr_hash_send(scr_flush_async_hash, 0, scr_comm_local);
  }

  /* get the total number of bytes to write */
  MPI_Allreduce(&my_bytes, &scr_flush_async_bytes, 1, MPI_DOUBLE, MPI_SUM, scr_comm_world);

  /* TODO: start transfer thread / process */

  /* make sure all processes have started before we leave */
  MPI_Barrier(scr_comm_world);
#endif
  return SCR_SUCCESS;
}

/* writes the specified command to the transfer file */
static int scr_flush_async_command_set(char* command)
{
#if 0
  /* have the master on each node write this command to the file */
  if (scr_my_rank_local == 0) {
    /* get a hash to store file data */
    scr_hash* hash = scr_hash_new();

    /* read the file */
    int fd = -1;
    scr_hash_lock_open_read(scr_transfer_file, &fd, hash);

    /* set the command */
    scr_hash_unset(hash, SCR_TRANSFER_KEY_COMMAND);
    scr_hash_set_kv(hash, SCR_TRANSFER_KEY_COMMAND, command);

    /* write the hash back */
    scr_hash_write_close_unlock(scr_transfer_file, &fd, hash);

    /* delete the hash */
    scr_hash_delete(hash);
  }
#endif
  return SCR_SUCCESS;
}

/* waits until all transfer processes are in the specified state */
static int scr_flush_async_state_wait(char* state)
{
#if 0
  /* wait until each process matches the state */
  int all_valid = 0;
  while (!all_valid) {
    /* assume we match the asked state */
    int valid = 1;

    /* have the master on each node check the state in the transfer file */
    if (scr_my_rank_local == 0) {
      /* get a hash to store file data */
      scr_hash* hash = scr_hash_new();

      /* open transfer file with lock */
      scr_hash_read_with_lock(scr_transfer_file, hash);

      /* check for the specified state */
      scr_hash* state_hash = scr_hash_get_kv(hash, SCR_TRANSFER_KEY_STATE, state);
      if (state_hash == NULL) {
        valid = 0;
      }

      /* delete the hash */
      scr_hash_delete(hash);
    }

    /* check whether everyone is at the specified state */
    if (scr_alltrue(valid)) {
      all_valid = 1;
    }

    /* if we're not there yet, sleep for sometime and they try again */
    if (!all_valid) {
      usleep(10*1000*1000);
    }
  }
#endif
  return SCR_SUCCESS;
}

/* removes all files from the transfer file */
static int scr_flush_async_file_clear_all()
{
#if 0
  /* have the master on each node clear the FILES field */
  if (scr_my_rank_local == 0) {
    /* get a hash to store file data */
    scr_hash* hash = scr_hash_new();

    /* read the file */
    int fd = -1;
    scr_hash_lock_open_read(scr_transfer_file, &fd, hash);

    /* clear the FILES entry */
    scr_hash_unset(hash, SCR_TRANSFER_KEY_FILES);

    /* write the hash back */
    scr_hash_write_close_unlock(scr_transfer_file, &fd, hash);

    /* delete the hash */
    scr_hash_delete(hash);
  }
#endif
  return SCR_SUCCESS;
}

/* stop an ongoing asynchronous flush for a specified checkpoint */
static int scr_flush_async_stop()
{
#if 0
  int tmp_rc;

  /* if user has disabled flush, return failure */
  if (scr_flush <= 0) {
    return SCR_FAILURE;
  }

  /* this may take a while, so tell user what we're doing */
  if (scr_my_rank_world == 0) {
    scr_dbg(1, "scr_flush_async_stop_all: Stopping flush");
  }

  /* write stop command to transfer file */
  scr_flush_async_command_set(SCR_TRANSFER_KEY_COMMAND_STOP);

  /* wait until all tasks know the transfer is stopped */
  scr_flush_async_state_wait(SCR_TRANSFER_KEY_STATE_STOP);

  /* remove the files list from the transfer file */
  scr_flush_async_file_clear_all();

  /* remove FLUSHING state from flush file */
  scr_flush_async_in_progress = 0;
  /*
  scr_flush_file_location_unset(id, SCR_FLUSH_KEY_LOCATION_FLUSHING);
  */

  /* clear internal flush_async variables to indicate there is no flush */
  if (scr_flush_async_hash != NULL) {
    scr_hash_delete(scr_flush_async_hash);
  }

  /* make sure all processes have made it this far before we leave */
  MPI_Barrier(scr_comm_world);
#endif
  return SCR_SUCCESS;
}

/* check whether the flush from cache to parallel file system has completed */
static int scr_flush_async_test(scr_filemap* map, int id, double* bytes)
{
#if 0
  /* initialize bytes to 0 */
  *bytes = 0.0;

  /* if user has disabled flush, return failure */
  if (scr_flush <= 0) {
    return SCR_FAILURE;
  }

  /* test that all of our files for this dataset are still here */
  int have_files = 1;
  if (have_files && (scr_cache_check_files(map, id) != SCR_SUCCESS)) {
    scr_err("scr_flush_async_test: One or more files is missing @ %s:%d", __FILE__, __LINE__);
    have_files = 0;
  }
  if (!scr_alltrue(have_files)) {
    if (scr_my_rank_world == 0) {
      scr_err("scr_flush_async_test: One or more processes are missing their files @ %s:%d",
              __FILE__, __LINE__
      );
      if (scr_log_enable) {
        double time_end = MPI_Wtime();
        double time_diff = time_end - scr_flush_async_time_start;
        time_t now = scr_log_seconds();
        scr_log_event("ASYNC FLUSH TEST FAILED", "Missing files in cache", &id, &now, &time_diff);
      }
    }
    return SCR_FAILURE;
  }

  /* assume the transfer is complete */
  int transfer_complete = 1;

  /* have master on each node check whether the flush is complete */
  double bytes_written = 0.0;
  if (scr_my_rank_local == 0) {
    /* create a hash to hold the transfer file data */
    scr_hash* hash = scr_hash_new();

    /* read transfer file with lock */
    if (scr_hash_read_with_lock(scr_transfer_file, hash) == SCR_SUCCESS) {
      /* test each file listed in the transfer hash */
      if (scr_flush_async_file_test(hash, &bytes_written) != SCR_SUCCESS) {
        transfer_complete = 0;
      }
    } else {
      /* failed to read the transfer file, can't determine whether the flush is complete */
      transfer_complete = 0;
    }

    /* free the hash */
    scr_hash_delete(hash);
  }

  /* compute the total number of bytes written */
  MPI_Allreduce(&bytes_written, bytes, 1, MPI_DOUBLE, MPI_SUM, scr_comm_world);

  /* determine whether the transfer is complete on all tasks */
  if (scr_alltrue(transfer_complete)) {
    return SCR_SUCCESS;
  }
  return SCR_FAILURE;
#endif
  return SCR_SUCCESS;
}

/* complete the flush from cache to parallel file system has completed */
static int scr_flush_async_complete(scr_filemap* map, int id)
{
#if 0
  int tmp_rc;
  int i;

  /* if user has disabled flush, return failure */
  if (scr_flush <= 0) {
    return SCR_FAILURE;
  }

  /* check that all processes still have all of their files */
  int have_files = 1;
  if (scr_cache_check_files(map, id) != SCR_SUCCESS) {
    scr_err("scr_flush_async_complete: One or more files is missing @ %s:%d", __FILE__, __LINE__);
    have_files = 0;
  }
  if (! scr_alltrue(have_files)) {
    if (scr_my_rank_world == 0) {
      scr_err("scr_flush_async_complete: One or more processes are missing their files @ %s:%d",
              __FILE__, __LINE__
      );
      if (scr_log_enable) {
        double time_end = MPI_Wtime();
        double time_diff = time_end - scr_flush_async_time_start;
        time_t now = scr_log_seconds();
        scr_log_event("ASYNC FLUSH COMPLETE FAILED", "Missing files in cache", &id, &now, &time_diff);
      }
    }
    return SCR_FAILURE;
  }

  /* TODO: have master tell each rank on node whether its files were written successfully */

  /* allocate structure to hold metadata info */
  scr_hash* data = scr_hash_new();
  if (data == NULL) {
    scr_err("scr_flush_async_complete: Failed to malloc data structure to write out summary file @ file %s:%d",
            __FILE__, __LINE__
    );
    MPI_Abort(scr_comm_world, 0);
  }

  /* set our rank */
  scr_hash* rank_hash = scr_hash_set_kv_int(data, SCR_SUMMARY_6_KEY_RANK, scr_my_rank_world);

  /* fill in metadata info for the files this process flushed */
  double total_bytes = 0.0;
  scr_hash_elem* elem = NULL;
  for (elem = scr_filemap_first_file(map, id, scr_my_rank_world);
       elem != NULL;
       elem = scr_hash_elem_next(elem))
  {
    /* get the filename */
    char* file = scr_hash_elem_key(elem);

    /* if we flushed this file, read its metadata info */
    if (scr_bool_flush_file(map, id, scr_my_rank_world, file)) {
      /* record the filename in the hash, and get reference to a hash for this file */
      char path[SCR_MAX_FILENAME];
      char name[SCR_MAX_FILENAME];
      scr_split_path(file, path, name);
      scr_hash* file_hash = scr_hash_set_kv(rank_hash, SCR_SUMMARY_6_KEY_FILE, name);

      /* read meta data for this file */
      scr_hash* flush_meta = scr_hash_new();
      scr_meta_read(file, flush_meta);

      /* TODO: check that this file was written successfully */

      /* successfully flushed this file, record the filesize */
      unsigned long flush_filesize = 0;
      if (scr_meta_get_filesize(flush_meta, &flush_filesize) == SCR_SUCCESS) {
        scr_hash_util_set_bytecount(file_hash, SCR_SUMMARY_6_KEY_SIZE, flush_filesize);
        total_bytes += (double) flush_filesize;
      }

      /* record the crc32 if one was computed */
      uLong flush_crc32;
      if (scr_meta_get_crc32(flush_meta, &flush_crc32) == SCR_SUCCESS) {
        scr_hash_util_set_crc32(file_hash, SCR_SUMMARY_6_KEY_CRC, flush_crc32);
      }

      /* delete our temporary meta data object */
      scr_hash_delete(flush_meta);
    }
  }

  /* gather metadata info from all tasks for all files to rank 0 */
  int flushed = SCR_SUCCESS;
  if (scr_my_rank_world == 0) {
    /* flow control with a sliding window of w processes */
    int w = scr_flush_width;
    if (w > scr_ranks_world-1) {
      w = scr_ranks_world-1;
    }

    /* allocate MPI_Request arrays and an array of ints */
    int*         ranks    = (int*)         malloc(w * sizeof(int));
    double*      bytes    = (double*)      malloc(w * sizeof(double));
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
        MPI_Irecv(&bytes[index], 1, MPI_DOUBLE, i, 0, scr_comm_world, &req_recv[index]);

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

      /* receive the meta data from this rank */
      scr_hash* incoming_hash = scr_hash_new();
      scr_hash_recv(incoming_hash, ranks[index], scr_comm_world);
      scr_hash_merge(data, incoming_hash);
      scr_hash_delete(incoming_hash);

      /* add the number of bytes written by this rank */
      total_bytes += bytes[index];

      /* one less request outstanding now */
      outstanding--;
    }

    /* free the MPI_Request arrays */
    if (req_send != NULL) {
      free(req_send);
      req_send = NULL;
    }
    if (req_recv != NULL) {
      free(req_recv);
      req_recv = NULL;
    }
    if (bytes != NULL) {
      free(bytes);
      bytes = NULL;
    }
    if (ranks != NULL) {
      free(ranks);
      ranks = NULL;
    }
  } else {
    /* receive signal to start */
    int start = 0;
    MPI_Status status;
    MPI_Recv(&start, 1, MPI_INT, 0, 0, scr_comm_world, &status);

    /* flush each of my files and fill in meta data structures */
    scr_hash_elem* elem = NULL;
    for (elem = scr_filemap_first_file(map, id, scr_my_rank_world);
         elem != NULL;
         elem = scr_hash_elem_next(elem))
    {
      /* get the filename */
      char* file = scr_hash_elem_key(elem);

      /* flush this file if needed */
      if (scr_bool_flush_file(map, id, scr_my_rank_world, file)) {
        /* record the filename in the hash, and get reference to a hash for this file */
        char path[SCR_MAX_FILENAME];
        char name[SCR_MAX_FILENAME];
        scr_split_path(file, path, name);
        scr_hash* file_hash = scr_hash_set_kv(rank_hash, SCR_SUMMARY_6_KEY_FILE, name);

        /* read meta data for this file */
        scr_hash* flush_meta = scr_hash_new();
        scr_meta_read(file, flush_meta);

        /* TODO: check that this file was written successfully */

        /* successfully flushed this file, record the filesize */
        unsigned long flush_filesize = 0;
        if (scr_meta_get_filesize(flush_meta, &flush_filesize) == SCR_SUCCESS) {
          scr_hash_util_set_bytecount(file_hash, SCR_SUMMARY_6_KEY_SIZE, flush_filesize);
          total_bytes += (double) flush_filesize;
        }

        /* record the crc32 if one was computed */
        uLong flush_crc32;
        if (scr_meta_get_crc32(flush_meta, &flush_crc32) == SCR_SUCCESS) {
          scr_hash_util_set_crc32(file_hash, SCR_SUMMARY_6_KEY_CRC, flush_crc32);
        }

        /* delete our temporary meta data object */
        scr_hash_delete(flush_meta);
      }
    }

    /* send total bytes to rank 0 */
    MPI_Send(&total_bytes, 1, MPI_DOUBLE, 0, 0, scr_comm_world);

    /* would be better to do this as a reduction-type of operation */
    scr_hash_send(data, 0, scr_comm_world);
  }

  /* determine whether everyone wrote their files ok */
  int all_success = scr_alltrue((flushed == SCR_SUCCESS));

  if (scr_my_rank_world == 0) {
    if (all_success) {
      /* everyone wrote their files ok, now write out summary file */
      int wrote_summary = scr_summary_write_old(scr_flush_async_dir, id, all_success, data);
      if (wrote_summary != SCR_SUCCESS) {
        flushed = SCR_FAILURE;
      }

      /* if we also wrote the summary file ok, update the current symlink */
      if (flushed == SCR_SUCCESS) {
        /* TODO: update 'flushed' depending on symlink update */

        /* build path to current symlink */
        char current[SCR_MAX_FILENAME];
        scr_build_path(current, sizeof(current), scr_par_prefix, SCR_CURRENT_LINK);

        /* if current exists, unlink it */
        if (access(current, F_OK) == 0) {
          unlink(current);
        }

        /* create new current to point to new directory */
        char target_path[SCR_MAX_FILENAME];
        char target_name[SCR_MAX_FILENAME];
        scr_split_path(scr_flush_async_dir, target_path, target_name);
        symlink(target_name, current);
      } else {
        /* someone failed to write a file */
        flushed = SCR_FAILURE;
      }
    }
  }

  /* have rank 0 broadcast whether the entire flush succeeded, including summary file and symlink update */
  MPI_Bcast(&flushed, 1, MPI_INT, 0, scr_comm_world);

  /* mark set as flushed to the parallel file system */
  if (flushed == SCR_SUCCESS) {
    scr_flush_file_location_set(id, SCR_FLUSH_KEY_LOCATION_PFS);
  }

  /* mark that we've stopped the flush */
  scr_flush_async_in_progress = 0;
  scr_flush_file_location_unset(id, SCR_FLUSH_KEY_LOCATION_FLUSHING);

  /* have master on each node remove files from the transfer file */
  if (scr_my_rank_local == 0) {
    /* get a hash to read from the file */
    scr_hash* transfer_hash = scr_hash_new();

    /* lock the transfer file, open it, and read it into the hash */
    int fd = -1;
    scr_hash_lock_open_read(scr_transfer_file, &fd, transfer_hash);

    /* remove files from the list */
    scr_flush_async_file_dequeue(transfer_hash, scr_flush_async_hash);

    /* set the STOP command */
    scr_hash_unset(transfer_hash, SCR_TRANSFER_KEY_COMMAND);
    scr_hash_set_kv(transfer_hash, SCR_TRANSFER_KEY_COMMAND, SCR_TRANSFER_KEY_COMMAND_STOP);

    /* write the hash back to the file */
    scr_hash_write_close_unlock(scr_transfer_file, &fd, transfer_hash);

    /* delete the hash */
    scr_hash_delete(transfer_hash);
  }

  /* free the file list for this checkpoint */
  scr_hash_delete(scr_flush_async_hash);

  /* free data structures */
  scr_hash_delete(data);

  /* stop timer, compute bandwidth, and report performance */
  if (scr_my_rank_world == 0) {
    double time_end = MPI_Wtime();
    double time_diff = time_end - scr_flush_async_time_start;
    double bw = scr_flush_async_bytes / (1024.0 * 1024.0 * time_diff);
    scr_dbg(1, "scr_flush_async_complete: %f secs, %e bytes, %f MB/s, %f MB/s per proc",
            time_diff, scr_flush_async_bytes, bw, bw/scr_ranks_world
    );

    /* log messages about flush */
    if (flushed == SCR_SUCCESS) {
      /* the flush worked, print a debug message */
      scr_dbg(1, "scr_flush_async_complete: Flush of checkpoint %d succeeded", id);

      /* log details of flush */
      if (scr_log_enable) {
        time_t now = scr_log_seconds();
        scr_log_event("ASYNC FLUSH SUCCEEDED", scr_flush_async_dir, &id, &now, &time_diff);

        /* lookup the checkpoint directory for this checkpoint */
        char* ckpt_dir = scr_reddesc_dir_from_filemap(map, id, scr_my_rank_world);
        scr_log_transfer("ASYNC FLUSH", ckpt_dir, scr_flush_async_dir, &id,
                         &scr_flush_async_timestamp_start, &time_diff, &scr_flush_async_bytes
        );
        if (ckpt_dir != NULL) {
          free(ckpt_dir);
        }
      }
    } else {
      /* the flush failed, this is more serious so print an error message */
      scr_err("scr_flush_async_complete: Flush failed");

      /* log details of flush */
      if (scr_log_enable) {
        time_t now = scr_log_seconds();
        scr_log_event("ASYNC FLUSH FAILED", scr_flush_async_dir, &id, &now, &time_diff);
      }
    }
  }

  return flushed;
#endif
  return SCR_SUCCESS;
}

/* wait until the checkpoint currently being flushed completes */
static int scr_flush_async_wait(scr_filemap* map)
{
#if 0
  if (scr_flush_async_in_progress) {
    while (scr_bool_is_flushing(scr_flush_async_checkpoint_id)) {
      /* test whether the flush has completed, and if so complete the flush */
      double bytes = 0.0;
      if (scr_flush_async_test(map, scr_flush_async_checkpoint_id, &bytes) == SCR_SUCCESS) {
        /* complete the flush */
        scr_flush_async_complete(map, scr_flush_async_checkpoint_id);
      } else {
        /* otherwise, sleep to get out of the way */
        if (scr_my_rank_world == 0) {
          scr_dbg(1, "Flush of checkpoint %d is %d%% complete",
                  scr_flush_async_checkpoint_id, (int) (bytes / scr_flush_async_bytes * 100.0)
          );
        }
        usleep(10*1000*1000);
      }
    }
  }
#endif
  return SCR_SUCCESS;
}

/* create container files
 * could do different things here depending on the file system, for example:
 *   Lustre - have first process writing to container create it
 *   GPFS   - gather container names to rank 0 and have it create them all */
static int scr_flush_create_containers(const scr_hash* file_list)
{
  int success = SCR_SUCCESS;

  /* here, we look at each segment a process writes,
   * and the process which writes data to offset 0 is responsible for creating the container */

  /* get the hash of containers */
  scr_hash* containers = scr_hash_get(file_list, SCR_SUMMARY_6_KEY_CONTAINER);

  /* iterate over each of our files */
  scr_hash_elem* file_elem;
  scr_hash* files = scr_hash_get(file_list, SCR_FLUSH_KEY_FILE);
  for (file_elem = scr_hash_elem_first(files);
       file_elem != NULL;
       file_elem = scr_hash_elem_next(file_elem))
  {
    /* get the hash for this file */
    scr_hash* hash = scr_hash_elem_hash(file_elem);

    /* iterate over the segments for this file */
    scr_hash_elem* segment_elem;
    scr_hash* segments = scr_hash_get(hash, SCR_SUMMARY_6_KEY_SEGMENT);
    for (segment_elem = scr_hash_elem_first(segments);
         segment_elem != NULL;
         segment_elem = scr_hash_elem_next(segment_elem))
    {
      /* get the hash for this segment */
      scr_hash* segment = scr_hash_elem_hash(segment_elem);

      /* lookup the container details for this segment */
      char* name;
      unsigned long size, offset, length;
      if (scr_container_get_name_size_offset_length(segment, containers, &name, &size, &offset, &length) == SCR_SUCCESS) {
        /* if we write something to offset 0 of this container,
         * we are responsible for creating the file */
        if (offset == 0 && length > 0) {
          /* open the file with create and truncate options, then just immediately close it */
          int fd = scr_open(name, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
          if (fd < 0) {
            /* the create failed */
            scr_err("Opening file for writing: scr_open(%s) errno=%d %m @ %s:%d",
                    name, errno, __FILE__, __LINE__
            );
            success = SCR_FAILURE;
          } else {
            /* the create succeeded, now just close the file */
            scr_close(name, fd);
          }
        }
      } else {
        /* failed to read container details from segment hash, consider this an error */
        success = SCR_FAILURE;
      }
    }
  }

  /* determine whether all processes successfully created their containers */
  if (scr_alltrue((success == SCR_SUCCESS))) {
    return SCR_SUCCESS;
  }
  return SCR_FAILURE;
}

static int scr_flush_file_to_containers(const char* file, scr_meta* meta, scr_hash* segments, const scr_hash* containers)
{
  /* check that we got something for a source file */
  if (file == NULL || strcmp(file, "") == 0) {
    scr_err("Invalid source file @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* check that our other arguments are valid */
  if (meta == NULL || segments == NULL || containers == NULL) {
    scr_err("Invalid metadata, segments, or container @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* open the file for reading */
  int fd_src = scr_open(file, O_RDONLY);
  if (fd_src < 0) {
    scr_err("Opening file to copy: scr_open(%s) errno=%d %m @ %s:%d",
            file, errno, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* TODO:
  posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED | POSIX_FADV_SEQUENTIAL)
  that tells the kernel that you don't ever need the pages
  from the file again, and it won't bother keeping them in the page cache.
  */
  posix_fadvise(fd_src, 0, 0, POSIX_FADV_DONTNEED | POSIX_FADV_SEQUENTIAL);

  /* get the buffer size we'll use to write to the file */
  unsigned long buf_size = scr_file_buf_size;

  /* allocate buffer to read in file chunks */
  char* buf = (char*) malloc(buf_size);
  if (buf == NULL) {
    scr_err("Allocating memory: malloc(%llu) errno=%d %m @ %s:%d",
            buf_size, errno, __FILE__, __LINE__
    );
    scr_close(file, fd_src);
    return SCR_FAILURE;
  }

  /* initialize crc value */
  uLong crc;
  if (scr_crc_on_flush) {
    crc = crc32(0L, Z_NULL, 0);
  }

  int rc = SCR_SUCCESS;

  /* write out each segment */
  scr_hash_sort_int(segments, SCR_HASH_SORT_ASCENDING);
  scr_hash_elem* elem;
  for (elem = scr_hash_elem_first(segments);
       elem != NULL;
       elem = scr_hash_elem_next(elem))
  {
    /* get the container info for this segment */
    scr_hash* hash = scr_hash_elem_hash(elem);

    /* get the offset into the container and the length of the segment (both in bytes) */
    char* container_name;
    unsigned long container_size, container_offset, segment_length;
    if (scr_container_get_name_size_offset_length(hash, containers,
      &container_name, &container_size, &container_offset, &segment_length) != SCR_SUCCESS)
    {
      scr_err("Failed to get segment offset and length @ %s:%d",
              __FILE__, __LINE__
      );
      rc = SCR_FAILURE;
      break;
    }

    /* open container file for writing -- we don't truncate here because more than one
     * process may be writing to the same file */
    int fd_container = scr_open(container_name, O_WRONLY | O_CREAT, S_IRUSR | S_IWUSR);
    if (fd_container < 0) {
      scr_err("Opening file for writing: scr_open(%s) errno=%d %m @ %s:%d",
              container_name, errno, __FILE__, __LINE__
      );
      rc = SCR_FAILURE;
      break;
    }

    /* TODO:
    posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED | POSIX_FADV_SEQUENTIAL)
    that tells the kernel that you don't ever need the pages
    from the file again, and it won't bother keeping them in the page cache.
    */
    posix_fadvise(fd_container, 0, 0, POSIX_FADV_DONTNEED | POSIX_FADV_SEQUENTIAL);

    /* seek to offset within container */
    off_t pos = (off_t) container_offset;
    if (lseek(fd_container, pos, SEEK_SET) == (off_t)-1) {
      /* our seek failed, return an error */
      scr_err("Failed to seek to byte %lu in %s @ %s:%d",
              pos, container_name, __FILE__, __LINE__
      );
      rc = SCR_FAILURE;
      break;
    }
    
    /* copy data from file into container in chunks */
    unsigned long remaining = segment_length;
    while (remaining > 0) {
      /* read / write up to buf_size bytes at a time from file */
      unsigned long count = remaining;
      if (count > buf_size) {
        count = buf_size;
      }

      /* attempt to read buf_size bytes from file */
      int nread = scr_read_attempt(file, fd_src, buf, count);

      /* if we read some bytes, write them out */
      if (nread > 0) {
        /* optionally compute crc value as we go */
        if (scr_crc_on_flush) {
          crc = crc32(crc, (const Bytef*) buf, (uInt) nread);
        }

        /* write our nread bytes out */
        int nwrite = scr_write_attempt(container_name, fd_container, buf, nread);

        /* check for a write error or a short write */
        if (nwrite != nread) {
          /* write had a problem, stop copying and return an error */
          rc = SCR_FAILURE;
          break;
        }

        /* subtract the bytes we've processed from the number remaining */
        remaining -= (unsigned long) nread;
      }

      /* assume a short read is an error */
      if (nread < count) {
        /* read had a problem, stop copying and return an error */
        rc = SCR_FAILURE;
        break;
      }

      /* check for a read error, stop copying and return an error */
      if (nread < 0) {
        /* read had a problem, stop copying and return an error */
        rc = SCR_FAILURE;
        break;
      }
    }

    /* close container */
    if (scr_close(container_name, fd_container) != SCR_SUCCESS) {
      rc = SCR_FAILURE;
    }
  }

  /* close the source file */
  if (scr_close(file, fd_src) != SCR_SUCCESS) {
    rc = SCR_FAILURE;
  }

  /* free buffer */
  if (buf != NULL) {
    free(buf);
    buf = NULL;
  }

  /* verify / set crc value */
  if (rc == SCR_SUCCESS) {
    uLong crc2;
    if (scr_crc_on_flush) {
      if (scr_meta_get_crc32(meta, &crc2) == SCR_SUCCESS) {
        /* if a crc is already set in the meta data, check that we computed the same value */
        if (crc != crc2) {
          scr_err("CRC32 mismatch detected when flushing file %s @ %s:%d",
                  file, __FILE__, __LINE__
          );
          rc = SCR_FAILURE;
        }
      } else {
        /* if there is no crc set, let's set it now */
        scr_meta_set_crc32(meta, crc);
      }
    }
  }

  return rc;
}

/* flush files specified in list, and record corresponding entries for summary file */
static int scr_flush_files_list(scr_hash* file_list, scr_hash* summary)
{
  /* assume we will succeed in this flush */
  int rc = SCR_SUCCESS;

  /* create a summary file entry for our rank */
  scr_hash* rank2file_hash = scr_hash_new();
  scr_hash* rank_hash = scr_hash_set_kv_int(rank2file_hash, SCR_SUMMARY_6_KEY_RANK, scr_my_rank_world);
  scr_hash_set(summary, SCR_SUMMARY_6_KEY_RANK2FILE, rank2file_hash);

  /* get pointer to containers hash and copy into summary info if one exists */
  scr_hash* containers = scr_hash_get(file_list, SCR_SUMMARY_6_KEY_CONTAINER);
  if (containers != NULL) {
    scr_hash* containers_copy = scr_hash_new();
    scr_hash_merge(containers_copy, containers);
    scr_hash_set(summary, SCR_SUMMARY_6_KEY_CONTAINER, containers_copy);
  }

  /* flush each of my files and fill in summary data structure */
  scr_hash_elem* elem = NULL;
  scr_hash* files = scr_hash_get(file_list, SCR_KEY_FILE);
  for (elem = scr_hash_elem_first(files);
       elem != NULL;
       elem = scr_hash_elem_next(elem))
  {
    /* get the filename */
    char* file = scr_hash_elem_key(elem);

    /* get the hash for this element */
    scr_hash* hash = scr_hash_elem_hash(elem);

    /* get meta data for this file */
    scr_meta* meta = scr_hash_get(hash, SCR_KEY_META);

    /* if containers are defined, we flush the file to its containers,
     * otherwise we copy the file out as is */
    if (containers != NULL) {
      /* TODO: get original filename here */

      /* add this file to the summary file */
      char path[SCR_MAX_FILENAME];
      char name[SCR_MAX_FILENAME];
      scr_split_path(file, path, name);
      scr_hash* file_hash = scr_hash_set_kv(rank_hash, SCR_SUMMARY_6_KEY_FILE, name);

      /* get segments hash for this file */
      scr_hash* segments = scr_hash_get(hash, SCR_SUMMARY_6_KEY_SEGMENT);

      /* flush the file to the containers listed in its segmenets */
      if (scr_flush_file_to_containers(file, meta, segments, containers) == SCR_SUCCESS) {
        /* successfully flushed this file, record the filesize */
        unsigned long filesize = 0;
        if (scr_meta_get_filesize(meta, &filesize) == SCR_SUCCESS) {
          scr_hash_util_set_bytecount(file_hash, SCR_SUMMARY_6_KEY_SIZE, filesize);
        }

        /* record the crc32 if one was computed */
        uLong crc = 0;
        if (scr_meta_get_crc32(meta, &crc) == SCR_SUCCESS) {
          scr_hash_util_set_crc32(file_hash, SCR_SUMMARY_6_KEY_CRC, crc);
        }

        /* record segment information in summary file */
        scr_hash* segments_copy = scr_hash_new();
        scr_hash_merge(segments_copy, segments);
        scr_hash_set(file_hash, SCR_SUMMARY_6_KEY_SEGMENT, segments_copy);
      } else {
        /* the flush failed */
        rc = SCR_FAILURE;

        /* explicitly mark file as incomplete */
        scr_hash_set_kv_int(file_hash, SCR_SUMMARY_6_KEY_COMPLETE, 0);
      }
    } else {
      /* record the name of the file in the summary hash, and get reference to a hash for this file */
      char path[SCR_MAX_FILENAME];
      char name[SCR_MAX_FILENAME];
      scr_split_path(file, path, name);
      scr_hash* file_hash = scr_hash_set_kv(rank_hash, SCR_SUMMARY_6_KEY_FILE, name);

      /* get directory to flush file to */
      char* dir;
      if (scr_hash_util_get_str(hash, SCR_KEY_PATH, &dir) == SCR_SUCCESS) {
        /* flush the file and fill in the meta data for this file */
        if (scr_flush_a_file(file, dir, meta) == SCR_SUCCESS) {
          /* successfully flushed this file, record the filesize */
          unsigned long filesize = 0;
          if (scr_meta_get_filesize(meta, &filesize) == SCR_SUCCESS) {
            scr_hash_util_set_bytecount(file_hash, SCR_SUMMARY_6_KEY_SIZE, filesize);
          }

          /* record the crc32 if one was computed */
          uLong crc = 0;
          if (scr_meta_get_crc32(meta, &crc) == SCR_SUCCESS) {
            scr_hash_util_set_crc32(file_hash, SCR_SUMMARY_6_KEY_CRC, crc);
          }
        } else {
          /* the flush failed */
          rc = SCR_FAILURE;

          /* explicitly mark incomplete files */
          scr_hash_set_kv_int(file_hash, SCR_SUMMARY_6_KEY_COMPLETE, 0);
        }
      } else {
        /* failed to get directory to flush file to */
        rc = SCR_FAILURE;

        /* explicitly mark file as incomplete */
        scr_hash_set_kv_int(file_hash, SCR_SUMMARY_6_KEY_COMPLETE, 0);
      }
    }
  }

  return rc;
}

/* flushes data for files specified in file_list (with flow control),
 * and records status of each file in data */
static int scr_flush_data(scr_hash* file_list, scr_hash* data)
{
  int flushed = SCR_SUCCESS;

  /* flow control the write among processes */
  if (scr_my_rank_world == 0) {
    /* first, flush each of my files and fill in meta data structure */
    if (scr_flush_files_list(file_list, data) != SCR_SUCCESS) {
      flushed = SCR_FAILURE;
    }

    /* now, have a sliding window of w processes write simultaneously */
    int w = scr_flush_width;
    if (w > (scr_ranks_world - 1)) {
      w = scr_ranks_world - 1;
    }

    /* allocate MPI_Request arrays and an array of ints */
    int*         flags = (int*)         malloc(2 * w * sizeof(int));
    MPI_Request* req   = (MPI_Request*) malloc(2 * w * sizeof(MPI_Request));
    MPI_Status status;

    int i = 1;
    int outstanding = 0;
    int index = 0;
    while (i < scr_ranks_world || outstanding > 0) {
      /* issue up to w outstanding sends and receives */
      while (i < scr_ranks_world && outstanding < w) {
        /* post a receive for the response message we'll get back when rank i is done */
        MPI_Irecv(&flags[w + index], 1, MPI_INT, i, 0, scr_comm_world, &req[w + index]);

        /* post a send to tell rank i to start */
        flags[index] = flushed;
        MPI_Isend(&flags[index], 1, MPI_INT, i, 0, scr_comm_world, &req[index]);

        /* update the number of outstanding requests */
        i++;
        outstanding++;
        index++;
      }

      /* wait to hear back from any rank */
      MPI_Waitany(w, &req[w], &index, &status);

      /* someone responded, the send to this rank should also be done, so complete it */
      MPI_Wait(&req[index], &status);

      /* determine whether this rank flushed its file successfully */
      if (flags[w + index] != SCR_SUCCESS) {
        flushed = SCR_FAILURE;
      }

      /* one less request outstanding now */
      outstanding--;
    }

    /* free the MPI_Request arrays */
    if (req != NULL) {
      free(req);
      req = NULL;
    }
    if (flags != NULL) {
      free(flags);
      flags = NULL;
    }
  } else {
    /* receive signal to start */
    int start = 0;
    MPI_Status status;
    MPI_Recv(&start, 1, MPI_INT, 0, 0, scr_comm_world, &status);

    /* flush files if we've had success so far, otherwise skip the flush and return failure */
    if (start == SCR_SUCCESS) {
      /* flush each of my files and fill in meta data strucutre */
      if (scr_flush_files_list(file_list, data) != SCR_SUCCESS) {
        flushed = SCR_FAILURE;
      }
    } else {
      /* someone failed before we even started, so don't bother */
      flushed = SCR_FAILURE;
    }

    /* send message to rank 0 to report that we're done */
    MPI_Send(&flushed, 1, MPI_INT, 0, 0, scr_comm_world);
  }

  /* determine whether everyone wrote their files ok */
  if (scr_alltrue((flushed == SCR_SUCCESS))) {
    return SCR_SUCCESS;
  }
  return SCR_FAILURE;
}

/* write summary file for flush */
static int scr_flush_summary(const scr_hash* file_list, scr_hash* data)
{
  int flushed = SCR_SUCCESS;

  /* TODO: current method is a flat tree with rank 0 as the root,
   * need a more scalable algorithm */

  /* flow control the send among processes, and gather meta data to rank 0 */
  if (scr_my_rank_world == 0) {
    /* now, have a sliding window of w processes send simultaneously */
    int w = scr_flush_width;
    if (w > (scr_ranks_world - 1)) {
      w = scr_ranks_world - 1;
    }

    /* allocate MPI_Request arrays and an array of ints */
    int*         ranks    = (int*)         malloc(w * sizeof(int));
    int*         flags    = (int*)         malloc(w * sizeof(int));
    MPI_Request* req_recv = (MPI_Request*) malloc(w * sizeof(MPI_Request));
    MPI_Request* req_send = (MPI_Request*) malloc(w * sizeof(MPI_Request));
    MPI_Status status;

    int i = 1;
    int outstanding = 0;
    int index = 0;
    while (i < scr_ranks_world || outstanding > 0) {
      /* issue up to w outstanding sends and receives */
      while (i < scr_ranks_world && outstanding < w) {
        /* record which rank we assign to this slot */
        ranks[index] = i;

        /* post a receive for the response message we'll get back when rank i is done */
        MPI_Irecv(&flags[index], 1, MPI_INT, i, 0, scr_comm_world, &req_recv[index]);

        /* post a send to tell rank i to start */
        MPI_Isend(&flushed, 1, MPI_INT, i, 0, scr_comm_world, &req_send[index]);

        /* update the number of outstanding requests */
        i++;
        outstanding++;
        index++;
      }

      /* wait to hear back from any rank */
      MPI_Waitany(w, req_recv, &index, &status);

      /* someone responded, the send to this rank should also be done, so complete it */
      MPI_Wait(&req_send[index], &status);

      /* receive the meta data from this rank */
      scr_hash* incoming_hash = scr_hash_new();
      scr_hash_recv(incoming_hash, ranks[index], scr_comm_world);
      scr_hash_merge(data, incoming_hash);
      scr_hash_delete(incoming_hash);

      /* one less request outstanding now */
      outstanding--;
    }

    /* free the MPI_Request arrays */
    if (req_send != NULL) {
      free(req_send);
      req_send = NULL;
    }
    if (req_recv != NULL) {
      free(req_recv);
      req_recv = NULL;
    }
    if (flags != NULL) {
      free(flags);
      flags = NULL;
    }
    if (ranks != NULL) {
      free(ranks);
      ranks = NULL;
    }
  } else {
    /* receive signal to start */
    int start = 0;
    MPI_Status status;
    MPI_Recv(&start, 1, MPI_INT, 0, 0, scr_comm_world, &status);

    /* flush each of my files and fill in meta data structures */
    if (start != SCR_SUCCESS) {
      flushed = SCR_FAILURE;
    }

    /* send message to rank 0 to report that we're done */
    MPI_Send(&flushed, 1, MPI_INT, 0, 0, scr_comm_world);

    /* would be better to do this as a reduction-type of operation */
    scr_hash_send(data, 0, scr_comm_world);
  }

  /* write out the summary file */
  if (scr_my_rank_world == 0) {
    /* determine whether flush was complete */
    int complete = 1;
    if (flushed != SCR_SUCCESS) {
      complete = 0;
    }

    /* now write out summary file */
    scr_dataset* dataset = scr_hash_get(file_list, SCR_KEY_DATASET);
    if (scr_summary_write(scr_par_prefix, dataset, complete, data) != SCR_SUCCESS) {
      flushed = SCR_FAILURE;
    }
  }

  /* determine whether everyone wrote their files ok */
  if (scr_alltrue((flushed == SCR_SUCCESS))) {
    return SCR_SUCCESS;
  }
  return SCR_FAILURE;
}

/* flush files from cache to parallel file system under SCR_PREFIX */
static int scr_cache_flush(scr_filemap* map, int id)
{
  int flushed = SCR_SUCCESS;
  int i;
  int tmp_rc;

  /* if user has disabled flush, return failure */
  if (scr_flush <= 0) {
    return SCR_FAILURE;
  }

  /* if we don't need a flush, return right away with success */
  if (! scr_bool_need_flush(id)) {
    return SCR_SUCCESS;
  }

  /* if scr_par_prefix is not set, return right away with an error */
  if (strcmp(scr_par_prefix, "") == 0) {
    return SCR_FAILURE;
  }

  /* this may take a while, so tell user what we're doing */
  if (scr_my_rank_world == 0) {
    scr_dbg(1, "Initiating flush of dataset %d", id);
  }

  /* make sure all processes make it this far before progressing */
  MPI_Barrier(scr_comm_world);

  /* start timer */
  time_t timestamp_start;
  double time_start;
  if (scr_my_rank_world == 0) {
    timestamp_start = scr_log_seconds();
    time_start = MPI_Wtime();
  }

  /* log the flush start */
  if (scr_my_rank_world == 0) {
    if (scr_log_enable) {
      time_t now = scr_log_seconds();
      scr_log_event("FLUSH STARTED", NULL, &id, &now, NULL);
    }
  }

  /* check that we have all of our files */
  int have_files = 1;
  if (scr_cache_check_files(map, id) != SCR_SUCCESS) {
    scr_err("One or more files is missing @ %s:%d", __FILE__, __LINE__);
    have_files = 0;
  }
  if (! scr_alltrue(have_files)) {
    if (scr_my_rank_world == 0) {
      scr_err("One or more processes are missing their files @ %s:%d", __FILE__, __LINE__);
      if (scr_log_enable) {
        double time_end = MPI_Wtime();
        double time_diff = time_end - time_start;
        time_t now = scr_log_seconds();
        scr_log_event("FLUSH FAILED", "Missing files in cache", &id, &now, &time_diff);
      }
    }
    return SCR_FAILURE;
  }

  /* if we are flushing something asynchronously, wait on it */
  if (scr_flush_async_in_progress) {
    scr_flush_async_wait(map);
    
    /* the flush we just waited on could be the requested dataset,
     * so perhaps we're already done */
    if (! scr_bool_need_flush(id)) {
      return SCR_SUCCESS;
    }
  }

  /* allocate a fresh hash to hold the list of files we'll be flushing */
  scr_hash* file_list = scr_hash_new();
  if (file_list == NULL) {
    scr_err("Failed to allocate file list hash @ %s:%d",
            __FILE__, __LINE__
    );
    MPI_Abort(scr_comm_world, 0);
  }

  /* build the list of files to flush, which includes meta data for each one */
  if (scr_flush_build_list(map, id, file_list) != SCR_SUCCESS) {
    scr_err("Failed to get list of files @ %s:%d",
            __FILE__, __LINE__
    );
    MPI_Abort(scr_comm_world, 0);
  }

  /* identify containers for our files */
  if (scr_use_containers) {
    scr_flush_identify_containers(file_list);
  }

  /* create directories for flush */
  if (scr_flush_create_dirs(file_list) != SCR_SUCCESS) {
    if (scr_my_rank_world == 0) {
      scr_err("Failed to create flush directories @ %s:%d", __FILE__, __LINE__);
      if (scr_log_enable) {
        double time_end = MPI_Wtime();
        double time_diff = time_end - time_start;
        time_t now = scr_log_seconds();
        scr_log_event("FLUSH FAILED", "Failed to create directory", &id, &now, &time_diff);
      }
    }
    scr_hash_delete(file_list);
    return SCR_FAILURE;
  }

  /* create container files */
  if (scr_use_containers) {
    if (scr_flush_create_containers(file_list) != SCR_SUCCESS) {
      /* TODO: delete the directories that we just created above? */
      scr_hash_delete(file_list);
      return SCR_FAILURE;
    }
  }

  /* allocate structure to hold summary file info */
  scr_hash* data = scr_hash_new();
  if (data == NULL) {
    scr_err("Failed to malloc data structure to write out summary file @ %s:%d",
            __FILE__, __LINE__
    );
    MPI_Abort(scr_comm_world, 0);
  }

  /* write the data out to files */
  if (scr_flush_data(file_list, data) != SCR_SUCCESS) {
    flushed = SCR_FAILURE;
  }

  /* write summary file */
  if (scr_flush_summary(file_list, data) != SCR_SUCCESS) {
    flushed = SCR_FAILURE;
  }

  /* create current symlink */
  double total_bytes = 0.0;
  if (scr_my_rank_world == 0) {
    if (flushed == SCR_SUCCESS) {
      /* get the dataset corresponding to this id */
      scr_dataset* dataset = scr_dataset_new();
      scr_filemap_get_dataset(map, id, scr_my_rank_world, dataset);

      /* get the number of bytes in the dataset */
      unsigned long dataset_bytes;
      if (scr_dataset_get_size(dataset, &dataset_bytes) == SCR_SUCCESS) {
        total_bytes = (double) dataset_bytes;
      }

      /* read the name of the dataset and update the current symlink */
      char* dataset_name;
      if (scr_dataset_get_name(dataset, &dataset_name) == SCR_SUCCESS) {
        /* build path to current symlink */
        char current[SCR_MAX_FILENAME];
        scr_build_path(current, sizeof(current), scr_par_prefix, SCR_CURRENT_LINK);

        /* if current exists, unlink it */
        if (access(current, F_OK) == 0) {
          unlink(current);
        }

        /* create new current to point to new directory */
        symlink(dataset_name, current);
      }

      /* delete the dataset object */
      scr_dataset_delete(dataset);
    }
  }

  /* have rank 0 broadcast whether the entire flush succeeded,
   * including summary file and symlink update */
  MPI_Bcast(&flushed, 1, MPI_INT, 0, scr_comm_world);

  /* mark this dataset as flushed to the parallel file system */
  if (flushed == SCR_SUCCESS) {
    scr_flush_file_location_set(id, SCR_FLUSH_KEY_LOCATION_PFS);
  }

  /* free data structures */
  scr_hash_delete(data);
  scr_hash_delete(file_list);

  /* stop timer, compute bandwidth, and report performance */
  if (scr_my_rank_world == 0) {
    /* stop timer and compute bandwidth */
    double time_end = MPI_Wtime();
    double time_diff = time_end - time_start;
    double bw = total_bytes / (1024.0 * 1024.0 * time_diff);
    scr_dbg(1, "scr_cache_flush: %f secs, %e bytes, %f MB/s, %f MB/s per proc",
            time_diff, total_bytes, bw, bw/scr_ranks_world
    );

    /* log messages about flush */
    if (flushed == SCR_SUCCESS) {
      /* the flush worked, print a debug message */
      scr_dbg(1, "scr_cache_flush: Flush of dataset %d succeeded", id);

      /* log details of flush */
      if (scr_log_enable) {
        time_t now = scr_log_seconds();
        scr_log_event("FLUSH SUCCEEDED", NULL, &id, &now, &time_diff);

        /* lookup the redundancy descriptor for this checkpoint */
        char* ckpt_dir = scr_reddesc_dir_from_filemap(map, id, scr_my_rank_world);
        scr_log_transfer("FLUSH", ckpt_dir, NULL, &id, &timestamp_start, &time_diff, &total_bytes);
        if (ckpt_dir != NULL) {
          free(ckpt_dir);
        }
      }
    } else {
      /* the flush failed, this is more serious so print an error message */
      scr_err("scr_cache_flush: Flush of dataset %d failed", id);

      /* log details of flush */
      if (scr_log_enable) {
        time_t now = scr_log_seconds();
        scr_log_event("FLUSH FAILED", NULL, &id, &now, &time_diff);
      }
    }
  }

  return flushed;
}

/* check whether a flush is needed, and execute flush if so */
static int scr_check_flush(scr_filemap* map)
{
  /* check whether user has flush enabled */
  if (scr_flush > 0) {
    /* every scr_flush checkpoints, flush the checkpoint set */
    if (scr_checkpoint_id > 0 && scr_checkpoint_id % scr_flush == 0) {
      if (scr_flush_async) {
        /* check that we don't start an async flush if one is already in progress */
        if (scr_flush_async_in_progress) {
          /* we need to flush the current checkpoint, however, another flush is ongoing,
           * so wait for this other flush to complete before starting the next one */
          scr_flush_async_wait(map);
        }

        /* start an async flush on the current checkpoint id */
        scr_flush_async_start(map, scr_checkpoint_id);
      } else {
        /* synchronously flush the current checkpoint */
        scr_cache_flush(map, scr_checkpoint_id);
      }
    }
  }
  return SCR_SUCCESS;
}

/*
=========================================
Halt logic
=========================================
*/

/* writes a halt file to indicate that the SCR should exit job at first opportunity */
static int scr_halt(const char* reason)
{
  /* copy in reason if one was given */
  if (reason != NULL) {
    scr_hash_unset(scr_halt_hash, SCR_HALT_KEY_EXIT_REASON);
    scr_hash_set_kv(scr_halt_hash, SCR_HALT_KEY_EXIT_REASON, reason);
  }

  /* log the halt condition */
  int* ckpt = NULL;
  if (scr_checkpoint_id > 0) {
    ckpt = &scr_checkpoint_id;
  }
  scr_log_halt(reason, ckpt);

  /* and write out the halt file */
  return scr_halt_sync_and_decrement(scr_halt_file, scr_halt_hash, 0);
}

/* check whether we should halt the job */
static int scr_bool_check_halt_and_decrement(int halt_cond, int decrement)
{
  /* assume we don't have to halt */
  int need_to_halt = 0;

  /* only rank 0 reads the halt file */
  if (scr_my_rank_world == 0) {
    /* TODO: all epochs are stored in ints, should be in unsigned ints? */
    /* get current epoch seconds */
    struct timeval tv;
    gettimeofday(&tv, NULL);
    int now = tv.tv_sec;

    /* locks halt file, reads it to pick up new values, decrements the
     * checkpoint counter, writes it out, and unlocks it */
    scr_halt_sync_and_decrement(scr_halt_file, scr_halt_hash, decrement);

    /* set halt seconds to value found in our halt hash */
    int halt_seconds;
    if (scr_hash_util_get_int(scr_halt_hash, SCR_HALT_KEY_SECONDS, &halt_seconds) != SCR_SUCCESS) {
      /* didn't find anything, so set value to 0 */
      halt_seconds = 0;
    }

    /* if halt secs enabled, check the remaining time */
    if (halt_seconds > 0) {
      int remaining = scr_env_seconds_remaining();
      if (remaining >= 0 && remaining <= halt_seconds) {
        if (halt_cond == SCR_TEST_AND_HALT) {
          scr_dbg(0, "Job exiting: Reached time limit: (seconds remaining = %d) <= (SCR_HALT_SECONDS = %d).",
                  remaining, halt_seconds
          );
          scr_halt("TIME_LIMIT");
        }
        need_to_halt = 1;
      }
    }

    /* check whether a reason has been specified */
    char* reason;
    if (scr_hash_util_get_str(scr_halt_hash, SCR_HALT_KEY_EXIT_REASON, &reason) == SCR_SUCCESS) {
      if (strcmp(reason, "") != 0) {
        /* since reason points at the EXIT_REASON string in the halt hash, and since
         * scr_halt() resets this value, we need to copy the current reason */
        char* tmp_reason = strdup(reason);
        if (halt_cond == SCR_TEST_AND_HALT && tmp_reason != NULL) {
          scr_dbg(0, "Job exiting: Reason: %s.", tmp_reason);
          scr_halt(tmp_reason);
        }
        if (tmp_reason != NULL) {
          free(tmp_reason);
          tmp_reason = NULL;
        }
        need_to_halt = 1;
      }
    }

    /* check whether we are out of checkpoints */
    int checkpoints_left;
    if (scr_hash_util_get_int(scr_halt_hash, SCR_HALT_KEY_CHECKPOINTS, &checkpoints_left) == SCR_SUCCESS) {
      if (checkpoints_left == 0) {
        if (halt_cond == SCR_TEST_AND_HALT) {
          scr_dbg(0, "Job exiting: No more checkpoints remaining.");
          scr_halt("NO_CHECKPOINTS_LEFT");
        }
        need_to_halt = 1;
      }
    }

    /* check whether we need to exit before a specified time */
    int exit_before;
    if (scr_hash_util_get_int(scr_halt_hash, SCR_HALT_KEY_EXIT_BEFORE, &exit_before) == SCR_SUCCESS) {
      if (now >= (exit_before - halt_seconds)) {
        if (halt_cond == SCR_TEST_AND_HALT) {
          time_t time_now  = (time_t) now;
          time_t time_exit = (time_t) exit_before - halt_seconds;
          char str_now[256];
          char str_exit[256];
          strftime(str_now,  sizeof(str_now),  "%c", localtime(&time_now));
          strftime(str_exit, sizeof(str_exit), "%c", localtime(&time_exit));
          scr_dbg(0, "Job exiting: Current time (%s) is past ExitBefore-HaltSeconds time (%s).",
                  str_now, str_exit
          );
          scr_halt("EXIT_BEFORE_TIME");
        }
        need_to_halt = 1;
      }
    }

    /* check whether we need to exit after a specified time */
    int exit_after;
    if (scr_hash_util_get_int(scr_halt_hash, SCR_HALT_KEY_EXIT_AFTER, &exit_after) == SCR_SUCCESS) {
      if (now >= exit_after) {
        if (halt_cond == SCR_TEST_AND_HALT) {
          time_t time_now  = (time_t) now;
          time_t time_exit = (time_t) exit_after;
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
  }

  MPI_Bcast(&need_to_halt, 1, MPI_INT, 0, scr_comm_world);
  if (need_to_halt && halt_cond == SCR_TEST_AND_HALT) {
    /* handle any async flush */
    if (scr_flush_async_in_progress) {
      if (scr_flush_async_checkpoint_id == scr_checkpoint_id) {
        /* we're going to sync flush this same checkpoint below, so kill it */
        scr_flush_async_stop(scr_map);
      } else {
        /* the async flush is flushing a different checkpoint, so wait for it */
        scr_flush_async_wait(scr_map);
      }
    }

    /* TODO: need to flush any output sets and the latest checkpoint set */

    /* flush files if needed */
    scr_cache_flush(scr_map, scr_checkpoint_id);

    /* sync up tasks before exiting (don't want tasks to exit so early that
     * runtime kills others after timeout) */
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

/* returns true if a an XOR file is found for this rank for the given checkpoint id,
 * sets xor_file to full filename */
static int scr_bool_have_xor_file(scr_filemap* map, int checkpoint_id, char* xor_file)
{
  int rc = 0;

  /* find the name of my xor chunk file: read filemap and check filetype of each file */
  scr_hash_elem* file_elem = NULL;
  for (file_elem = scr_filemap_first_file(map, checkpoint_id, scr_my_rank_world);
       file_elem != NULL;
       file_elem = scr_hash_elem_next(file_elem))
  {
    /* get the filename */
    char* file = scr_hash_elem_key(file_elem);

    /* read the meta for this file */
    scr_meta* meta = scr_meta_new();
    scr_filemap_get_meta(map, checkpoint_id, scr_my_rank_world, file, meta);

    /* if the filetype of this file is an XOR fule, copy the filename and bail out */
    char* filetype = NULL;
    if (scr_meta_get_filetype(meta, &filetype) == SCR_SUCCESS) {
      if (strcmp(filetype, SCR_META_FILE_XOR) == 0) {
        strcpy(xor_file, file);
        rc = 1;
        scr_meta_delete(meta);
        break;
      }
    }

    /* free the meta data for this file and go on to the next */
    scr_meta_delete(meta);
  }

  return rc;
}

/* given a filemap, a redundancy descriptor, a dataset id, and a failed rank in my xor set,
 * rebuild files and add them to the filemap */
static int scr_rebuild_xor(scr_filemap* map, const struct scr_reddesc* c, int id, int root)
{
  int rc = SCR_SUCCESS;
  int i;
  MPI_Status status[2];

  int fd_chunk = 0;
  char full_chunk_filename[SCR_MAX_FILENAME] = "";
  char path[SCR_MAX_FILENAME] = "";
  char name[SCR_MAX_FILENAME] = "";

  int* fds = NULL;
  char** filenames = NULL;
  unsigned long* filesizes = NULL;

  /* allocate hash object to read in (or receive) the header of the XOR file */
  scr_hash* header = scr_hash_new();

  int num_files = -1;
  scr_hash* current_hash = NULL;
  if (root != c->my_rank) {
    /* lookup name of xor file */
    if (! scr_bool_have_xor_file(map, id, full_chunk_filename)) {
      scr_abort(-1, "Missing XOR file %s @ %s:%d",
              full_chunk_filename, __FILE__, __LINE__
      );
    }

    /* open our xor file for reading */
    fd_chunk = scr_open(full_chunk_filename, O_RDONLY);
    if (fd_chunk < 0) {
      scr_abort(-1, "Opening XOR file for reading in XOR rebuild: scr_open(%s, O_RDONLY) errno=%d %m @ %s:%d",
              full_chunk_filename, errno, __FILE__, __LINE__
      );
    }

    /* read in the xor chunk header */
    scr_hash_read_fd(full_chunk_filename, fd_chunk, header);

    /* lookup number of files this process wrote */
    current_hash = scr_hash_get(header, SCR_KEY_COPY_XOR_CURRENT);
    if (scr_hash_util_get_int(current_hash, SCR_KEY_COPY_XOR_FILES, &num_files) != SCR_SUCCESS) {
      scr_abort(-1, "Failed to read number of files from XOR file header: %s @ %s:%d",
              full_chunk_filename, __FILE__, __LINE__
      );
    }

    /* allocate arrays to hold file descriptors, filenames, and filesizes for each of our files */
    if (num_files > 0) {
      fds       = (int*)           malloc(num_files * sizeof(int));
      filenames = (char**)         malloc(num_files * sizeof(char*));
      filesizes = (unsigned long*) malloc(num_files * sizeof(unsigned long));
      if (fds == NULL || filenames == NULL || filesizes == NULL) {
        scr_abort(-1, "Failed to allocate file arrays @ %s:%d",
                  __FILE__, __LINE__
        );
      }
    }

    /* get path from chunk file */
    scr_split_path(full_chunk_filename, path, name);

    /* open each of our files */
    for (i=0; i < num_files; i++) {
      /* lookup meta data for this file from header hash */
      scr_hash* meta_tmp = scr_hash_get_kv_int(current_hash, SCR_KEY_COPY_XOR_FILE, i);
      if (meta_tmp == NULL) {
        scr_abort(-1, "Failed to find file %d in XOR file header %s @ %s:%d",
                  i, full_chunk_filename, __FILE__, __LINE__
        );
      }

      /* get pointer to filename for this file */
      char* filename = NULL;
      if (scr_meta_get_filename(meta_tmp, &filename) != SCR_SUCCESS) {
        scr_abort(-1, "Failed to read filename for file %d in XOR file header %s @ %s:%d",
                  i, full_chunk_filename, __FILE__, __LINE__
        );
      }

      /* create full path to the file */
      char full_file[SCR_MAX_FILENAME];
      scr_build_path(full_file, sizeof(full_file), path, filename);

      /* copy the full filename */
      filenames[i] = strdup(full_file);
      if (filenames[i] == NULL) {
        scr_abort(-1, "Failed to copy filename during rebuild @ %s:%d",
                  full_file, __FILE__, __LINE__
        );
      }

      /* lookup the filesize */
      if (scr_meta_get_filesize(meta_tmp, &filesizes[i]) != SCR_SUCCESS) {
        scr_abort(-1, "Failed to read file size for file %s in XOR file header during rebuild @ %s:%d",
                  full_file, __FILE__, __LINE__
        );
      }

      /* open the file for reading */
      fds[i] = scr_open(full_file, O_RDONLY);
      if (fds[i] < 0) {
        /* TODO: try again? */
        scr_abort(-1, "Opening checkpoint file for reading in XOR rebuild: scr_open(%s, O_RDONLY) errno=%d %m @ %s:%d",
                  full_file, errno, __FILE__, __LINE__
        );
      }
    }

    /* if failed rank is to my left, i have the meta for his files, send him the header */
    if (root == c->lhs_rank) {
      scr_hash_send(header, c->lhs_rank, c->comm);
    }

    /* if failed rank is to my right, send him my file info so he can write his XOR header */
    if (root == c->rhs_rank) {
      scr_hash_send(current_hash, c->rhs_rank, c->comm);
    }
  } else {
    /* receive the header from right-side partner;
     * includes number of files and meta data for my files, as well as, 
     * the checkpoint id and the chunk size */
    scr_hash_recv(header, c->rhs_rank, c->comm);

    /* rename PARTNER to CURRENT in our header */
    current_hash = scr_hash_new();
    scr_hash* old_hash = scr_hash_get(header, SCR_KEY_COPY_XOR_PARTNER);
    scr_hash_merge(current_hash, old_hash);
    scr_hash_unset(header, SCR_KEY_COPY_XOR_CURRENT);
    scr_hash_unset(header, SCR_KEY_COPY_XOR_PARTNER);
    scr_hash_set(header, SCR_KEY_COPY_XOR_CURRENT, current_hash);

    /* receive number of files our left-side partner has and allocate an array of
     * meta structures to store info */
    scr_hash* partner_hash = scr_hash_new();
    scr_hash_recv(partner_hash, c->lhs_rank, c->comm);
    scr_hash_set(header, SCR_KEY_COPY_XOR_PARTNER, partner_hash);

    /* get the number of files */
    if (scr_hash_util_get_int(current_hash, SCR_KEY_COPY_XOR_FILES, &num_files) != SCR_SUCCESS) {
      scr_abort(-1, "Failed to read number of files from XOR file header during rebuild @ %s:%d",
              __FILE__, __LINE__
      );
    }
    if (num_files > 0) {
      fds       = (int*)           malloc(num_files * sizeof(int));
      filenames = (char**)         malloc(num_files * sizeof(char*));
      filesizes = (unsigned long*) malloc(num_files * sizeof(unsigned long));
      if (fds == NULL || filenames == NULL || filesizes == NULL) {
        scr_abort(-1, "Failed to allocate file arrays @ %s:%d",
                  __FILE__, __LINE__
        );
      }
    }

    /* set chunk filename of form:  <xor_rank+1>_of_<xorset_size>_in_<level_partion>x<xorset_size>.xor */
    char dir[SCR_MAX_FILENAME];
    scr_cache_dir_get(c, id, dir);
    sprintf(full_chunk_filename, "%s/%d_of_%d_in_%d.xor", dir, c->my_rank+1, c->ranks, c->group_id);

    /* split file into path and name */
    scr_split_path(full_chunk_filename, path, name);

    /* record our chunk file and each of our files in the filemap before creating */
    scr_filemap_add_file(map, id, scr_my_rank_world, full_chunk_filename);
    for (i=0; i < num_files; i++) {
      /* lookup meta data for this file from header hash */
      scr_hash* meta_tmp = scr_hash_get_kv_int(current_hash, SCR_KEY_COPY_XOR_FILE, i);
      if (meta_tmp == NULL) {
        scr_abort(-1, "Failed to find file %d in XOR file header %s @ %s:%d",
                  i, full_chunk_filename, __FILE__, __LINE__
        );
      }

      /* get pointer to filename for this file */
      char* filename = NULL;
      if (scr_meta_get_filename(meta_tmp, &filename) != SCR_SUCCESS) {
        scr_abort(-1, "Failed to read filename for file %d in XOR file header %s @ %s:%d",
                  i, full_chunk_filename, __FILE__, __LINE__
        );
      }

      /* get the filename */
      char full_file[SCR_MAX_FILENAME];
      scr_build_path(full_file, sizeof(full_file), path, filename);

      /* copy the filename */
      filenames[i] = strdup(full_file);
      if (filenames[i] == NULL) {
        scr_abort(-1, "Failed to copy filename during rebuild @ %s:%d",
                  full_file, __FILE__, __LINE__
        );
      }

      /* get the filesize */
      if (scr_meta_get_filesize(meta_tmp, &filesizes[i]) != SCR_SUCCESS) {
        scr_abort(-1, "Failed to read file size for file %s in XOR file header during rebuild @ %s:%d",
                  full_file, __FILE__, __LINE__
        );
      }

      /* add the file to our filemap */
      scr_filemap_add_file(map, id, scr_my_rank_world, full_file);
    }
    scr_filemap_set_expected_files(map, id, scr_my_rank_world, num_files + 1);
    scr_filemap_write(scr_map_file, map);

    /* open my chunk file for writing */
    fd_chunk = scr_open(full_chunk_filename, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
    if (fd_chunk < 0) {
      /* TODO: try again? */
      scr_abort(-1, "Opening XOR chunk file for writing in XOR rebuild: scr_open(%s) errno=%d %m @ %s:%d",
                full_chunk_filename, errno, __FILE__, __LINE__
      );
    }

    /* open each of my files for writing */
    for (i=0; i < num_files; i++) {
      /* open my file for writing */
      fds[i] = scr_open(filenames[i], O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
      if (fds[i] < 0) {
        /* TODO: try again? */
        scr_abort(-1, "Opening file for writing in XOR rebuild: scr_open(%s) errno=%d %m @ %s:%d",
                  filenames[i], errno, __FILE__, __LINE__
        );
      }
    }

    /* write XOR chunk file header */
    scr_hash_write_fd(full_chunk_filename, fd_chunk, header);
  }

  /* read the chunk size used to compute the xor data */
  unsigned long chunk_size;
  if (scr_hash_util_get_unsigned_long(header, SCR_KEY_COPY_XOR_CHUNK, &chunk_size) != SCR_SUCCESS) {
    scr_abort(-1, "Failed to read chunk size from XOR file header %s @ %s:%d",
            full_chunk_filename, __FILE__, __LINE__
    );
  }

  /* allocate buffer to read a piece of my file */
  char* send_buf = (char*) scr_align_malloc(scr_mpi_buf_size, scr_page_size);
  if (send_buf == NULL) {
    scr_abort(-1, "Allocating memory for send buffer: malloc(%d) errno=%d %m @ %s:%d",
            scr_mpi_buf_size, errno, __FILE__, __LINE__
    );
  }

  /* allocate buffer to read a piece of the recevied chunk file */
  char* recv_buf = (char*) scr_align_malloc(scr_mpi_buf_size, scr_page_size);
  if (recv_buf == NULL) {
    scr_abort(-1, "Allocating memory for recv buffer: malloc(%d) errno=%d %m @ %s:%d",
            scr_mpi_buf_size, errno, __FILE__, __LINE__
    );
  }

  /* Pipelined XOR Reduce to root */
  unsigned long offset = 0;
  int chunk_id;
  for (chunk_id = 0; chunk_id < c->ranks; chunk_id++) {
    size_t nread = 0;
    while (nread < chunk_size) {
      size_t count = chunk_size - nread;
      if (count > scr_mpi_buf_size) {
        count = scr_mpi_buf_size;
      }

      if (root != c->my_rank) {
        /* read the next set of bytes for this chunk from my file into send_buf */
        if (chunk_id != c->my_rank) {
          /* for this chunk, read data from the logical file */
          if (scr_read_pad_n(num_files, filenames, fds,
                             send_buf, count, offset, filesizes) != SCR_SUCCESS)
          {
            /* read failed, make sure we fail this rebuild */
            rc = SCR_FAILURE;
          }
          offset += count;
        } else {
          /* for this chunk, read data from the XOR file */
          if (scr_read_attempt(full_chunk_filename, fd_chunk, send_buf, count) != count) {
            /* read failed, make sure we fail this rebuild */
            rc = SCR_FAILURE;
          }
        }

        /* if not start of pipeline, receive data from left and xor with my own */
        if (root != c->lhs_rank) {
          int i;
          MPI_Recv(recv_buf, count, MPI_BYTE, c->lhs_rank, 0, c->comm, &status[0]);
          for (i = 0; i < count; i++) {
            send_buf[i] ^= recv_buf[i];
          }
        }

        /* send data to right-side partner */
        MPI_Send(send_buf, count, MPI_BYTE, c->rhs_rank, 0, c->comm);
      } else {
        /* root of rebuild, just receive incoming chunks and write them out */
        MPI_Recv(recv_buf, count, MPI_BYTE, c->lhs_rank, 0, c->comm, &status[0]);

        /* if this is not my xor chunk, write data to normal file, otherwise write to my xor chunk */
        if (chunk_id != c->my_rank) {
          /* for this chunk, write data to the logical file */
          if (scr_write_pad_n(num_files, filenames, fds,
                              recv_buf, count, offset, filesizes) != SCR_SUCCESS)
          {
            /* write failed, make sure we fail this rebuild */
            rc = SCR_FAILURE;
          }
          offset += count;
        } else {
          /* for this chunk, write data from the XOR file */
          if (scr_write_attempt(full_chunk_filename, fd_chunk, recv_buf, count) != count) {
            /* write failed, make sure we fail this rebuild */
            rc = SCR_FAILURE;
          }
        }
      }

      nread += count;
    }
  }

  /* close my chunkfile */
  if (scr_close(full_chunk_filename, fd_chunk) != SCR_SUCCESS) {
    rc = SCR_FAILURE;
  }

  /* close my checkpoint files */
  for (i=0; i < num_files; i++) {
    if (scr_close(filenames[i], fds[i]) != SCR_SUCCESS) {
      rc = SCR_FAILURE;
    }
  }

  /* if i'm the rebuild rank, complete my file and xor chunk */
  if (root == c->my_rank) {
    /* complete each of our files and mark each as complete */
    for (i=0; i < num_files; i++) {
      /* TODO: need to check for errors, check that file is really valid */

      /* fill out meta info for our file and complete it */
      scr_hash* meta_tmp = scr_hash_get_kv_int(current_hash, SCR_KEY_COPY_XOR_FILE, i);
      scr_filemap_set_meta(map, id, scr_my_rank_world, filenames[i], meta_tmp);

      /* TODODSET:write out filemap here? */

      /* if crc_on_copy is set, compute and store CRC32 value for each file */
      if (scr_crc_on_copy) {
        /* check for mismatches here, in case we failed to rebuild the file correctly */
        if (scr_compute_crc(map, id, scr_my_rank_world, filenames[i]) != SCR_SUCCESS) {
          scr_err("Failed to verify CRC32 after rebuild on file %s @ %s:%d",
                  filenames[i], __FILE__, __LINE__
          );

          /* make sure we fail this rebuild */
          rc = SCR_FAILURE;
        }
      }
    }

    /* create meta data for chunk and complete it */
    unsigned long full_chunk_filesize = scr_filesize(full_chunk_filename);
    scr_meta* meta_chunk = scr_meta_new();
    scr_meta_set_filename(meta_chunk, full_chunk_filename);
    scr_meta_set_filetype(meta_chunk, SCR_META_FILE_XOR);
    scr_meta_set_filesize(meta_chunk, full_chunk_filesize);
    scr_meta_set_complete(meta_chunk, 1);
    /* TODODSET: move the ranks field elsewhere, for now it's needed by scr_index.c */
    scr_meta_set_ranks(meta_chunk, scr_ranks_world);
    scr_filemap_set_meta(map, id, scr_my_rank_world, full_chunk_filename, meta_chunk);
    scr_filemap_write(scr_map_file, map);
    scr_meta_delete(meta_chunk);

    /* if crc_on_copy is set, compute and store CRC32 value for chunk file */
    if (scr_crc_on_copy) {
      /* TODO: would be nice to check for mismatches here, but we did not save this value in the partner XOR file */
      scr_compute_crc(map, id, scr_my_rank_world, full_chunk_filename);
    }
  }

  /* free the buffers */
  scr_align_free(recv_buf);
  scr_align_free(send_buf);
  if (filesizes != NULL) {
    free(filesizes);
    filesizes = NULL;
  }
  if (filenames != NULL) {
    /* free each of the filenames we strdup'd */
    for (i=0; i < num_files; i++) {
      if (filenames[i] != NULL) {
        free(filenames[i]);
        filenames[i] = NULL;
      }
    }
    free(filenames);
    filenames = NULL;
  }
  if (fds != NULL) {
    free(fds);
    fds = NULL;
  }
  scr_hash_delete(header);

  return rc;
}

/* given a dataset id, check whether files can be rebuilt via xor and execute the rebuild if needed */
static int scr_attempt_rebuild_xor(scr_filemap* map, const struct scr_reddesc* c, int id)
{
  /* check whether we have our files */
  int have_my_files = scr_bool_have_files(map, id, scr_my_rank_world);

  /* check whether we have our XOR file */
  char xor_file[SCR_MAX_FILENAME];
  if (! scr_bool_have_xor_file(map, id, xor_file)) {
    have_my_files = 0;
  }

  /* TODO: check whether each of the files listed in our xor file exists? */

  /* check whether I have my full checkpoint file, assume I don't */
  int need_rebuild = 1;
  if (have_my_files) {
    need_rebuild = 0;
  }

  /* count how many in my xor set need to rebuild */
  int total_rebuild;
  MPI_Allreduce(&need_rebuild, &total_rebuild, 1, MPI_INT, MPI_SUM, c->comm); 

  /* check whether all sets can rebuild, if not, bail out */
  int set_can_rebuild = (total_rebuild <= 1);
  if (! scr_alltrue(set_can_rebuild)) {
    if (scr_my_rank_world == 0) {
      scr_err("Cannot rebuild missing files @ %s:%d", __FILE__, __LINE__);
    }
    return SCR_FAILURE;
  }

  /* it's possible to rebuild; rebuild if we need to */
  int rc = SCR_SUCCESS;
  if (total_rebuild > 0) {
    /* someone in my set needs to rebuild, determine who */
    int tmp_rank = need_rebuild ? c->my_rank : -1;
    int rebuild_rank;
    MPI_Allreduce(&tmp_rank, &rebuild_rank, 1, MPI_INT, MPI_MAX, c->comm);

    /* rebuild */
    if (need_rebuild) {
      scr_dbg(1, "Rebuilding file from XOR segments");
    }
    rc = scr_rebuild_xor(map, c, id, rebuild_rank);
  }

  /* check whether all sets rebuilt ok */
  if (! scr_alltrue(rc == SCR_SUCCESS)) {
    if (scr_my_rank_world == 0) {
      scr_dbg(1, "One or more processes failed to rebuild its files @ %s:%d", __FILE__, __LINE__);
    }
    return SCR_FAILURE;
  }

  return SCR_SUCCESS;
}

/* given a filemap, a dataset, and a rank, unlink those files and remove them from the map */
static int scr_unlink_rank(scr_filemap* map, int id, int rank)
{
  /* delete each file and remove its metadata file */
  scr_hash_elem* file_elem;
  for (file_elem = scr_filemap_first_file(map, id, rank);
       file_elem != NULL;
       file_elem = scr_hash_elem_next(file_elem))
  {
    char* file = scr_hash_elem_key(file_elem);
    if (file != NULL) {
      scr_dbg(2, "Delete file Dataset %d, Rank %d, File %s", id, rank, file);

      /* delete the file */
      unlink(file);

      /* remove the file from the map */
      scr_filemap_remove_file(map, id, rank, file);
    }
  }

  /* unset the expected number of files for this rank */
  scr_filemap_unset_expected_files(map, id, rank);

  /* write the new filemap to disk */
  scr_filemap_write(scr_map_file, map);

  return SCR_SUCCESS;
}

/* since on a restart we may end up with more or fewer ranks on a node than the previous run,
 * rely on the master to read in and distribute the filemap to other ranks on the node */
static int scr_scatter_filemaps(scr_filemap* my_map)
{
  /* allocate empty send hash */
  scr_hash* send_hash = scr_hash_new();

  /* if i'm the master on this node, read in all filemaps */
  if (scr_my_rank_local == 0) {
    /* create an empty filemap */
    scr_filemap* all_map = scr_filemap_new();

    /* read in the master map */
    scr_hash* hash = scr_hash_new();
    scr_hash_read(scr_master_map_file, hash);

    /* for each filemap listed in the master map */
    scr_hash_elem* elem;
    for (elem = scr_hash_elem_first(scr_hash_get(hash, "Filemap"));
         elem != NULL;
         elem = scr_hash_elem_next(elem))
    {
      /* get the filename of this filemap */
      char* file = scr_hash_elem_key(elem);

      /* read in the filemap */
      scr_filemap* tmp_map = scr_filemap_new();
      scr_filemap_read(file, tmp_map);

      /* merge it with the all_map */
      scr_filemap_merge(all_map, tmp_map);

      /* delete filemap */
      scr_filemap_delete(tmp_map);

      /* delete the file */
      unlink(file);
    }

    /* free the hash object */
    scr_hash_delete(hash);

    /* write out new local 0 filemap */
    if (scr_filemap_num_ranks(all_map) > 0) {
      scr_filemap_write(scr_map_file, all_map);
    }

    /* get global rank of each rank on this node */
    int* ranks = (int*) malloc(scr_ranks_local * sizeof(int));
    if (ranks == NULL) {
      scr_abort(-1,"Failed to allocate memory to record local rank list @ %s:%d",
         __FILE__, __LINE__
      );
    }
    MPI_Gather(&scr_my_rank_world, 1, MPI_INT, ranks, 1, MPI_INT, 0, scr_comm_local);

    /* for each rank on this node, send them their own file data if we have it */
    int i;
    for (i=0; i < scr_ranks_local; i++) {
      int rank = ranks[i];
      if (scr_filemap_have_rank(all_map, rank)) {
        /* extract the filemap for this rank */
        scr_filemap* tmp_map = scr_filemap_extract_rank(all_map, rank);

        /* get a reference to the hash object that we'll send to this rank,
         * and merge this filemap into it */
        scr_hash* tmp_hash = scr_hash_getf(send_hash, "%d", i);
        if (tmp_hash == NULL) {
          /* if we don't find an existing entry in the send_hash,
           * create an empty hash and insert it */
          scr_hash* empty_hash = scr_hash_new();
          scr_hash_setf(send_hash, empty_hash, "%d", i);
          tmp_hash = empty_hash;
        }
        scr_hash_merge(tmp_hash, tmp_map);

        /* delete the filemap for this rank */
        scr_filemap_delete(tmp_map);
      }
    }

    /* free our rank list */
    if (ranks != NULL) {
      free(ranks);
      ranks = NULL;
    }

    /* now just round robin the remainder across the set (load balancing) */
    int num;
    int* remaining_ranks = NULL;
    scr_filemap_list_ranks(all_map, &num, &remaining_ranks);

    int j = 0;
    while (j < num) {
      /* pick a rank in scr_ranks_local to send to */
      i = j % scr_ranks_local;

      /* extract the filemap for this rank */
      scr_filemap* tmp_map = scr_filemap_extract_rank(all_map, remaining_ranks[j]);

      /* get a reference to the hash object that we'll send to this rank,
       * and merge this filemap into it */
      scr_hash* tmp_hash = scr_hash_getf(send_hash, "%d", i);
      if (tmp_hash == NULL) {
        /* if we don't find an existing entry in the send_hash,
         * create an empty hash and insert it */
        scr_hash* empty_hash = scr_hash_new();
        scr_hash_setf(send_hash, empty_hash, "%d", i);
        tmp_hash = empty_hash;
      }
      scr_hash_merge(tmp_hash, tmp_map);

      /* delete the filemap for this rank */
      scr_filemap_delete(tmp_map);
      j++;
    }

    if (remaining_ranks != NULL) {
      free(remaining_ranks);
      remaining_ranks = NULL;
    }

    /* delete the filemap */
    scr_filemap_delete(all_map);

    /* write out the new master filemap */
    hash = scr_hash_new();
    char file[SCR_MAX_FILENAME];
    for (i=0; i < scr_ranks_local; i++) {
      sprintf(file, "%s/filemap_%d.scrinfo", scr_cntl_prefix, i);
      scr_hash_set_kv(hash, "Filemap", file);
    }
    scr_hash_write(scr_master_map_file, hash);
    scr_hash_delete(hash);
  } else {
    /* send our global rank to the master */
    MPI_Gather(&scr_my_rank_world, 1, MPI_INT, NULL, 1, MPI_INT, 0, scr_comm_local);
  }

  /* receive our filemap from master */
  scr_hash* recv_hash = scr_hash_new();
  scr_hash_exchange(send_hash, recv_hash, scr_comm_local);

  /* merge map sent from master into our map */
  scr_hash* map_from_master = scr_hash_getf(recv_hash, "%d", 0);
  if (map_from_master != NULL) {
    scr_hash_merge(my_map, map_from_master);
  }

  /* write out our local filemap */
  if (scr_filemap_num_ranks(my_map) > 0) {
    scr_filemap_write(scr_map_file, my_map);
  }

  /* free off our send and receive hashes */
  scr_hash_delete(recv_hash);
  scr_hash_delete(send_hash);

  return SCR_SUCCESS;
}

/* broadcast dataset hash from smallest rank we can find that has a copy */
static int scr_distribute_datasets(scr_filemap* map, int id)
{
  int i;

  /* create a new hash to record dataset descriptor */
  scr_hash* send_hash = scr_hash_new();

  /* for this dataset, get list of ranks we have data for */
  int  nranks = 0;
  int* ranks = NULL;
  scr_filemap_list_ranks_by_dataset(map, id, &nranks, &ranks);

  /* for each rank we have files for, check whether we also have its dataset descriptor */
  int invalid_rank_found = 0;
  int have_dset = 0;
  for (i=0; i < nranks; i++) {
    /* get the rank id */
    int rank = ranks[i];

    /* check that the rank is within range */
    if (rank < 0 || rank >= scr_ranks_world) {
      scr_err("Invalid rank id %d in world of %d @ %s:%d",
         rank, scr_ranks_world, __FILE__, __LINE__
      );
      invalid_rank_found = 1;
    }

    /* lookup the dataset descriptor hash for this rank */
    scr_hash* desc = scr_hash_new();
    scr_filemap_get_dataset(map, id, rank, desc);

    /* if this descriptor has entries, add it to our send hash, delete the hash otherwise */
    if (scr_hash_size(desc) > 0) {
      have_dset = 1;
      scr_hash_merge(send_hash, desc);
      scr_hash_delete(desc);
      break;
    } else {
      scr_hash_delete(desc);
    }
  }

  /* free off our list of ranks */
  if (ranks != NULL) {
    free(ranks);
    ranks = NULL;
  }

  /* check that we didn't find an invalid rank on any process */
  if (! scr_alltrue(invalid_rank_found == 0)) {
    scr_hash_delete(send_hash);
    return SCR_FAILURE;
  }

  /* identify the smallest rank that has the dataset */
  int source_rank = scr_ranks_world;
  if (have_dset) {
    source_rank = scr_my_rank_world;
  }
  int min_rank;
  MPI_Allreduce(&source_rank, &min_rank, 1, MPI_INT, MPI_MIN, scr_comm_world);

  /* if there is no rank, return with failure */
  if (min_rank >= scr_ranks_world) {
    scr_hash_delete(send_hash);
    return SCR_FAILURE;
  }

  /* otherwise, bcast the dataset from the minimum rank */
  if (scr_my_rank_world != min_rank) {
    scr_hash_unset_all(send_hash);
  }
  scr_hash_bcast(send_hash, min_rank, scr_comm_world);

  /* record the descriptor in our filemap */
  scr_filemap_set_dataset(map, id, scr_my_rank_world, send_hash);
  scr_filemap_write(scr_map_file, map);

  /* TODO: at this point, we could delete descriptors for other ranks for this checkpoint */

  /* free off our send hash */
  scr_hash_delete(send_hash);

  return SCR_SUCCESS;
}

/* this transfers redundancy descriptors for the given dataset id */
static int scr_distribute_reddescs(scr_filemap* map, int id, struct scr_reddesc* c)
{
  int i;

  /* create a new hash to record redundancy descriptors that we have */
  scr_hash* send_hash = scr_hash_new();

  /* for this dataset, get list of ranks we have data for */
  int  nranks = 0;
  int* ranks = NULL;
  scr_filemap_list_ranks_by_dataset(map, id, &nranks, &ranks);

  /* for each rank we have files for, check whether we also have its redundancy descriptor */
  int invalid_rank_found = 0;
  for (i=0; i < nranks; i++) {
    /* get the rank id */
    int rank = ranks[i];

    /* check that the rank is within range */
    if (rank < 0 || rank >= scr_ranks_world) {
      scr_err("Invalid rank id %d in world of %d @ %s:%d",
         rank, scr_ranks_world, __FILE__, __LINE__
      );
      invalid_rank_found = 1;
    }

    /* lookup the redundancy descriptor hash for this rank */
    scr_hash* desc = scr_hash_new();
    scr_filemap_get_desc(map, id, rank, desc);

    /* if this descriptor has entries, add it to our send hash, delete the hash otherwise */
    if (scr_hash_size(desc) > 0) {
      scr_hash_setf(send_hash, desc, "%d", rank);
    } else {
      scr_hash_delete(desc);
    }
  }

  /* free off our list of ranks */
  if (ranks != NULL) {
    free(ranks);
    ranks = NULL;
  }

  /* check that we didn't find an invalid rank on any process */
  if (! scr_alltrue(invalid_rank_found == 0)) {
    scr_hash_delete(send_hash);
    return SCR_FAILURE;
  }

  /* create an empty hash to receive any incoming descriptors */
  /* exchange descriptors with other ranks */
  scr_hash* recv_hash = scr_hash_new();
  scr_hash_exchange(send_hash, recv_hash, scr_comm_world);

  /* check that everyone can get their descriptor */
  int num_desc = scr_hash_size(recv_hash);
  if (! scr_alltrue(num_desc > 0)) {
    scr_hash_delete(recv_hash);
    scr_hash_delete(send_hash);
    scr_dbg(2, "Cannot find process that has my redundancy descriptor @ %s:%d", __FILE__, __LINE__);
    return SCR_FAILURE;
  }

  /* just go with the first redundancy descriptor in our list -- they should all be the same */
  scr_hash_elem* desc_elem = scr_hash_elem_first(recv_hash);
  scr_hash* desc_hash = scr_hash_elem_hash(desc_elem);

  /* record the descriptor in our filemap */
  scr_filemap_set_desc(map, id, scr_my_rank_world, desc_hash);
  scr_filemap_write(scr_map_file, map);

  /* TODO: at this point, we could delete descriptors for other ranks for this checkpoint */

  /* read our redundancy descriptor from the map */
  scr_reddesc_create_from_filemap(map, id, scr_my_rank_world, c);

  /* free off our send and receive hashes */
  scr_hash_delete(recv_hash);
  scr_hash_delete(send_hash);

  return SCR_SUCCESS;
}

/* this moves all files in the cache to make them accessible to new rank mapping */
static int scr_distribute_files(scr_filemap* map, const struct scr_reddesc* c, int id)
{
  int i, round;
  int rc = SCR_SUCCESS;

  /* clean out any incomplete files before we start */
  scr_cache_clean(map);

  /* for this dataset, get list of ranks we have data for */
  int  nranks = 0;
  int* ranks = NULL;
  scr_filemap_list_ranks_by_dataset(map, id, &nranks, &ranks);

  /* walk backwards through the list of ranks, and set our start index to the rank which
   * is the first rank that is equal to or higher than our own rank -- when we assign round
   * ids below, this offsetting helps distribute the load */
  int start_index = 0;
  int invalid_rank_found = 0;
  for (i = nranks-1; i >= 0; i--) {
    int rank = ranks[i];

    /* pick the first rank whose rank id is equal to or higher than our own */
    if (rank >= scr_my_rank_world) {
      start_index = i;
    }

    /* while we're at it, check that the rank is within range */
    if (rank < 0 || rank >= scr_ranks_world) {
      scr_err("Invalid rank id %d in world of %d @ %s:%d",
         rank, scr_ranks_world, __FILE__, __LINE__
      );
      invalid_rank_found = 1;
    }
  }

  /* check that we didn't find an invalid rank on any process */
  if (! scr_alltrue(invalid_rank_found == 0)) {
    if (ranks != NULL) {
      free(ranks);
      ranks = NULL;
    }
    return SCR_FAILURE;
  }

  /* allocate array to record the rank we can send to in each round */
  int* have_rank_by_round = (int*) malloc(sizeof(int) * nranks);
  int* send_flag_by_round = (int*) malloc(sizeof(int) * nranks);
  if (have_rank_by_round == NULL || send_flag_by_round == NULL) {
    scr_abort(-1,"Failed to allocate memory to record rank id by round @ %s:%d",
       __FILE__, __LINE__
    );
  }

  /* check that we have all of the files for each rank, and determine the round we can send them */
  scr_hash* send_hash = scr_hash_new();
  scr_hash* recv_hash = scr_hash_new();
  for (round = 0; round < nranks; round++) {
    /* get the rank id */
    int index = (start_index + round) % nranks;
    int rank = ranks[index];

    /* record the rank indexed by the round number */
    have_rank_by_round[round] = rank;

    /* assume we won't be sending to this rank in this round */
    send_flag_by_round[round] = 0;

    /* if we have files for this rank, specify the round we can send those files in */
    if (scr_bool_have_files(map, id, rank)) {
      scr_hash_setf(send_hash, NULL, "%d %d", rank, round);
    }
  }
  scr_hash_exchange(send_hash, recv_hash, scr_comm_world);

  /* search for the minimum round we can get our files */
  int retrieve_rank  = -1;
  int retrieve_round = -1;
  scr_hash_elem* elem = NULL;
  for (elem = scr_hash_elem_first(recv_hash);
       elem != NULL;
       elem = scr_hash_elem_next(elem))
  {
    /* get the rank id */
    int rank = scr_hash_elem_key_int(elem);

    /* get the round id */
    scr_hash* round_hash = scr_hash_elem_hash(elem);
    scr_hash_elem* round_elem = scr_hash_elem_first(round_hash);
    char* round_str = scr_hash_elem_key(round_elem);
    int round = atoi(round_str);

    /* record this round and rank number if it's less than the current round */
    if (round < retrieve_round || retrieve_round == -1) {
      retrieve_round = round;
      retrieve_rank  = rank;
    }
  }

  /* done with the round hashes, free them off */
  scr_hash_delete(recv_hash);
  scr_hash_delete(send_hash);

  /* free off our list of ranks */
  if (ranks != NULL) {
    free(ranks);
    ranks = NULL;
  }

  /* for some redundancy schemes, we know at this point whether we can recover all files */
  int can_get_files = (retrieve_rank != -1);
  if (c->copy_type != SCR_COPY_XOR && !scr_alltrue(can_get_files)) {
    /* print a debug message indicating which rank is missing files */
    if (! can_get_files) {
      scr_dbg(2, "Cannot find process that has my checkpoint files @ %s:%d", __FILE__, __LINE__);
    }
    return SCR_FAILURE;
  }

  /* get the maximum retrieve round */
  int max_rounds = 0;
  MPI_Allreduce(&retrieve_round, &max_rounds, 1, MPI_INT, MPI_MAX, scr_comm_world);

  /* tell destination which round we'll take our files in */
  send_hash = scr_hash_new();
  recv_hash = scr_hash_new();
  if (retrieve_rank != -1) {
    scr_hash_setf(send_hash, NULL, "%d %d", retrieve_rank, retrieve_round);
  }
  scr_hash_exchange(send_hash, recv_hash, scr_comm_world);

  /* determine which ranks want to fetch their files from us */
  for(elem = scr_hash_elem_first(recv_hash);
      elem != NULL;
      elem = scr_hash_elem_next(elem))
  {
    /* get the round id */
    scr_hash* round_hash = scr_hash_elem_hash(elem);
    scr_hash_elem* round_elem = scr_hash_elem_first(round_hash);
    char* round_str = scr_hash_elem_key(round_elem);
    int round = atoi(round_str);

    /* record whether this rank wants its files from us */
    if (round >= 0 && round < nranks) {
      send_flag_by_round[round] = 1;
    }
  }

  /* done with the round hashes, free them off */
  scr_hash_delete(recv_hash);
  scr_hash_delete(send_hash);

  int tmp_rc = 0;

  /* get the path for this dataset */
  char dir[SCR_MAX_FILENAME];
  scr_cache_dir_get(c, id, dir);

  /* run through rounds and exchange files */
  for (round = 0; round <= max_rounds; round++) {
    /* assume we don't need to send or receive any files this round */
    int send_rank = MPI_PROC_NULL;
    int recv_rank = MPI_PROC_NULL;
    int send_num  = 0;
    int recv_num  = 0;

    /* check whether I can potentially send to anyone in this round */
    if (round < nranks) {
      /* have someone's files, check whether they are asking for them this round */
      if (send_flag_by_round[round]) {
        /* need to send files this round, remember to whom and how many */
        int dst_rank = have_rank_by_round[round];
        send_rank = dst_rank;
        send_num  = scr_filemap_num_files(map, id, dst_rank);
      }
    }

    /* if I'm supposed to get my files this round, set the recv_rank */
    if (retrieve_round == round) {
      recv_rank = retrieve_rank;
    }

    /* TODO: another special case is to just move files if the processes are on the same node */

    /* if i'm sending to myself, just move (rename) each file */
    if (send_rank == scr_my_rank_world) {
      /* get our file list */
      int numfiles = 0;
      char** files = NULL;
      scr_filemap_list_files(map, id, send_rank, &numfiles, &files);

      /* iterate over and rename each file */
      for (i=0; i < numfiles; i++) {
        /* get the existing filename and split into path and name components */
        char* file = files[i];
        char path[SCR_MAX_FILENAME];
        char name[SCR_MAX_FILENAME];
        scr_split_path(file, path, name);

        /* build the new filename */
        char newfile[SCR_MAX_FILENAME];
        scr_build_path(newfile, sizeof(newfile), dir, name);

        /* if the new file name is different from the old name, rename it */
        if (strcmp(file, newfile) != 0) {
          /* record the new filename to our map and write it to disk */
          scr_filemap_add_file(map, id, send_rank, newfile);
          scr_meta* oldmeta = scr_meta_new();
          scr_filemap_get_meta(map, id, send_rank, file, oldmeta);
          scr_filemap_set_meta(map, id, send_rank, newfile, oldmeta);
          scr_filemap_write(scr_map_file, map);
          scr_meta_delete(oldmeta);

          /* rename the file */
          scr_dbg(2, "Round %d: rename(%s, %s)", round, file, newfile);
          tmp_rc = rename(file, newfile);
          if (tmp_rc != 0) {
            /* TODO: to cross mount points, if tmp_rc == EXDEV, open new file, copy, and delete orig */
            scr_err("Moving checkpoint file: rename(%s, %s) %m errno=%d @ %s:%d",
                    file, newfile, errno, __FILE__, __LINE__
            );
            rc = SCR_FAILURE;
          }

          /* remove the old name from the filemap and write it to disk */
          scr_filemap_remove_file(map, id, send_rank, file);
          scr_filemap_write(scr_map_file, map);
        }
      }

      /* free the list of filename pointers */
      if (files != NULL) {
        free(files);
        files = NULL;
      }
    } else {
      /* if we have files for this round, but the correspdonding rank doesn't need them, delete the files */
      if (round < nranks && send_rank == MPI_PROC_NULL) {
        int dst_rank = have_rank_by_round[round];
        scr_unlink_rank(map, id, dst_rank);
      }

      /* sending to and/or recieving from another node */
      if (send_rank != MPI_PROC_NULL || recv_rank != MPI_PROC_NULL) {
        /* remember the send rank for later, we'll set it to -1 eventually */
        int filemap_send_rank = send_rank;

        /* have someone to send to or receive from */
        int have_outgoing = 0;
        int have_incoming = 0;
        if (send_rank != MPI_PROC_NULL) {
          have_outgoing = 1;
        }
        if (recv_rank != MPI_PROC_NULL) {
          have_incoming = 1;
        }

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
        if (num_req > 0) {
          MPI_Waitall(num_req, request, status);
        }

        /* record how many files I will receive (need to distinguish between 0 files and not knowing) */
        if (have_incoming) {
          scr_filemap_set_expected_files(map, id, scr_my_rank_world, recv_num);
        }

        /* turn off send or receive flags if the file count is 0, nothing else to do */
        if (send_num == 0) {
          have_outgoing = 0;
          send_rank = MPI_PROC_NULL;
        }
        if (recv_num == 0) {
          have_incoming = 0;
          recv_rank = MPI_PROC_NULL;
        }

        /* get our file list for the destination */
        int numfiles = 0;
        char** files = NULL;
        if (have_outgoing) {
          scr_filemap_list_files(map, id, send_rank, &numfiles, &files);
        }

        /* while we have a file to send or receive ... */
        while (have_incoming || have_outgoing) {
          /* get the filename */
          char* file = NULL;
          scr_meta* send_meta = NULL;
          if (have_outgoing) {
            file = files[numfiles - send_num];
            send_meta = scr_meta_new();
            scr_filemap_get_meta(map, id, send_rank, file, send_meta);
          }

          /* exhange file names with partners */
          char file_partner[SCR_MAX_FILENAME];
          scr_swap_file_names(file, send_rank, file_partner, sizeof(file_partner), recv_rank,
                              dir, scr_comm_world
          );

          /* if we'll receive a file, record the name of our file in the filemap and write it to disk */
          scr_meta* recv_meta = NULL;
          if (recv_rank != MPI_PROC_NULL) {
            recv_meta = scr_meta_new();
            scr_filemap_add_file(map, id, scr_my_rank_world, file_partner);
            scr_filemap_write(scr_map_file, map);
          }

          /* either sending or receiving a file this round, since we move files,
           * it will be deleted or overwritten */
          char newfile[SCR_MAX_FILENAME];
          if (scr_swap_files(MOVE_FILES, file, send_meta, send_rank, file_partner, recv_meta, recv_rank, scr_comm_world)
                != SCR_SUCCESS)
          {
            scr_err("Swapping files: %s to %d, %s from %d @ %s:%d",
                    file, send_rank, file_partner, recv_rank, __FILE__, __LINE__
            );
            rc = SCR_FAILURE;
          }

          /* if we received a file, record its meta data and decrement our receive count */
          if (have_incoming) {
            /* record meta data for the file we received */
            scr_filemap_set_meta(map, id, scr_my_rank_world, file_partner, recv_meta);
            scr_meta_delete(recv_meta);

            /* decrement receive count */
            recv_num--;
            if (recv_num == 0) {
              have_incoming = 0;
              recv_rank = MPI_PROC_NULL;
            }
          }

          /* if we sent a file, remove it from the filemap and decrement our send count */
          if (have_outgoing) {
            /* remove file from the filemap */
            scr_filemap_remove_file(map, id, send_rank, file);
            scr_meta_delete(send_meta);

            /* decrement our send count */
            send_num--;
            if (send_num == 0) {
              have_outgoing = 0;
              send_rank = MPI_PROC_NULL;
            }
          }

          /* update filemap on disk */
          scr_filemap_write(scr_map_file, map);
        }

        /* free our file list */
        if (files != NULL) {
          free(files);
          files = NULL;
        }
      }
    }
  }

  /* if we have more rounds than max rounds, delete the remainder of our files */
  for (round = max_rounds+1; round < nranks; round++) {
    /* have someone's files for this round, so delete them */
    int dst_rank = have_rank_by_round[round];
    scr_unlink_rank(map, id, dst_rank);
  }

  if (send_flag_by_round != NULL) {
    free(send_flag_by_round);
    send_flag_by_round = NULL;
  }
  if (have_rank_by_round != NULL) {
    free(have_rank_by_round);
    have_rank_by_round = NULL;
  }

  /* write out new filemap and free the memory resources */
  scr_filemap_write(scr_map_file, map);

  /* clean out any incomplete files */
  scr_cache_clean(map);

  /* TODO: if the exchange or redundancy rebuild failed, we should also delete any *good* files we received */

  /* return whether distribute succeeded, it does not ensure we have all of our files,
   * only that the transfer completed without failure */
  return rc;
}

/* rebuilds files for specified dataset id using specified redundancy descriptor,
 * adds them to filemap, and returns SCR_SUCCESS if all processes succeeded */
static int scr_rebuild_files(scr_filemap* map, const struct scr_reddesc* c, int id)
{
  int rc = SCR_SUCCESS;

  /* for xor, need to call rebuild_xor here */
  if (c->copy_type == SCR_COPY_XOR) {
    rc = scr_attempt_rebuild_xor(map, c, id);
  }

  /* check that rebuild worked */
  if (rc != SCR_SUCCESS) {
    if (scr_my_rank_world == 0) {
      scr_dbg(1, "Missing files @ %s:%d",
              __FILE__, __LINE__
      );
    }
    return SCR_FAILURE;
  }

  /* at this point, we should have all of our files, check that they're all here */

  /* check whether everyone has their files */
  int have_my_files = scr_bool_have_files(map, id, scr_my_rank_world);
  if (! scr_alltrue(have_my_files)) {
    if (scr_my_rank_world == 0) {
      scr_dbg(1, "Missing files @ %s:%d",
              __FILE__, __LINE__
      );
    }
    return SCR_FAILURE;
  }

  /* for LOCAL and PARTNER, we need to apply the copy to complete the rebuild,
   * with XOR the copy is done as part of the rebuild process */
  if (c->copy_type == SCR_COPY_LOCAL || c->copy_type == SCR_COPY_PARTNER) {
    double bytes_copied = 0.0;
    rc = scr_copy_files(map, c, id, &bytes_copied);
  }

  return rc;
}

/* distribute and rebuild files in cache */
static int scr_cache_rebuild(scr_filemap* map)
{
  int rc = SCR_FAILURE;

  double time_start, time_end, time_diff;

  /* start timer */
  time_t time_t_start;
  if (scr_my_rank_world == 0) {
    time_t_start = scr_log_seconds();
    time_start = MPI_Wtime();
  }

  /* we set this variable to 1 if we actually try to distribute files for a restart */
  int distribute_attempted = 0;

  /* clean any incomplete files from our cache */
  scr_cache_clean(map);

  /* get the list of datasets we have in our cache */
  int ndsets;
  int* dsets;
  scr_filemap_list_datasets(map, &ndsets, &dsets);

  /* TODO: put dataset selection logic into a function */

  /* TODO: also attempt to recover datasets which we were in the middle of flushing */
  int current_id;
  int dset_index = 0;
  do {
    /* get the smallest index across all processes (returned in current_id),
     * this also updates our dset_index value if appropriate */
    scr_next_dataset(ndsets, dsets, &dset_index, &current_id);

    /* if we found a dataset, try to distribute and rebuild it */
    if (current_id != -1) {
      /* remember that we made an attempt to distribute at least one dataset */
      distribute_attempted = 1;
      
      /* log the attempt */
      if (scr_my_rank_world == 0) {
        scr_dbg(1, "Attempting to distribute and rebuild dataset %d", current_id);
        if (scr_log_enable) {
          time_t now = scr_log_seconds();
          scr_log_event("REBUILD STARTED", NULL, &current_id, &now, NULL);
        }
      }

      /* distribute dataset descriptor for this dataset */
      int rebuild_succeeded = 0;
      if (scr_distribute_datasets(map, current_id) == SCR_SUCCESS) {
        /* distribute redundancy descriptor for this dataset */
        struct scr_reddesc c;
        if (scr_distribute_reddescs(map, current_id, &c) == SCR_SUCCESS) {
          /* create a directory for this dataset */
          scr_cache_dir_create(&c, current_id);

          /* distribute the files for this dataset */
          scr_distribute_files(map, &c, current_id);

          /* rebuild files for this dataset */
          int tmp_rc = scr_rebuild_files(map, &c, current_id);
          if (tmp_rc == SCR_SUCCESS) {
            /* rebuild succeeded */
            rebuild_succeeded = 1;

            /* if we rebuild any checkpoint, return success */
            rc = SCR_SUCCESS;

            /* update scr_dataset_id */
            if (current_id > scr_dataset_id) {
              scr_dataset_id = current_id;
            }

            /* TODO: dataset may not be a checkpoint */
            /* update scr_checkpoint_id */
            if (current_id > scr_checkpoint_id) {
              scr_checkpoint_id = current_id;
            }

            /* update our flush file to indicate this dataset is in cache */
            scr_flush_file_location_set(current_id, SCR_FLUSH_KEY_LOCATION_CACHE);

            /* TODO: if storing flush file in control directory on each node, if we find
             * any process that has marked the dataset as flushed, marked it as flushed
             * in every flush file */

            /* TODO: would like to restore flushing status to datasets that were in the middle of a flush,
             * but we need to better manage the transfer file to do this, so for now just forget about flushing
             * this dataset */
            scr_flush_file_location_unset(current_id, SCR_FLUSH_KEY_LOCATION_FLUSHING);
          }

          /* free redundancy descriptor */
          scr_reddesc_free(&c);
        }
      }

      /* if the distribute or rebuild failed, delete the dataset */
      if (! rebuild_succeeded) {
        /* log that we failed */
        if (scr_my_rank_world == 0) {
          scr_dbg(1, "Failed to distribute and rebuild dataset %d", current_id);
          if (scr_log_enable) {
            time_t now = scr_log_seconds();
            scr_log_event("REBUILD FAILED", NULL, &current_id, &now, NULL);
          }
        }

        /* rebuild failed, delete this dataset from cache */
        scr_cache_delete(map, current_id);
      } else {
        /* rebuid worked, log success */
        if (scr_my_rank_world == 0) {
          scr_dbg(1, "Rebuilt dataset %d", current_id);
          if (scr_log_enable) {
            time_t now = scr_log_seconds();
            scr_log_event("REBUILD SUCCEEDED", NULL, &current_id, &now, NULL);
          }
        }
      }
    }
  } while (current_id != -1);

  /* stop timer and report performance */
  if (scr_my_rank_world == 0) {
    time_end = MPI_Wtime();
    time_diff = time_end - time_start;

    if (distribute_attempted) {
      if (rc == SCR_SUCCESS) {
        scr_dbg(1, "Scalable restart succeeded for checkpoint %d, took %f secs",
                scr_checkpoint_id, time_diff
        );
        if (scr_log_enable) {
          time_t now = scr_log_seconds();
          scr_log_event("RESTART SUCCEEDED", NULL, &scr_checkpoint_id, &time_t_start, &time_diff);
        }
      } else {
        /* scr_checkpoint_id is not defined */
        scr_dbg(1, "Scalable restart failed, took %f secs", time_diff);
        if (scr_log_enable) {
          time_t now = scr_log_seconds();
          scr_log_event("RESTART FAILED", NULL, NULL, &time_t_start, &time_diff);
        }
      }
    }
  }

  /* free our list of dataset ids */
  if (dsets != NULL) {
    free(dsets);
    dsets = NULL;
  }

  return rc;
}

/* attempt to fetch most recent checkpoint from prefix directory into cache */
static int scr_cache_fetch(scr_filemap* map, int* fetch_attempted)
{
  /* we only return success if we successfully fetch a checkpoint */
  int rc = SCR_FAILURE;

  double time_start, time_end, time_diff;

  /* start timer */
  if (scr_my_rank_world == 0) {
    time_start = MPI_Wtime();
  }

  /* build the filename for the current symlink */
  char scr_current[SCR_MAX_FILENAME];
  scr_build_path(scr_current, sizeof(scr_current), scr_par_prefix, SCR_CURRENT_LINK);

  /* have rank 0 read the index file */
  scr_hash* index_hash = NULL;
  int read_index_file = 0;
  if (scr_my_rank_world == 0) {
    /* create an empty hash to store our index */
    index_hash = scr_hash_new();

    /* read the index file */
    if (scr_index_read(scr_par_prefix, index_hash) == SCR_SUCCESS) {
      /* remember that we read the index file ok, so we know we can write to it later
       * this way we don't overwrite an existing index file just because the read happened to fail */
      read_index_file = 1;
    }
  }

  /* now start fetching, we keep trying until we exhaust all valid checkpoints */
  char target[SCR_MAX_FILENAME];
  char fetch_dir[SCR_MAX_FILENAME];
  int current_checkpoint_id = -1;
  int continue_fetching = 1;
  while (continue_fetching) {
    /* initialize our target and fetch directory values to empty strings */
    strcpy(target, "");
    strcpy(fetch_dir, "");

    /* rank 0 determines the directory to fetch from */
    if (scr_my_rank_world == 0) {
      /* read the target of the current symlink if there is one */
      if (access(scr_current, R_OK) == 0) {
        int target_len = readlink(scr_current, target, sizeof(target)-1);
        if (target_len >= 0) {
          target[target_len] = '\0';
        }
      }

      /* if we read the index file, lookup the checkpoint id */
      if (read_index_file) {
        int next_checkpoint_id = -1;
        if (strcmp(target, "") != 0) {
          /* we have a subdirectory name, lookup the checkpoint id corresponding to this directory */
          scr_index_get_id_by_dir(index_hash, target, &next_checkpoint_id);
        } else {
          /* otherwise, just get the most recent complete checkpoint
           * (that's older than the current id) */
          scr_index_get_most_recent_complete(index_hash, current_checkpoint_id, &next_checkpoint_id, target);
        }
        current_checkpoint_id = next_checkpoint_id;

        /* TODODSET: need to verify that dataset is really a checkpoint and keep searching if not */
      }

      /* if we have a subdirectory (target) name, build the full fetch directory */
      if (strcmp(target, "") != 0) {
        /* record that we're attempting a fetch of this checkpoint */
        *fetch_attempted = 1;
        if (read_index_file && current_checkpoint_id != -1) {
          scr_index_mark_fetched(index_hash, current_checkpoint_id, target);
          scr_index_write(scr_par_prefix, index_hash);
        }

        /* we have a subdirectory, now build the full path */
        scr_build_path(fetch_dir, sizeof(fetch_dir), scr_par_prefix, target);
      }
    }

    /* now attempt to fetch the checkpoint */
    int dset_id, ckpt_id;
    rc = scr_fetch_files(map, fetch_dir, &dset_id, &ckpt_id);
    if (rc == SCR_SUCCESS) {
      /* set the dataset and checkpoint ids */
      scr_dataset_id = dset_id;
      scr_checkpoint_id = ckpt_id;

      /* we succeeded in fetching this checkpoint, set current to point to it, and stop fetching */
      if (scr_my_rank_world == 0) {
        symlink(target, scr_current);
      }
      continue_fetching = 0;
    } else {
      /* fetch failed, delete the current symlink */
      unlink(scr_current);

      /* if we had a fetch directory, mark it as failed so we don't try it again */
      if (strcmp(fetch_dir, "") != 0) {
        if (scr_my_rank_world == 0) {
          if (read_index_file && current_checkpoint_id != -1 && strcmp(target, "") != 0) {
            scr_index_mark_failed(index_hash, current_checkpoint_id, target);
            scr_index_write(scr_par_prefix, index_hash);
          }
        }
      } else {
        /* we ran out of valid checkpoints in the index file, bail out of the loop */
        continue_fetching = 0;
      }
    }
  }

  /* delete the index hash */
  if (scr_my_rank_world == 0) {
    scr_hash_delete(index_hash);
  }

  /* broadcast whether we actually attempted to fetch anything (only rank 0 knows) */
  MPI_Bcast(fetch_attempted, 1, MPI_INT, 0, scr_comm_world);

  /* stop timer for fetch */
  if (scr_my_rank_world == 0) {
    time_end = MPI_Wtime();
    time_diff = time_end - time_start;
    scr_dbg(1, "scr_fetch_files: return code %d, %f secs", rc, time_diff);
  }

  return rc;
}

/* given a dataset id and a filename, return the full path to the file which the user should write to */
static int scr_route_file(const struct scr_reddesc* c, int id, const char* file, char* newfile, int n)
{
  /* check that we got a file and newfile to write to */
  if (file == NULL || strcmp(file, "") == 0 || newfile == NULL) {
    return SCR_FAILURE;
  }

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
  char dir[SCR_MAX_FILENAME];
  scr_cache_dir_get(c, id, dir);

  /* build the composed name */
  if (scr_build_path(newfile, n, dir, name) != SCR_SUCCESS) {
    /* abort if the new name is longer than our buffer */
    scr_abort(-1, "file name (%s/%s) is longer than n (%d) @ %s:%d",
              dir, name, n, __FILE__, __LINE__
    );
  }

  return SCR_SUCCESS;
}

/* read in environment variables */
static int scr_get_params()
{
  char* value;
  scr_hash* tmp;
  double d;
  unsigned long long ull;

  /* user may want to disable SCR at runtime, read env var to avoid reading config files */
  if ((value = getenv("SCR_ENABLE")) != NULL) {
    scr_enabled = atoi(value);
  }

  /* if not enabled, bail with an error */
  if (! scr_enabled) {
    return SCR_FAILURE;
  }

  /* read in our configuration parameters */
  scr_param_init();

  /* check enabled parameter again, this time including settings from config files */
  if ((value = scr_param_get("SCR_ENABLE")) != NULL) {
    scr_enabled = atoi(value);
  }

  /* if not enabled, bail with an error */
  if (! scr_enabled) {
    scr_param_finalize();
    return SCR_FAILURE;
  }

  /* set debug verbosity level */
  if ((value = scr_param_get("SCR_DEBUG")) != NULL) {
    scr_debug = atoi(value);
  }

  /* set logging */
  if ((value = scr_param_get("SCR_LOG_ENABLE")) != NULL) {
    scr_log_enable = atoi(value);
  }

  /* read username from SCR_USER_NAME, if not set, try to read from environment */
  if ((value = scr_param_get("SCR_USER_NAME")) != NULL) {
    scr_username = strdup(value);
  } else {
    scr_username = scr_env_username();
  }

  /* check that the username is defined, fatal error if not */
  if (scr_username == NULL) {
    scr_abort(-1, "Failed to record username @ %s:%d",
            __FILE__, __LINE__
    );
  }

  /* read jobid from SCR_JOB_ID, if not set, try to read from environment */
  if ((value = scr_param_get("SCR_JOB_ID")) != NULL) {
    scr_jobid = strdup(value);
  } else {
    scr_jobid = scr_env_jobid();
  }

  /* check that the jobid is defined, fatal error if not */
  if (scr_jobid == NULL) {
    scr_abort(-1, "Failed to record jobid @ %s:%d",
            __FILE__, __LINE__
    );
  }

  /* read job name from SCR_JOB_NAME */
  if ((value = scr_param_get("SCR_JOB_NAME")) != NULL) {
    scr_jobname = strdup(value);
    if (scr_jobname == NULL) {
      scr_abort(-1, "Failed to allocate memory to record jobname (%s) @ %s:%d",
              value, __FILE__, __LINE__
      );
    }
  }

  /* read cluster name from SCR_CLUSTER_NAME, if not set, try to read from environment */
  if ((value = scr_param_get("SCR_CLUSTER_NAME")) != NULL) {
    scr_clustername = strdup(value);
  } else {
    scr_clustername = scr_env_cluster();
  }

  /* check that the cluster name is defined, fatal error if not */
  if (scr_clustername == NULL) {
    if (scr_my_rank_world == 0) {
      scr_warn("Failed to record cluster name @ %s:%d",
              __FILE__, __LINE__
      );
    }
  }

  /* override default base control directory */
  if ((value = scr_param_get("SCR_CNTL_BASE")) != NULL) {
    strcpy(scr_cntl_base, value);
  }

  /* override default base directory for checkpoint cache */
  if ((value = scr_param_get("SCR_CACHE_BASE")) != NULL) {
    strcpy(scr_cache_base, value);
  }

  /* set maximum number of checkpoints to keep in cache */
  if ((value = scr_param_get("SCR_CACHE_SIZE")) != NULL) {
    scr_cache_size = atoi(value);
  }

  /* fill in a hash of cache descriptors */
  scr_cachedesc_hash = scr_hash_new();
  tmp = scr_param_get_hash(SCR_CONFIG_KEY_CACHEDESC);
  if (tmp != NULL) {
    scr_hash_set(scr_cachedesc_hash, SCR_CONFIG_KEY_CACHEDESC, tmp);
  } else {
    /* fill in info for one CACHE type */
    tmp = scr_hash_set_kv(scr_cachedesc_hash, SCR_CONFIG_KEY_CACHEDESC, "0");
    scr_hash_util_set_str(tmp, SCR_CONFIG_KEY_BASE, scr_cache_base);
    scr_hash_util_set_int(tmp, SCR_CONFIG_KEY_SIZE, scr_cache_size);
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
      scr_copy_type = SCR_COPY_FILE;
    }
  }

  /* specify the number of tasks in xor set */
  if ((value = scr_param_get("SCR_SET_SIZE")) != NULL) {
    scr_set_size = atoi(value);
  }

  /* number of nodes between partners */
  if ((value = scr_param_get("SCR_HOP_DISTANCE")) != NULL) {
    scr_hop_distance = atoi(value);
  }

  /* fill in a hash of redundancy descriptors */
  scr_reddesc_hash = scr_hash_new();
  if (scr_copy_type == SCR_COPY_LOCAL) {
    /* fill in info for one LOCAL checkpoint */
    tmp = scr_hash_set_kv(scr_reddesc_hash, SCR_CONFIG_KEY_CKPTDESC, "0");
    scr_hash_util_set_str(tmp, SCR_CONFIG_KEY_BASE,         scr_cache_base);
    scr_hash_util_set_str(tmp, SCR_CONFIG_KEY_TYPE,         "LOCAL");
  } else if (scr_copy_type == SCR_COPY_PARTNER) {
    /* fill in info for one PARTNER checkpoint */
    tmp = scr_hash_set_kv(scr_reddesc_hash, SCR_CONFIG_KEY_CKPTDESC, "0");
    scr_hash_util_set_str(tmp, SCR_CONFIG_KEY_BASE,         scr_cache_base);
    scr_hash_util_set_str(tmp, SCR_CONFIG_KEY_TYPE,         "PARTNER");
    scr_hash_util_set_int(tmp, SCR_CONFIG_KEY_HOP_DISTANCE, scr_hop_distance);
  } else if (scr_copy_type == SCR_COPY_XOR) {
    /* fill in info for one XOR checkpoint */
    tmp = scr_hash_set_kv(scr_reddesc_hash, SCR_CONFIG_KEY_CKPTDESC, "0");
    scr_hash_util_set_str(tmp, SCR_CONFIG_KEY_BASE,         scr_cache_base);
    scr_hash_util_set_str(tmp, SCR_CONFIG_KEY_TYPE,         "XOR");
    scr_hash_util_set_int(tmp, SCR_CONFIG_KEY_HOP_DISTANCE, scr_hop_distance);
    scr_hash_util_set_int(tmp, SCR_CONFIG_KEY_SET_SIZE,     scr_set_size);
  } else {
    /* read info from our configuration files */
    tmp = scr_param_get_hash(SCR_CONFIG_KEY_CKPTDESC);
    if (tmp != NULL) {
      scr_hash_set(scr_reddesc_hash, SCR_CONFIG_KEY_CKPTDESC, tmp);
    } else {
      scr_abort(-1, "Failed to define checkpoints @ %s:%d",
              __FILE__, __LINE__
      );
    }
  }

  /* if job has fewer than SCR_HALT_SECONDS remaining after completing a checkpoint, halt it */
  if ((value = scr_param_get("SCR_HALT_SECONDS")) != NULL) {
    scr_halt_seconds = atoi(value);
  }

  /* set MPI buffer size (file chunk size) */
  if ((value = scr_param_get("SCR_MPI_BUF_SIZE")) != NULL) {
    if (scr_abtoull(value, &ull) == SCR_SUCCESS) {
      scr_mpi_buf_size = (size_t) ull;
    } else {
      scr_err("Failed to read SCR_MPI_BUF_SIZE successfully @ %s:%d", __FILE__, __LINE__);
    }
  }

  /* whether to distribute files in filemap to ranks in SCR_Init */
  if ((value = scr_param_get("SCR_DISTRIBUTE")) != NULL) {
    scr_distribute = atoi(value);
  }

  /* whether to fetch files from the parallel file system in SCR_Init */
  if ((value = scr_param_get("SCR_FETCH")) != NULL) {
    scr_fetch = atoi(value);
  }

  /* specify number of processes to read files simultaneously */
  if ((value = scr_param_get("SCR_FETCH_WIDTH")) != NULL) {
    scr_fetch_width = atoi(value);
  }

  /* specify how often we should flush files */
  if ((value = scr_param_get("SCR_FLUSH")) != NULL) {
    scr_flush = atoi(value);
  }

  /* specify number of processes to write files simultaneously */
  if ((value = scr_param_get("SCR_FLUSH_WIDTH")) != NULL) {
    scr_flush_width = atoi(value);
  }

  /* specify whether to always flush latest checkpoint from cache on restart */
  if ((value = scr_param_get("SCR_FLUSH_ON_RESTART")) != NULL) {
    scr_flush_on_restart = atoi(value);
  }

  /* set to 1 if code must be restarted from the parallel file system */
  if ((value = scr_param_get("SCR_GLOBAL_RESTART")) != NULL) {
    scr_global_restart = atoi(value);
  }

  /* specify whether to use asynchronous flush */
  if ((value = scr_param_get("SCR_FLUSH_ASYNC")) != NULL) {
    scr_flush_async = atoi(value);
  }

  /* bandwidth limit imposed during async flush (in bytes/sec) */
  if ((value = scr_param_get("SCR_FLUSH_ASYNC_BW")) != NULL) {
    if (scr_atod(value, &d) == SCR_SUCCESS) {
      scr_flush_async_bw = d;
    } else {
      scr_err("Failed to read SCR_FLUSH_ASYNC_BW successfully @ %s:%d", __FILE__, __LINE__);
    }
  }

  /* bandwidth limit imposed during async flush (in bytes/sec) */
  if ((value = scr_param_get("SCR_FLUSH_ASYNC_PERCENT")) != NULL) {
    if (scr_atod(value, &d) == SCR_SUCCESS) {
      scr_flush_async_percent = d;
    } else {
      scr_err("Failed to read SCR_FLUSH_ASYNC_PERCENT successfully @ %s:%d", __FILE__, __LINE__);
    }
  }

  /* set file copy buffer size (file chunk size) */
  if ((value = scr_param_get("SCR_FILE_BUF_SIZE")) != NULL) {
    if (scr_abtoull(value, &ull) == SCR_SUCCESS) {
      scr_file_buf_size = (size_t) ull;
    } else {
      scr_err("Failed to read SCR_FILE_BUF_SIZE successfully @ %s:%d", __FILE__, __LINE__);
    }
  }

  /* specify whether to compute CRC on redundancy copy */
  if ((value = scr_param_get("SCR_CRC_ON_COPY")) != NULL) {
    scr_crc_on_copy = atoi(value);
  }

  /* specify whether to compute CRC on fetch and flush */
  if ((value = scr_param_get("SCR_CRC_ON_FLUSH")) != NULL) {
    scr_crc_on_flush = atoi(value);
  }

  /* specify whether to compute and check CRC when deleting a file */
  if ((value = scr_param_get("SCR_CRC_ON_DELETE")) != NULL) {
    scr_crc_on_delete = atoi(value);
  }

  if ((value = scr_param_get("SCR_PRESERVE_USER_DIRECTORIES")) != NULL) {
    scr_preserve_user_directories = atoi(value);
  }

  if ((value = scr_param_get("SCR_USE_CONTAINERS")) != NULL) {
    scr_use_containers = atoi(value);
  }

  if ((value = scr_param_get("SCR_CONTAINER_SIZE")) != NULL) {
    if (scr_abtoull(value, &ull) == SCR_SUCCESS) {
      scr_container_size = (unsigned long) ull;
    } else {
      scr_err("Failed to read SCR_CONTAINER_SIZE successfully @ %s:%d", __FILE__, __LINE__);
    }
  }

  /* override default checkpoint interval (number of times to call Need_checkpoint between checkpoints) */
  if ((value = scr_param_get("SCR_CHECKPOINT_INTERVAL")) != NULL) {
    scr_checkpoint_interval = atoi(value);
  }

  /* override default minimum number of seconds between checkpoints */
  if ((value = scr_param_get("SCR_CHECKPOINT_SECONDS")) != NULL) {
    scr_checkpoint_seconds = atoi(value);
  }

  /* override default maximum allowed checkpointing overhead */
  if ((value = scr_param_get("SCR_CHECKPOINT_OVERHEAD")) != NULL) {
    if (scr_atod(value, &d) == SCR_SUCCESS) {
      scr_checkpoint_overhead = d;
    } else {
      scr_err("Failed to read SCR_CHECKPOINT_OVERHEAD successfully @ %s:%d", __FILE__, __LINE__);
    }
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

  /* connect to the SCR log database if enabled */
  /* NOTE: We do this inbetween our existing calls to scr_param_init and scr_param_finalize,
   * since scr_log_init itself calls param_init to read the db username and password from the
   * config file, which in turn requires a bcast.  However, only rank 0 calls scr_log_init(),
   * so the bcast would fail if scr_param_init really had to read the config file again. */
  if (scr_my_rank_world == 0 && scr_log_enable) {
    if (scr_log_init() != SCR_SUCCESS) {
      scr_warn("Failed to initialize SCR logging, disabling logging @ %s:%d",
              __FILE__, __LINE__
      );
      scr_log_enable = 0;
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
  int i;

  /* check whether user has disabled library via environment variable */
  char* value = NULL;
  if ((value = getenv("SCR_ENABLE")) != NULL) {
    scr_enabled = atoi(value);
  }

  /* if not enabled, bail with an error */
  if (! scr_enabled) {
    return SCR_FAILURE;
  }

  /* NOTE: SCR_ENABLE can also be set in a config file, but to read a config file,
   * we must at least create scr_comm_world and call scr_get_params() */

  /* create a context for the library */
  MPI_Comm_dup(MPI_COMM_WORLD, &scr_comm_world);

  /* find our rank and the size of our world */
  MPI_Comm_rank(scr_comm_world, &scr_my_rank_world);
  MPI_Comm_size(scr_comm_world, &scr_ranks_world);

  /* get my hostname (used in debug and error messages) */
  if (gethostname(scr_my_hostname, sizeof(scr_my_hostname)) != 0) {
    scr_err("Call to gethostname failed @ %s:%d",
            __FILE__, __LINE__
    );
    MPI_Abort(scr_comm_world, 0);
  }

  /* get the page size (used to align communication buffers) */
  scr_page_size = getpagesize();
  if (scr_page_size <= 0) {
    scr_err("Call to getpagesize failed @ %s:%d",
            __FILE__, __LINE__
    );
    MPI_Abort(scr_comm_world, 0);
  }

  /* read our configuration: environment variables, config file, etc. */
  scr_get_params();

  /* if not enabled, bail with an error */
  if (! scr_enabled) {
    /* we dup'd comm_world to broadcast parameters in scr_get_params,
     * need to free it here */
    MPI_Comm_free(&scr_comm_world);

    return SCR_FAILURE;
  }

  /* check that some required parameters are set */
  if (scr_username == NULL || scr_jobid == NULL) {
    scr_abort(-1,
              "Jobid or username is not set; you may need to manually set SCR_JOB_ID or SCR_USER_NAME @ %s:%d",
              __FILE__, __LINE__
    );
  }

  /* create a scr_comm_local communicator to hold all tasks on the same node */
#ifdef HAVE_LIBGCS
  /* determine the length of the maximum hostname (including terminating NULL character),
   * and check that our own buffer is at least as big */
  int my_hostname_len = strlen(scr_my_hostname) + 1;
  int max_hostname_len = 0;
  MPI_Allreduce(&my_hostname_len, &max_hostname_len, 1, MPI_INT, MPI_MAX, scr_comm_world);
  if (max_hostname_len > sizeof(scr_my_hostname)) {
    scr_err("Hostname is too long on some process @ %s:%d",
            __FILE__, __LINE__
    );
    MPI_Abort(scr_comm_world, 0);
  }

  /* split ranks based on hostname */
  GCS_Comm_splitv(
      scr_comm_world,
      scr_my_hostname, max_hostname_len, GCS_CMP_STR,
      NULL,            0,                GCS_CMP_IGNORE,
      &scr_comm_local
  );
#else /* HAVE_LIBGCS */
  /* TODO: maybe a better way to identify processes on the same node?
   * TODO: could improve scalability here using a parallel sort and prefix scan
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
  int host_id = (int) ((struct in_addr *) hostent->h_addr_list[0])->s_addr;

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
  int host_index = 0;
  for (i=0; i < scr_ranks_world; i++) {
    if (host_ids[i] == host_id) {
      host_index = i;
    }
  }
  free(host_ids);

  /* finally create the communicator holding all ranks on the same node */
  MPI_Comm_split(scr_comm_world, host_index, scr_my_rank_world, &scr_comm_local);
#endif /* HAVE_LIBGCS */

  /* find our position in the local communicator */
  MPI_Comm_rank(scr_comm_local, &scr_my_rank_local);
  MPI_Comm_size(scr_comm_local, &scr_ranks_local);

  /* Based on my local rank, create communicators consisting of all tasks at same local rank level */
  MPI_Comm_split(scr_comm_world, scr_my_rank_local, scr_my_rank_world, &scr_comm_level);

  /* find our position in the level communicator */
  MPI_Comm_rank(scr_comm_level, &scr_my_rank_level);
  MPI_Comm_size(scr_comm_level, &scr_ranks_level);

  /* setup redundancy descriptors */
  if (scr_reddesc_create_list() != SCR_SUCCESS) {
    if (scr_my_rank_world == 0) {
      scr_err("Failed to prepare one or more redundancy descriptors @ %s:%d",
              __FILE__, __LINE__
      );
    }
  }

  /* check that we have an enabled redundancy descriptor with interval of one */
  int found_one = 0;
  for (i=0; i < scr_nreddescs; i++) {
    /* check that we have at least one descriptor enabled with an interval of one */
    if (scr_reddescs[i].enabled && scr_reddescs[i].interval == 1) {
      found_one = 1;
    }
  }
  if (!found_one) {
    if (scr_my_rank_world == 0) {
      scr_abort(-1, "Failed to find an enabled redundancy descriptor with interval 1 @ %s:%d",
              __FILE__, __LINE__
      );
    }
  }

  /* register this job in the logging database */
  if (scr_my_rank_world == 0 && scr_log_enable) {
    if (scr_username != NULL && scr_jobname != NULL) {
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

  /* build the control directory name: CNTL_BASE/username/scr.jobid */
  int cntldir_str_len = strlen(scr_cntl_base) + 1 + strlen(scr_username) + strlen("/scr.") + strlen(scr_jobid);
  scr_cntl_prefix = (char*) malloc(cntldir_str_len + 1);
  if (scr_cntl_prefix == NULL) {
    scr_abort(-1, "Failed to allocate buffer to store control prefix @ %s:%d",
              __FILE__, __LINE__
    );
  }
  sprintf(scr_cntl_prefix, "%s/%s/scr.%s", scr_cntl_base, scr_username, scr_jobid);

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

  /* TODO: should we check for access and required space in cache directory at this point? */

  /* create the cache directories */
  if (scr_my_rank_local == 0) {
    for (i=0; i < scr_nreddescs; i++) {
      /* TODO: if checkpoints can be enabled at run time, we'll need to create them all up front */
      if (scr_reddescs[i].enabled) {
        scr_dbg(2, "Creating cache directory: %s", scr_reddescs[i].directory);
        if (scr_mkdir(scr_reddescs[i].directory, S_IRWXU | S_IRWXG) != SCR_SUCCESS) {
          scr_abort(-1, "Failed to create cache directory: %s @ %s:%d",
                    scr_reddescs[i].directory, __FILE__, __LINE__
          );
        }
      }
    }
  }

  /* TODO: should we check for access and required space in cache directore at this point? */

  /* ensure that the control and checkpoint directories are ready on our node */
  MPI_Barrier(scr_comm_local);

  /* place the halt, flush, and nodes files in the prefix directory */
  scr_build_path(scr_halt_file,  sizeof(scr_halt_file),  scr_par_prefix, "halt.scr");
  scr_build_path(scr_flush_file, sizeof(scr_flush_file), scr_par_prefix, "flush.scr");
  scr_build_path(scr_nodes_file, sizeof(scr_nodes_file), scr_par_prefix, "nodes.scr");

  /* build the file names using the control directory prefix */
  sprintf(scr_map_file,        "%s/filemap_%d.scrinfo", scr_cntl_prefix, scr_my_rank_local);
  sprintf(scr_master_map_file, "%s/filemap.scrinfo",    scr_cntl_prefix);
  sprintf(scr_transfer_file,   "%s/transfer.scrinfo",   scr_cntl_prefix);

  /* TODO: continue draining a checkpoint if one is in progress from the previous run,
   * for now, just delete the transfer file so we'll start over from scratch */
  if (scr_my_rank_local == 0) {
    unlink(scr_transfer_file);
  }

  /* TODO: should we also record the list of nodes and / or MPI rank to node mapping? */
  /* record the number of nodes being used in this job to the nodes file */
  int num_nodes = 0;
  MPI_Allreduce(&scr_ranks_level, &num_nodes, 1, MPI_INT, MPI_MAX, scr_comm_world);
  if (scr_my_rank_world == 0) {
    scr_hash* nodes_hash = scr_hash_new();
    scr_hash_util_set_int(nodes_hash, SCR_NODES_KEY_NODES, num_nodes);
    scr_hash_write(scr_nodes_file, nodes_hash);
    scr_hash_delete(nodes_hash);
  }

  /* initialize halt info before calling scr_bool_check_halt_and_decrement
   * and set the halt seconds in our halt data structure,
   * this will be overridden if a value is already set in the halt file */
  scr_halt_hash = scr_hash_new();

  /* record the halt seconds if they are set */
  if (scr_halt_seconds > 0) {
    scr_hash_util_set_unsigned_long(scr_halt_hash, SCR_HALT_KEY_SECONDS, scr_halt_seconds);
  }

  /* sync everyone up */
  MPI_Barrier(scr_comm_world);

  /* now all processes are initialized (be careful when moving this line up or down) */
  scr_initialized = 1;

  /* since we may be shuffling files around, stop any ongoing async flush */
  if (scr_flush_async) {
    scr_flush_async_stop();
  }

  /* exit right now if we need to halt */
  scr_bool_check_halt_and_decrement(SCR_TEST_AND_HALT, 0);

  int rc = SCR_FAILURE;

  double time_start, time_end, time_diff;

  /* if the code is restarting from the parallel file system,
   * disable fetch and enable flush_on_restart */
  if (scr_global_restart) {
    scr_flush_on_restart = 1;
    scr_fetch = 0;
  }

  /* if scr_fetch or scr_flush is enabled, check that scr_par_prefix is set */
  if ((scr_fetch != 0 || scr_flush > 0) && strcmp(scr_par_prefix, "") == 0) {
    if (scr_my_rank_world == 0) {
      scr_halt("SCR_INIT_FAILED");
      scr_abort(-1, "SCR_PREFIX must be set to use SCR_FETCH or SCR_FLUSH @ %s:%d"
                __FILE__, __LINE__
      );
    }

    /* rank 0 will abort above, but we don't want other processes to continue past this point */
    MPI_Barrier(scr_comm_world);
  }

  /* allocate a new global filemap object */
  scr_map = scr_filemap_new();

  /* master on each node reads all filemaps and distributes them to other ranks
   * on the node, if any */
  scr_scatter_filemaps(scr_map);

  /* attempt to distribute files for a restart */
  if (rc != SCR_SUCCESS && scr_distribute) {
    /* distribute and rebuild files in cache,
     * sets scr_dataset_id and scr_checkpoint_id upon success */
    rc = scr_cache_rebuild(scr_map);

    /* if distribute succeeds, check whether we should flush on restart */
    if (rc == SCR_SUCCESS) {
      /* since the flush file is not deleted between job allocations,
       * we need to rebuild it based on what's currently in cache data,
       * if the rebuild failed, we'll delete the flush file after purging the cache below */
      scr_flush_file_rebuild(scr_map);

      if (scr_flush_on_restart) {
        /* always flush on restart if scr_flush_on_restart is set */
        scr_cache_flush(scr_map, scr_checkpoint_id);
      } else {
        /* otherwise, flush only if we need to flush */
        scr_check_flush(scr_map);
      }
    }
  }

  /* TODO: there is some risk here of cleaning the cache when we shouldn't if given a badly placed nodeset
   * for a restart job step within an allocation with lots of spares. */
  /* if the distribute fails, or if the code must restart from the parallel file system, clear the cache */
  if (rc != SCR_SUCCESS || scr_global_restart) {
    scr_cache_purge(scr_map);
    scr_dataset_id    = 0;
    scr_checkpoint_id = 0;

    /* delete the flush file which may be stale */
    scr_flush_file_rebuild(scr_map);
  }

  /* attempt to fetch files from parallel file system */
  int fetch_attempted = 0;
  if (rc != SCR_SUCCESS && scr_fetch) {
    /* sets scr_dataset_id and scr_checkpoint_id upon success */
    rc = scr_cache_fetch(scr_map, &fetch_attempted);
  }

  /* TODO: there is some risk here of cleaning the cache when we shouldn't if given a badly placed nodeset
     for a restart job step within an allocation with lots of spares. */
  /* if the fetch fails, lets clear the cache */
  if (rc != SCR_SUCCESS) {
    scr_cache_purge(scr_map);
    scr_dataset_id    = 0;
    scr_checkpoint_id = 0;
  }

  /* both the distribute and the fetch failed */
  if (rc != SCR_SUCCESS) {
    /* if a fetch was attempted but we failed, print a warning */
    if (scr_my_rank_world == 0 && fetch_attempted) {
      scr_err("Failed to fetch checkpoint set into cache @ %s:%d", __FILE__, __LINE__);
    }
    rc = SCR_SUCCESS;
  }

  /* sync everyone before returning to ensure that subsequent calls to SCR functions are valid */
  MPI_Barrier(scr_comm_world);

  /* start the clocks for measuring the compute time and time of last checkpoint */
  if (scr_my_rank_world == 0) {
    /* set the checkpoint end time, we use this time in Need_checkpoint */
    scr_time_checkpoint_end = MPI_Wtime();

    /* start the clocks for measuring the compute time */
    scr_timestamp_compute_start = scr_log_seconds();
    scr_time_compute_start = MPI_Wtime();

    /* log the start time of this compute phase */
    if (scr_log_enable) {
      int compute_id = scr_checkpoint_id + 1;
      scr_log_event("COMPUTE STARTED", NULL, &compute_id, &scr_timestamp_compute_start, NULL);
    }
  }

  /* all done, ready to go */
  return rc;
}

/* Close down and clean up */
int SCR_Finalize()
{ 
  /* if not enabled, bail with an error */
  if (! scr_enabled) {
    return SCR_FAILURE;
  }

  /* bail out if not initialized -- will get bad results */
  if (! scr_initialized) {
    scr_abort(-1, "SCR has not been initialized @ %s:%d", __FILE__, __LINE__);
    return SCR_FAILURE;
  }

  if (scr_my_rank_world == 0) {
    /* stop the clock for measuring the compute time */
    scr_time_compute_end = MPI_Wtime();

    /* if we reach SCR_Finalize, assume that we should not restart the job */
    scr_halt("SCR_FINALIZE_CALLED");
  }

  /* TODO: flush any output sets and latest checkpoint set if needed */

  /* handle any async flush */
  if (scr_flush_async_in_progress) {
    if (scr_flush_async_checkpoint_id == scr_checkpoint_id) {
      /* we're going to sync flush this same checkpoint below, so kill it */
      scr_flush_async_stop();
    } else {
      /* the async flush is flushing a different checkpoint, so wait for it */
      scr_flush_async_wait(scr_map);
    }
  }

  /* flush checkpoint set if we need to */
  if (scr_bool_need_flush(scr_checkpoint_id)) {
    scr_cache_flush(scr_map, scr_checkpoint_id);
  }

  /* disconnect from database */
  if (scr_my_rank_world == 0 && scr_log_enable) {
    scr_log_finalize();
  }

  /* free off the memory allocated for our redundancy descriptors */
  scr_reddesc_free_list();

  /* delete the cache descriptor and redundancy descriptor hashes */
  scr_hash_delete(scr_cachedesc_hash);
  scr_hash_delete(scr_reddesc_hash);

  /* free off our global filemap object */
  scr_filemap_delete(scr_map);

  /* free off the library's communicators */
  MPI_Comm_free(&scr_comm_level);
  MPI_Comm_free(&scr_comm_local);
  MPI_Comm_free(&scr_comm_world);

  /* free memory allocated for variables */
  if (scr_username) {
    free(scr_username);
    scr_username = NULL;
  }
  if (scr_jobid) {
    free(scr_jobid);
    scr_jobid    = NULL;
  }
  if (scr_jobname) {
    free(scr_jobname);
    scr_jobname  = NULL;
  }
  if (scr_clustername) {
    free(scr_clustername);
    scr_clustername  = NULL;
  }

  /* free off the memory we allocated for our cntl prefix */
  if (scr_cntl_prefix != NULL) {
    free(scr_cntl_prefix);
    scr_cntl_prefix = NULL;
  }

  /* we're no longer in an initialized state */
  scr_initialized = 0;

  return SCR_SUCCESS;
}

/* sets flag to 1 if a checkpoint should be taken, flag is set to 0 otherwise */
int SCR_Need_checkpoint(int* flag)
{
  /* if not enabled, bail with an error */
  if (! scr_enabled) {
    *flag = 0;
    return SCR_FAILURE;
  }

  /* say no if not initialized */
  if (! scr_initialized) {
    *flag = 0;
    scr_abort(-1, "SCR has not been initialized @ %s:%d", __FILE__, __LINE__);
    return SCR_FAILURE;
  }

  /* track the number of times a user has called SCR_Need_checkpoint */
  scr_need_checkpoint_count++;

  /* assume we don't need to checkpoint */
  *flag = 0;

  /* check whether a halt condition is active (don't halt, just be sure to return 1 in this case) */
  if (!*flag && scr_bool_check_halt_and_decrement(SCR_TEST_BUT_DONT_HALT, 0)) {
    *flag = 1;
  }

  /* have rank 0 make the decision and broadcast the result */
  if (scr_my_rank_world == 0) {
    /* TODO: account for MTBF, time to flush, etc. */
    /* if we don't need to halt, check whether we can afford to checkpoint */

    /* if checkpoint interval is set, check the current checkpoint id */
    if (!*flag && scr_checkpoint_interval > 0 && scr_need_checkpoint_count % scr_checkpoint_interval == 0) {
      *flag = 1;
    }

    /* if checkpoint seconds is set, check the time since the last checkpoint */
    if (!*flag && scr_checkpoint_seconds > 0) {
      double now_seconds = MPI_Wtime();
      if ((int)(now_seconds - scr_time_checkpoint_end) >= scr_checkpoint_seconds) {
        *flag = 1;
      }
    }

    /* check whether we can afford to checkpoint based on the max allowed checkpoint overhead, if set */
    if (!*flag && scr_checkpoint_overhead > 0) {
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

        /* if our current percent cost is less than allowable overhead,
         * indicate that it's time for a checkpoint */
        if (percent_cost < scr_checkpoint_overhead) {
          *flag = 1;
        }
      }
    }

    /* no way to determine whether we need to checkpoint, so always say yes */
    if (!*flag &&
        scr_checkpoint_interval <= 0 &&
        scr_checkpoint_seconds <= 0 &&
        scr_checkpoint_overhead <= 0)
    {
      *flag = 1;
    }
  }

  /* rank 0 broadcasts the decision */
  MPI_Bcast(flag, 1, MPI_INT, 0, scr_comm_world);

  return SCR_SUCCESS;
}

/* informs SCR that a fresh checkpoint set is about to start */
int SCR_Start_checkpoint()
{
  /* if not enabled, bail with an error */
  if (! scr_enabled) {
    return SCR_FAILURE;
  }
  
  /* bail out if not initialized -- will get bad results */
  if (! scr_initialized) {
    scr_abort(-1, "SCR has not been initialized @ %s:%d", __FILE__, __LINE__);
    return SCR_FAILURE;
  }

  /* bail out if user called Start_checkpoint twice without Complete_checkpoint in between */
  if (scr_in_output) {
    scr_abort(-1,
            "SCR_Complete_checkpoint must be called before SCR_Start_checkpoint is called again @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* make sure everyone is ready to start before we delete any existing checkpoints */
  MPI_Barrier(scr_comm_world);

  /* set the checkpoint flag to indicate we have entered a new checkpoint */
  scr_in_output = 1;

  /* stop clock recording compute time */
  if (scr_my_rank_world == 0) {
    /* stop the clock for measuring the compute time */
    scr_time_compute_end = MPI_Wtime();

    /* log the end of this compute phase */
    if (scr_log_enable) {
      int compute_id = scr_checkpoint_id + 1;
      double time_diff = scr_time_compute_end - scr_time_compute_start;
      time_t now = scr_log_seconds();
      scr_log_event("COMPUTE COMPLETED", NULL, &compute_id, &now, &time_diff);
    }
  }

  /* increment our dataset and checkpoint counters */
  scr_dataset_id++;
  scr_checkpoint_id++;

  /* get the redundancy descriptor for this checkpoint id */
  struct scr_reddesc* c = scr_ckptdesc_get(scr_checkpoint_id, scr_nreddescs, scr_reddescs);

  /* start the clock to record how long it takes to checkpoint */
  if (scr_my_rank_world == 0) {
    scr_timestamp_checkpoint_start = scr_log_seconds();
    scr_time_checkpoint_start = MPI_Wtime();

    /* log the start of this checkpoint phase */
    if (scr_log_enable) {
      scr_log_event("CHECKPOINT STARTED", c->base, &scr_checkpoint_id, &scr_timestamp_checkpoint_start, NULL);
    }
  }

  /* get an ordered list of the datasets currently in cache */
  int ndsets;
  int* dsets = NULL;
  scr_filemap_list_datasets(scr_map, &ndsets, &dsets);

  /* lookup the number of checkpoints we're allowed to keep in the base for this checkpoint */
  int size = scr_cachedesc_size(c->base);

  int i;
  char* base = NULL;

  /* run through each of our checkpoints and count how many we have in this base */
  int nckpts_base = 0;
  for (i=0; i < ndsets; i++) {
    /* TODODSET: need to check whether this dataset is really a checkpoint */

    /* if this checkpoint is not currently flushing, delete it */
    base = scr_reddesc_base_from_filemap(scr_map, dsets[i], scr_my_rank_world);
    if (base != NULL) {
      if (strcmp(base, c->base) == 0) {
        nckpts_base++;
      }
      free(base);
    }
  }

  /* run through and delete checkpoints from base until we make room for the current one */
  int flushing = -1;
  for (i=0; i < ndsets && nckpts_base >= size; i++) {
    /* TODODSET: need to check whether this dataset is really a checkpoint */

    base = scr_reddesc_base_from_filemap(scr_map, dsets[i], scr_my_rank_world);
    if (base != NULL) {
      if (strcmp(base, c->base) == 0) {
        if (! scr_bool_is_flushing(dsets[i])) {
          /* this checkpoint is in our base, and it's not being flushed, so delete it */
          scr_cache_delete(scr_map, dsets[i]);
          nckpts_base--;
        } else if (flushing == -1) {
          /* this checkpoint is in our base, but we're flushing it, don't delete it */
          flushing = dsets[i];
        }
      }
      free(base);
    }
  }

  /* if we still don't have room and we're flushing, the checkpoint we need to delete
   * must be flushing, so wait for it to finish */
  if (nckpts_base >= size && flushing != -1) {
    /* TODO: we could increase the transfer bandwidth to reduce our wait time */

    /* wait for this checkpoint to complete its flush */
    scr_flush_async_wait(scr_map);

    /* alright, this checkpoint is no longer flushing, so we can delete it now and continue on */
    scr_cache_delete(scr_map, flushing);
    nckpts_base--;
  }

  /* free the list of datasets */
  if (dsets != NULL) {
    free(dsets);
    dsets = NULL;
  }

  /* rank 0 builds dataset object and broadcasts it out to other ranks */
  scr_dataset* dataset = scr_dataset_new();
  if (scr_my_rank_world == 0) {
    /* capture time and build name of dataset */
    int64_t dataset_time = scr_time_usecs();
    char dataset_name[SCR_MAX_FILENAME];
    scr_dataset_build_name(scr_dataset_id, dataset_time, dataset_name, sizeof(dataset_name));

    /* fill in fields for dataset */
    scr_dataset_set_id(dataset, scr_dataset_id);
    scr_dataset_set_name(dataset, dataset_name);
    scr_dataset_set_created(dataset, dataset_time);
    scr_dataset_set_username(dataset, scr_username);
    if (scr_jobname != NULL) {
      scr_dataset_set_jobname(dataset, scr_jobname);
    }
    scr_dataset_set_jobid(dataset, scr_jobid);
    if (scr_clustername != NULL) {
      scr_dataset_set_cluster(dataset, scr_clustername);
    }
    scr_dataset_set_ckpt(dataset, scr_checkpoint_id);
    /* TODO: record machine (cluster) name */
  }
  scr_hash_bcast(dataset, 0, scr_comm_world);
  scr_filemap_set_dataset(scr_map, scr_dataset_id, scr_my_rank_world, dataset);
  scr_dataset_delete(dataset);

  /* store the redundancy descriptor in the filemap, so if we die before completing
   * the checkpoint, we'll have a record of the new directory we're about to create */
  scr_hash* my_desc_hash = scr_hash_new();
  scr_reddesc_store_to_hash(c, my_desc_hash);
  scr_filemap_set_desc(scr_map, scr_dataset_id, scr_my_rank_world, my_desc_hash);
  scr_filemap_write(scr_map_file, scr_map);
  scr_hash_delete(my_desc_hash);

  /* make directory in cache to store files for this checkpoint */
  scr_cache_dir_create(c, scr_dataset_id);

  /* print a debug message to indicate we've started the checkpoint */
  if (scr_my_rank_world == 0) {
    scr_dbg(1, "Starting checkpoint %d", scr_checkpoint_id);
  }

  return SCR_SUCCESS;
}

/* given a filename, return the full path to the file which the user should write to */
int SCR_Route_file(const char* file, char* newfile)
{
  /* if not enabled, bail with an error */
  if (! scr_enabled) {
    return SCR_FAILURE;
  }
  
  /* bail out if not initialized -- will get bad results */
  if (! scr_initialized) {
    scr_abort(-1, "SCR has not been initialized @ %s:%d", __FILE__, __LINE__);
    return SCR_FAILURE;
  }

  /* get the redundancy descriptor for the current checkpoint */
  struct scr_reddesc* c = scr_ckptdesc_get(scr_checkpoint_id, scr_nreddescs, scr_reddescs);

  /* route the file */
  int n = SCR_MAX_FILENAME;
  if (scr_route_file(c, scr_dataset_id, file, newfile, n) != SCR_SUCCESS) {
    return SCR_FAILURE;
  }

  /* if we are in a new dataset, record this file in our filemap,
   * otherwise, we are likely in a restart, so check whether the file exists */
  if (scr_in_output) {
    /* TODO: to avoid duplicates, check that the file is not already in the filemap,
     * at the moment duplicates just overwrite each other, so there's no harm */

    /* add the file to the filemap */
    scr_filemap_add_file(scr_map, scr_dataset_id, scr_my_rank_world, newfile);

    /* read meta data for this file */
    scr_meta* meta = scr_meta_new();
    scr_filemap_get_meta(scr_map, scr_dataset_id, scr_my_rank_world, newfile, meta);

    /* set parameters for the file */
    scr_meta_set_filename(meta, newfile);
    scr_meta_set_filetype(meta, SCR_META_FILE_FULL);
    scr_meta_set_complete(meta, 0);
    /* TODODSET: move the ranks field elsewhere, for now it's needed by scr_index.c */
    scr_meta_set_ranks(meta, scr_ranks_world);
    scr_meta_set_orig(meta, file);

    /* determine full path to original file and record it in the meta data */
    char path_file[SCR_MAX_FILENAME];
    if (scr_build_absolute_path(path_file, sizeof(path_file), file) == SCR_SUCCESS) {
      /* store the full path and name of the original file */
      char path[SCR_MAX_FILENAME];
      char name[SCR_MAX_FILENAME];
      scr_split_path(path_file, path, name);
      scr_meta_set_origpath(meta, path);
      scr_meta_set_origname(meta, name);
    } else {
      scr_err("Failed to build absolute path to %s @ %s:%d",
              file, __FILE__, __LINE__
      );
    }

    /* record the meta data for this file */
    scr_filemap_set_meta(scr_map, scr_dataset_id, scr_my_rank_world, newfile, meta);

    /* write out the filemap */
    scr_filemap_write(scr_map_file, scr_map);

    /* delete the meta data object */
    scr_meta_delete(meta);
  } else {
    /* if we can't read the file, return an error */
    if (access(newfile, R_OK) < 0) {
      return SCR_FAILURE;
    }
  }

  return SCR_SUCCESS;
}

/* completes the checkpoint set and marks it as valid or not */
int SCR_Complete_checkpoint(int valid)
{
  /* if not enabled, bail with an error */
  if (! scr_enabled) {
    return SCR_FAILURE;
  }

  /* bail out if not initialized -- will get bad results */
  if (! scr_initialized) {
    scr_abort(-1, "SCR has not been initialized @ %s:%d", __FILE__, __LINE__);
    return SCR_FAILURE;
  }

  /* bail out if there is no active call to Start_checkpoint */
  if (! scr_in_output) {
    scr_abort(-1,
            "SCR_Start_checkpoint must be called before SCR_Complete_checkpoint @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* record filesize for each file */
  unsigned long my_counts[3] = {0, 0, 0};
  scr_hash_elem* elem;
  for (elem = scr_filemap_first_file(scr_map, scr_dataset_id, scr_my_rank_world);
       elem != NULL;
       elem = scr_hash_elem_next(elem))
  {
    /* get the filename */
    char* file = scr_hash_elem_key(elem);
    my_counts[0]++;

    /* get size of this file */
    unsigned long filesize = scr_filesize(file);
    my_counts[1] += filesize;

    /* fill in filesize and complete flag in the meta data for the file */
    scr_meta* meta = scr_meta_new();
    scr_filemap_get_meta(scr_map, scr_dataset_id, scr_my_rank_world, file, meta);
    scr_meta_set_filesize(meta, filesize);
    scr_meta_set_complete(meta, valid);
    scr_filemap_set_meta(scr_map, scr_dataset_id, scr_my_rank_world, file, meta);
    scr_meta_delete(meta);
  }

  /* we execute a sum as a logical allreduce to determine whether everyone is valid
   * we interpret the result to be true only if the sum adds up to the number of processes */
  if (valid) {
    my_counts[2] = 1;
  }

  /* TODODSET: we may want to delay setting COMPLETE in the dataset until after copy call? */

  /* store total number of files, total number of bytes, and complete flag in dataset */
  unsigned long total_counts[3];
  MPI_Allreduce(my_counts, total_counts, 3, MPI_UNSIGNED_LONG, MPI_SUM, scr_comm_world);
  scr_dataset* dataset = scr_dataset_new();
  scr_filemap_get_dataset(scr_map, scr_dataset_id, scr_my_rank_world, dataset);
  scr_dataset_set_files(dataset, (int) total_counts[0]);
  scr_dataset_set_size(dataset,        total_counts[1]);
  if (total_counts[2] == scr_ranks_world) {
    scr_dataset_set_complete(dataset, 1);
  } else {
    scr_dataset_set_complete(dataset, 0);
  }
  scr_filemap_set_dataset(scr_map, scr_dataset_id, scr_my_rank_world, dataset);
  scr_dataset_delete(dataset);

  /* write out info to filemap */
  scr_filemap_write(scr_map_file, scr_map);

  /* apply redundancy scheme */
  double bytes_copied = 0.0;
  struct scr_reddesc* c = scr_ckptdesc_get(scr_checkpoint_id, scr_nreddescs, scr_reddescs);
  int rc = scr_copy_files(scr_map, c, scr_dataset_id, &bytes_copied);

  /* TODO: set size of dataset and complete flag */

  /* record the cost of the checkpoint and log its completion */
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
      /* log the end of this checkpoint phase */
      double time_diff = scr_time_checkpoint_end - scr_time_checkpoint_start;
      time_t now = scr_log_seconds();
      scr_log_event("CHECKPOINT COMPLETED", c->base, &scr_checkpoint_id, &now, &time_diff);

      /* log the transfer details */
      char dir[SCR_MAX_FILENAME];
      scr_cache_dir_get(c, scr_dataset_id, dir);
      scr_log_transfer("CHECKPOINT", c->base, dir, &scr_checkpoint_id,
                       &scr_timestamp_checkpoint_start, &cost, &bytes_copied
      );
    }

    /* print out a debug message with the result of the copy */
    scr_dbg(1, "Completed checkpoint %d with return code %d",
            scr_checkpoint_id, rc
    );
  }

  /* if copy is good, check whether we need to flush or halt,
   * otherwise delete the checkpoint to conserve space */
  if (rc == SCR_SUCCESS) {
    /* check_flush may start an async flush, whereas check_halt will call sync flush,
     * so place check_flush after check_halt */
    scr_flush_file_location_set(scr_dataset_id, SCR_FLUSH_KEY_LOCATION_CACHE);
    scr_bool_check_halt_and_decrement(SCR_TEST_AND_HALT, 1);
    scr_check_flush(scr_map);
  } else {
    /* something went wrong, so delete this checkpoint from the cache */
    scr_cache_delete(scr_map, scr_dataset_id);
  }

  /* if we have an async flush ongoing, take this chance to check whether it's completed */
  if (scr_flush_async_in_progress) {
    double bytes = 0.0;
    if (scr_flush_async_test(scr_map, scr_flush_async_checkpoint_id, &bytes) == SCR_SUCCESS) {
      /* async flush has finished, go ahead and complete it */
      scr_flush_async_complete(scr_map, scr_flush_async_checkpoint_id);
    } else {
      /* not done yet, just print a progress message to the screen */
      if (scr_my_rank_world == 0) {
        scr_dbg(1, "Flush of checkpoint %d is %d%% complete",
                scr_flush_async_checkpoint_id, (int) (bytes / scr_flush_async_bytes * 100.0)
        );
      }
    }
  }

  /* make sure everyone is ready before we exit */
  MPI_Barrier(scr_comm_world);

  /* unset the checkpoint flag to indicate we have exited the current checkpoint */
  scr_in_output = 0;

  /* start the clock for measuring the compute time */
  if (scr_my_rank_world == 0) {
    scr_timestamp_compute_start = scr_log_seconds();
    scr_time_compute_start = MPI_Wtime();

    /* log the start time of this compute phase */
    if (scr_log_enable) {
      int compute_id = scr_checkpoint_id + 1;
      scr_log_event("COMPUTE STARTED", NULL, &compute_id, &scr_timestamp_compute_start, NULL);
    }
  }

  return rc;
}
