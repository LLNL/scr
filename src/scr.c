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

/* All rights reserved. This program and the accompanying materials
 * are made available under the terms of the BSD-3 license which accompanies this
 * distribution in LICENSE.TXT
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the BSD-3  License in
 * LICENSE.TXT for more details.
 *
 * GOVERNMENT LICENSE RIGHTS-OPEN SOURCE SOFTWARE
 * The Government's rights to use, modify, reproduce, release, perform,
 * display, or disclose this software are subject to the terms of the BSD-3
 * License as provided in Contract No. B609815.
 * Any reproduction of computer software, computer software documentation, or
 * portions thereof marked with this legend must also reproduce the markings.
 *
 * Author: Christopher Holguin <christopher.a.holguin@intel.com>
 *
 * (C) Copyright 2015-2016 Intel Corporation.
 */

#include "scr_globals.h"

/* include the DTCMP library if it's available */
#ifdef HAVE_LIBDTCMP
#include "dtcmp.h"
#endif /* HAVE_LIBDTCMP */

#ifdef HAVE_LIBPMIX
#include "pmix.h"
#endif

#ifdef HAVE_LIBCPPR
#include "cppr.h"
#endif

/*
=========================================
Halt logic
=========================================
*/

#define SCR_TEST_AND_HALT (1)
#define SCR_TEST_BUT_DONT_HALT (2)
#define SCR_FINALIZE_CALLED "SCR_FINALIZE_CALLED"

/* writes entry to halt file to indicate that SCR should exit job at first opportunity */
static int scr_halt(const char* reason)
{
  /* copy reason if one was given */
  if (reason != NULL) {
    scr_hash_util_set_str(scr_halt_hash, SCR_HALT_KEY_EXIT_REASON, reason);
  }

  /* log the halt condition */
  int* dset = NULL;
  if (scr_dataset_id > 0) {
    dset = &scr_dataset_id;
  }
  scr_log_halt(reason, dset);

  /* and write out the halt file */
  int rc = scr_halt_sync_and_decrement(scr_halt_file, scr_halt_hash, 0);
  return rc;
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
      long int remaining = scr_env_seconds_remaining();
      if (remaining >= 0 && remaining <= halt_seconds) {
        if (halt_cond == SCR_TEST_AND_HALT) {
          scr_dbg(0, "Job exiting: Reached time limit: (seconds remaining = %ld) <= (SCR_HALT_SECONDS = %d).",
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
        /* got a reason, but let's ignore SCR_FINALIZE_CALLED if it's set
         * and assume user restarted intentionally */
        if (strcmp(reason, SCR_FINALIZE_CALLED) != 0) {
          /* since reason points at the EXIT_REASON string in the halt hash, and since
           * scr_halt() resets this value, we need to copy the current reason */
          char* tmp_reason = strdup(reason);
          if (halt_cond == SCR_TEST_AND_HALT && tmp_reason != NULL) {
            scr_dbg(0, "Job exiting: Reason: %s.", tmp_reason);
            scr_halt(tmp_reason);
          }
          scr_free(&tmp_reason);
          need_to_halt = 1;
        }
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

  /* broadcast halt decision from rank 0 */
  MPI_Bcast(&need_to_halt, 1, MPI_INT, 0, scr_comm_world);

  /* halt job if we need to, and flush latest checkpoint if needed */
  if (need_to_halt && halt_cond == SCR_TEST_AND_HALT) {
    /* handle any async flush */
    if (scr_flush_async_in_progress) {
      /* there's an async flush ongoing, see which dataset is being flushed */
      if (scr_flush_async_dataset_id == scr_dataset_id) {
	/* we're going to sync flush this same checkpoint below, so kill it if it's from POSIX */
	/* else wait */
	/* get the TYPE of the store for checkpoint */
	/* neither strdup nor free */
#ifdef HAVE_LIBCPPR
	/* it's faster to wait on async flush if we have CPPR  */
	scr_flush_async_wait(scr_map);
#else
	scr_reddesc* reddesc = scr_reddesc_for_checkpoint(scr_dataset_id,
							  scr_nreddescs,
							  scr_reddescs);
	int storedesc_index = scr_storedescs_index_from_name(reddesc->base);
	char* type = scr_storedescs[storedesc_index].type;
	if(!strcmp(type, "DATAWARP") || !strcmp(type, "DW")){
	  scr_flush_async_wait(scr_map);
	}else{//if type posix
	  scr_flush_async_stop();
	}
#endif
      } else {
        /* the async flush is flushing a different dataset, so wait for it */
        scr_flush_async_wait(scr_map);
      }
    }

    /* TODO: need to flush any output sets and the latest checkpoint set */

    /* flush files if needed */
    if(scr_bool_need_flush(scr_checkpoint_id)){
      if(scr_my_rank_world == 0){
	scr_dbg(2, "sync flush due to need to hald @ %s:%d", __FILE__, __LINE__);
      }
      scr_flush_sync(scr_map, scr_checkpoint_id);
    }

    /* give our async flush method a chance to shut down */
    if (scr_flush_async) {
      scr_flush_async_shutdown();
    }

    /* sync up tasks before exiting (don't want tasks to exit so early that
     * runtime kills others after timeout) */
    MPI_Barrier(scr_comm_world);

#ifdef HAVE_LIBPMIX
    /* sync procs in pmix before shutdown */
    int retval = PMIx_Fence(NULL, 0, NULL, 0);
    if (retval != PMIX_SUCCESS) {
      scr_err("PMIx_Fence failed: rc=%d, rank: %d @ %s:%d",
        retval, scr_pmix_proc.rank, __FILE__, __LINE__
      );
    }

/*
    scr_dbg(0, "about to call pmix notify in HALT: pmix rank: %d", scr_pmix_proc.rank);
    retval = PMIx_Notify_event(-1,
                      &scr_pmix_proc,
                      PMIX_RANGE_GLOBAL,
                      NULL, 0,
                      NULL, (void *)NULL);
    if (retval != PMIX_SUCCESS) {
      scr_dbg(0, "error calling pmix_notify_event: %d", retval);
    }
*/

    /* shutdown pmix */
    retval = PMIx_Finalize(NULL, 0);
    if (retval != PMIX_SUCCESS) {
      scr_err("PMIx_Finalize failed: rc=%d, rank: %d @ %s:%d",
        retval, scr_pmix_proc.rank, __FILE__, __LINE__
      );
    }

    /* TODO: remove this once ompi has a fix?? */
    MPI_Barrier(scr_comm_world);
    MPI_Finalize();
#endif /* HAVE_LIBPMIX */

    /* and exit the job */
    exit(0);
  }

  return need_to_halt;
}

/*
=========================================
Utility functions
=========================================
*/

/* check whether a flush is needed, and execute flush if so */
static int scr_check_flush(scr_filemap* map)
{
  /* check whether user has flush enabled */
  if (scr_flush > 0) {
    /* every scr_flush checkpoints, flush the checkpoint set */
    if (scr_checkpoint_id > 0 && scr_checkpoint_id % scr_flush == 0) {
      /* need to flush this checkpoint, determine whether to use async or sync flush */
      if (scr_flush_async) {
        if (scr_my_rank_world == 0) {
          scr_dbg(2, "async flush attempt @ %s:%d", __FILE__, __LINE__);;
        }

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
        if (scr_my_rank_world == 0) {
          scr_dbg(2, "sync flush attempt @ %s:%d", __FILE__, __LINE__);
        }
        scr_flush_sync(map, scr_checkpoint_id);
      }
    }
  }
  return SCR_SUCCESS;
}

/* given a dataset id and a filename,
 * return the full path to the file which the caller should use to access the file */
static int scr_route_file(const scr_reddesc* reddesc, int id, const char* file, char* newfile, int n)
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

  /* lookup the cache directory for this dataset */
  char* dir = scr_cache_dir_get(reddesc, id);

  /* chop file to just the file name and prepend directory */
  scr_path* path_file = scr_path_from_str(file);
  scr_path_basename(path_file);
  scr_path_prepend_str(path_file, dir);

  /* copy to user's buffer */
  size_t n_size = (size_t) n;
  scr_path_strcpy(newfile, n_size, path_file);

  /* free the file path */
  scr_path_delete(&path_file);

  /* free the cache directory */
  scr_free(&dir);

  return SCR_SUCCESS;
}

/*
=========================================
Configuration parameters
=========================================
*/

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
    scr_cntl_base = scr_path_strdup_reduce_str(value);
  } else {
    scr_cntl_base = scr_path_strdup_reduce_str(SCR_CNTL_BASE);
  }

  /* override default base directory for checkpoint cache */
  if ((value = scr_param_get("SCR_CACHE_BASE")) != NULL) {
    scr_cache_base = scr_path_strdup_reduce_str(value);
  } else {
    scr_cache_base = scr_path_strdup_reduce_str(SCR_CACHE_BASE);
  }

  /* set maximum number of checkpoints to keep in cache */
  if ((value = scr_param_get("SCR_CACHE_SIZE")) != NULL) {
    scr_cache_size = atoi(value);
  }

  /* fill in a hash of group descriptors */
  scr_groupdesc_hash = scr_hash_new();
  tmp = scr_param_get_hash(SCR_CONFIG_KEY_GROUPDESC);
  if (tmp != NULL) {
    scr_hash_set(scr_groupdesc_hash, SCR_CONFIG_KEY_GROUPDESC, tmp);
  }

  /* fill in a hash of store descriptors */
  scr_storedesc_hash = scr_hash_new();
  tmp = scr_param_get_hash(SCR_CONFIG_KEY_STOREDESC);
  if (tmp != NULL) {
    scr_hash_set(scr_storedesc_hash, SCR_CONFIG_KEY_STOREDESC, tmp);
  } else {
    /* TODO: consider requiring user to specify config file for this */

    /* create a store descriptor for the cache directory */
    tmp = scr_hash_set_kv(scr_storedesc_hash, SCR_CONFIG_KEY_STOREDESC, scr_cache_base);
    scr_hash_util_set_int(tmp, SCR_CONFIG_KEY_COUNT, scr_cache_size);

    /* also create one for control directory if cntl != cache */
    if (strcmp(scr_cntl_base, scr_cache_base) != 0) {
      tmp = scr_hash_set_kv(scr_storedesc_hash, SCR_CONFIG_KEY_STOREDESC, scr_cntl_base);
      scr_hash_util_set_int(tmp, SCR_CONFIG_KEY_COUNT, 0);
    }
  }

  /* select copy method */
  if ((value = scr_param_get("SCR_COPY_TYPE")) != NULL) {
    if (strcasecmp(value, "single") == 0) {
      scr_copy_type = SCR_COPY_SINGLE;
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

  /* specify the group name to protect failures */
  if ((value = scr_param_get("SCR_GROUP")) != NULL) {
    scr_group = strdup(value);
  } else {
    scr_group = strdup(SCR_GROUP);
  }

  /* fill in a hash of redundancy descriptors */
  scr_reddesc_hash = scr_hash_new();
  if (scr_copy_type == SCR_COPY_SINGLE) {
    /* fill in info for one SINGLE checkpoint */
    tmp = scr_hash_set_kv(scr_reddesc_hash, SCR_CONFIG_KEY_CKPTDESC, "0");
    scr_hash_util_set_str(tmp, SCR_CONFIG_KEY_STORE,    scr_cache_base);
    scr_hash_util_set_str(tmp, SCR_CONFIG_KEY_TYPE,     "SINGLE");
  } else if (scr_copy_type == SCR_COPY_PARTNER) {
    /* fill in info for one PARTNER checkpoint */
    tmp = scr_hash_set_kv(scr_reddesc_hash, SCR_CONFIG_KEY_CKPTDESC, "0");
    scr_hash_util_set_str(tmp, SCR_CONFIG_KEY_STORE,    scr_cache_base);
    scr_hash_util_set_str(tmp, SCR_CONFIG_KEY_TYPE,     "PARTNER");
    scr_hash_util_set_str(tmp, SCR_CONFIG_KEY_GROUP,    scr_group);
  } else if (scr_copy_type == SCR_COPY_XOR) {
    /* fill in info for one XOR checkpoint */
    tmp = scr_hash_set_kv(scr_reddesc_hash, SCR_CONFIG_KEY_CKPTDESC, "0");
    scr_hash_util_set_str(tmp, SCR_CONFIG_KEY_STORE,    scr_cache_base);
    scr_hash_util_set_str(tmp, SCR_CONFIG_KEY_TYPE,     "XOR");
    scr_hash_util_set_str(tmp, SCR_CONFIG_KEY_GROUP,    scr_group);
    scr_hash_util_set_int(tmp, SCR_CONFIG_KEY_SET_SIZE, scr_set_size);
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

  /* if job has fewer than SCR_HALT_SECONDS remaining after completing a checkpoint,
   * halt it */
  if ((value = scr_param_get("SCR_HALT_SECONDS")) != NULL) {
    scr_halt_seconds = atoi(value);
  }

  /* set MPI buffer size (file chunk size) */
  if ((value = scr_param_get("SCR_MPI_BUF_SIZE")) != NULL) {
    if (scr_abtoull(value, &ull) == SCR_SUCCESS) {
      scr_mpi_buf_size = (size_t) ull;
    } else {
      scr_err("Failed to read SCR_MPI_BUF_SIZE successfully @ %s:%d",
        __FILE__, __LINE__
      );
    }
  }

  /* whether to distribute files in filemap to ranks */
  if ((value = scr_param_get("SCR_DISTRIBUTE")) != NULL) {
    scr_distribute = atoi(value);
  }

  /* whether to fetch files from the parallel file system */
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
      scr_err("Failed to read SCR_FLUSH_ASYNC_BW successfully @ %s:%d",
        __FILE__, __LINE__
      );
    }
  }

  /* runtime overhead limit imposed during async flush (in percentage) */
  if ((value = scr_param_get("SCR_FLUSH_ASYNC_PERCENT")) != NULL) {
    if (scr_atod(value, &d) == SCR_SUCCESS) {
      scr_flush_async_percent = d;
    } else {
      scr_err("Failed to read SCR_FLUSH_ASYNC_PERCENT successfully @ %s:%d",
        __FILE__, __LINE__
      );
    }
  }

  /* set file copy buffer size (file chunk size) */
  if ((value = scr_param_get("SCR_FILE_BUF_SIZE")) != NULL) {
    if (scr_abtoull(value, &ull) == SCR_SUCCESS) {
      scr_file_buf_size = (size_t) ull;
    } else {
      scr_err("Failed to read SCR_FILE_BUF_SIZE successfully @ %s:%d",
        __FILE__, __LINE__
      );
    }
  }

  /* specify whether to compute CRC when applying redundancy scheme */
  if ((value = scr_param_get("SCR_CRC_ON_COPY")) != NULL) {
    scr_crc_on_copy = atoi(value);
  }

  /* specify whether to compute CRC on fetch and flush */
  if ((value = scr_param_get("SCR_CRC_ON_FLUSH")) != NULL) {
    scr_crc_on_flush = atoi(value);
  }

  /* specify whether to compute and check CRC when deleting files from cache */
  if ((value = scr_param_get("SCR_CRC_ON_DELETE")) != NULL) {
    scr_crc_on_delete = atoi(value);
  }

  /* whether to create user-specified directories when flushing to file system */
  if ((value = scr_param_get("SCR_PRESERVE_DIRECTORIES")) != NULL) {
    scr_preserve_directories = atoi(value);
  }

  /* wether to store files in containers when flushing to file system */
  if ((value = scr_param_get("SCR_USE_CONTAINERS")) != NULL) {
    scr_use_containers = atoi(value);

    /* we don't yet support containers with the async flush,
     * need to change transfer file format for this */
    if (scr_flush_async && scr_use_containers) {
      scr_warn("Async flush does not yet support containers, disabling containers @ %s:%d",
          __FILE__, __LINE__
      );
      scr_use_containers = 0;
    }
  }

  /* number of bytes to store per container */
  if ((value = scr_param_get("SCR_CONTAINER_SIZE")) != NULL) {
    if (scr_abtoull(value, &ull) == SCR_SUCCESS) {
      scr_container_size = (unsigned long) ull;
    } else {
      scr_err("Failed to read SCR_CONTAINER_SIZE successfully @ %s:%d",
        __FILE__, __LINE__
      );
    }
  }

  /* override default checkpoint interval
   * (number of times to call Need_checkpoint between checkpoints) */
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
      scr_err("Failed to read SCR_CHECKPOINT_OVERHEAD successfully @ %s:%d",
        __FILE__, __LINE__
      );
    }
  }

  /* set scr_prefix_path and scr_prefix */
  if ((value = scr_param_get("SCR_PREFIX")) != NULL) {
    scr_prefix_path = scr_path_from_str(value);
  } else {
    /* if user didn't set with SCR_PREFIX,
     * pick up the current working directory as a default */
    char current_dir[SCR_MAX_FILENAME];
    if (scr_getcwd(current_dir, sizeof(current_dir)) != SCR_SUCCESS) {
      scr_abort(-1, "Problem reading current working directory @ %s:%d",
        __FILE__, __LINE__
      );
    }
    scr_prefix_path = scr_path_from_str(current_dir);
  }
  scr_path_reduce(scr_prefix_path);
  scr_prefix = scr_path_strdup(scr_prefix_path);

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

#ifdef HAVE_LIBDTCMP
  /* initialize the DTCMP library for sorting and ranking routines
   * if we're using it */
  int dtcmp_rc = DTCMP_Init();
  if (dtcmp_rc != DTCMP_SUCCESS) {
    scr_abort(-1, "Failed to initialize DTCMP library @ %s:%d",
      __FILE__, __LINE__
    );
  }
#endif /* HAVE_LIBDTCMP */

  /* NOTE: SCR_ENABLE can also be set in a config file, but to read
   * a config file, we must at least create scr_comm_world and call
   * scr_get_params() */

  /* create a context for the library */
  MPI_Comm_dup(MPI_COMM_WORLD,  &scr_comm_world);
  MPI_Comm_rank(scr_comm_world, &scr_my_rank_world);
  MPI_Comm_size(scr_comm_world, &scr_ranks_world);

  /* get my hostname (used in debug and error messages) */
  scr_my_hostname = scr_env_nodename();
  if (scr_my_hostname == NULL) {
    scr_err("Failed to get hostname @ %s:%d",
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

  #ifdef HAVE_LIBDTCMP
    /* shut down the DTCMP library if we're using it */
    int dtcmp_rc = DTCMP_Finalize();
    if (dtcmp_rc != DTCMP_SUCCESS) {
      scr_abort(-1, "Failed to finalized DTCMP library @ %s:%d",
        __FILE__, __LINE__
      );
    }
  #endif /* HAVE_LIBDTCMP */

    return SCR_FAILURE;
  }

  /* check that some required parameters are set */
  if (scr_username == NULL || scr_jobid == NULL) {
    scr_abort(-1, "Jobid or username is not set; set SCR_JOB_ID or SCR_USER_NAME @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* setup group descriptors */
  if (scr_groupdescs_create() != SCR_SUCCESS) {
    if (scr_my_rank_world == 0) {
      scr_err("Failed to prepare one or more group descriptors @ %s:%d",
        __FILE__, __LINE__
      );
    }
  }

  /* setup store descriptors (refers to group descriptors) */
  if (scr_storedescs_create() != SCR_SUCCESS) {
    if (scr_my_rank_world == 0) {
      scr_err("Failed to prepare one or more store descriptors @ %s:%d",
        __FILE__, __LINE__
      );
    }
  }

  /* setup redundancy descriptors (refers to store descriptors) */
  if (scr_reddescs_create() != SCR_SUCCESS) {
    if (scr_my_rank_world == 0) {
      scr_err("Failed to prepare one or more redundancy descriptors @ %s:%d",
        __FILE__, __LINE__
      );
    }
  }

  /* check that we have an enabled redundancy descriptor with
   * interval of one, this is necessary so a reddesc is defined
   * for every checkpoint */
  int found_one = 0;
  for (i=0; i < scr_nreddescs; i++) {
    /* check that we have at least one descriptor enabled with
     * an interval of one */
    if (scr_reddescs[i].enabled && scr_reddescs[i].interval == 1) {
      found_one = 1;
    }
  }
  if (! found_one) {
    if (scr_my_rank_world == 0) {
      scr_abort(-1, "Failed to find an enabled redundancy descriptor with interval 1 @ %s:%d",
        __FILE__, __LINE__
      );
    }
  }

  /* if we're using containers we need two other communicators during
   * each flush, one contains all procs on each node, and one contains
   * all procs that have the same rank per node */
  scr_groupdesc* groupdesc_node = scr_groupdescs_from_name(SCR_GROUP_NODE);
  if (groupdesc_node != NULL) {
    /* just dup the communicator from the NODE group */
    MPI_Comm_dup(groupdesc_node->comm, &scr_comm_node);

    /* then split comm_world by our rank within this comm */
    int rank_node;
    MPI_Comm_rank(scr_comm_node, &rank_node);
    MPI_Comm_split(scr_comm_world, rank_node, scr_my_rank_world, &scr_comm_node_across);
  } else {
    scr_err("Failed to create communicator for procs on each node @ %s:%d",
      __FILE__, __LINE__
    );
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

  /* TODO MEMFS: mount storage for control directory */

  /* check that scr_prefix is set */
  if (scr_prefix == NULL || strcmp(scr_prefix, "") == 0) {
    if (scr_my_rank_world == 0) {
      scr_halt("SCR_INIT_FAILED");
    }
    SCR_ALLABORT(-1, "SCR_PREFIX must be set");
  }

  /* define the path to the .scr subdir within the prefix dir */
  scr_path* path_prefix_scr = scr_path_dup(scr_prefix_path);
  scr_path_append_str(path_prefix_scr, ".scr");
  scr_prefix_scr = scr_path_strdup(path_prefix_scr);
  scr_path_delete(&path_prefix_scr);

  /* TODO: create store descriptor for prefix directory */
  /* create the .scr subdirectory */
  if (scr_my_rank_world == 0) {
    mode_t mode_dir = scr_getmode(1, 1, 1);
    if (scr_mkdir(scr_prefix_scr, mode_dir) != SCR_SUCCESS) {
      scr_abort(-1, "Failed to create .scr subdirectory %s @ %s:%d",
        scr_prefix_scr, __FILE__, __LINE__
      );
    }
  }

  /* build the control directory name: CNTL_BASE/username/scr.jobid */
  scr_path* path_cntl_prefix = scr_path_from_str(scr_cntl_base);
  scr_path_append_str(path_cntl_prefix, scr_username);
  scr_path_append_strf(path_cntl_prefix, "scr.%s", scr_jobid);
  scr_path_reduce(path_cntl_prefix);
  scr_cntl_prefix = scr_path_strdup(path_cntl_prefix);
  scr_path_delete(&path_cntl_prefix);

  /* create the control directory */
  if (scr_storedesc_dir_create(scr_storedesc_cntl, scr_cntl_prefix)
      != SCR_SUCCESS)
  {
    scr_abort(-1, "Failed to create control directory: %s @ %s:%d",
      scr_cntl_prefix, __FILE__, __LINE__
    );
  }

  /* TODO: should we check for access and required space in cntl
   * directory at this point? */

  /* num_nodes will be used later, this line is moved above cache_dir creation
   * to make sure scr_my_hostid is set before we try to create directories.
   * The logic that uses num_nodes can't be moved here because it relies on the
   * scr_node_file variable computed later */
  int num_nodes;
  MPI_Comm_rank(scr_comm_node, &scr_my_rank_host);
  //MPI_Allreduce(&ranks_across, &num_nodes, 1, MPI_INT, MPI_MAX, scr_comm_world);
  scr_rank_str(scr_comm_world, scr_my_hostname, &num_nodes, &scr_my_hostid);//

  /* create the cache directories */
  for (i=0; i < scr_nreddescs; i++) {
    /* TODO: if checkpoints can be enabled at run time,
     * we'll need to create them all up front */
    scr_reddesc* reddesc = &scr_reddescs[i];
    if (reddesc->enabled) {
      scr_storedesc* store = scr_reddesc_get_store(reddesc);
      if (store != NULL) {
        /* TODO MEMFS: mount storage for cache directory */

        if (scr_storedesc_dir_create(store, reddesc->directory)
          != SCR_SUCCESS)
        {
          scr_abort(-1, "Failed to create cache directory: %s @ %s:%d",
            reddesc->directory, __FILE__, __LINE__
            );
        }

	      /* set up artificially node-local directories if the store view is global */
        if ( !strcmp(store->view, "GLOBAL")){
	        /* make sure we can create directories */
          if ( ! store->can_mkdir ){
           scr_abort(-1, "Cannot use global view storage %s without mkdir enabled: @%s:%d",
            store->name, __FILE__, __LINE__);

         }

	      /* create directory on rank 0 of each node */
        int node_rank;
        MPI_Comm_rank(scr_comm_node, &node_rank);
          if(node_rank == 0){
            scr_path* path = scr_path_from_str(reddesc->directory);
            scr_path_append_strf(path, "node.%d", scr_my_hostid);
            scr_path_reduce(path);
            char* path_str = scr_path_strdup(path);
            scr_path_delete(&path);
            scr_mkdir(path_str, S_IRWXU | S_IRWXG);
            scr_free(&path_str);
          }
        }

      } else {
        scr_abort(-1, "Invalid store for redundancy descriptor @ %s:%d",
         __FILE__, __LINE__
         );
      }
    }
  }

  /* TODO: should we check for access and required space in cache
   * directories at this point? */

  /* ensure that the control and cache directories are ready */
  MPI_Barrier(scr_comm_world);

#if (SCR_MACHINE_TYPE == SCR_PMIX)
  /* init pmix */
  int retval = PMIx_Init(&scr_pmix_proc, NULL, 0);
  if (retval != PMIX_SUCCESS) {
    scr_err("PMIx_Init failed: rc=%d @ %s:%d",
      retval, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }
  scr_dbg(1, "PMIx_Init succeeded @ %s:%d", __FILE__, __LINE__);
#endif /* SCR_MACHINE_TYPE == SCR_PMIX */

#ifdef HAVE_LIBCPPR
  /* attempt to init cppr */
  int cppr_ret = cppr_status();
  if (cppr_ret != CPPR_SUCCESS) {
    scr_abort(-1, "libcppr cppr_status() failed: %d '%s' @ %s:%d",
              cppr_ret, cppr_err_to_str(cppr_ret), __FILE__, __LINE__
    );
  }
  scr_dbg(1, "#bold CPPR is present @ %s:%d", __FILE__, __LINE__);
#endif /* HAVE_LIBCPPR */

  /* place the halt, flush, and nodes files in the prefix directory */
  scr_halt_file = scr_path_from_str(scr_prefix_scr);
  scr_path_append_str(scr_halt_file, "halt.scr");

  scr_flush_file = scr_path_from_str(scr_prefix_scr);
  scr_path_append_str(scr_flush_file, "flush.scr");

  scr_nodes_file = scr_path_from_str(scr_prefix_scr);
  scr_path_append_str(scr_nodes_file, "nodes.scr");

  /* build the file names using the control directory prefix */
  scr_map_file = scr_path_from_str(scr_cntl_prefix);
  scr_path_append_strf(scr_map_file, "filemap_%d.scrinfo", scr_storedesc_cntl->rank);

  scr_master_map_file = scr_path_from_str(scr_cntl_prefix);
  scr_path_append_str(scr_master_map_file, "filemap.scrinfo");

  scr_path* path_transfer_file = scr_path_from_str(scr_cntl_prefix);
  scr_path_append_str(path_transfer_file, "transfer.scrinfo");
  scr_transfer_file = scr_path_strdup(path_transfer_file);
  scr_path_delete(&path_transfer_file);

  /* TODO: should we also record the list of nodes and / or MPI rank to node mapping? */
  /* record the number of nodes being used in this job to the nodes file */
  /* Each rank records its node number in the global scr_my_hostid */
  //  int ranks_across;
  //MPI_Comm_size(scr_comm_node_across, &ranks_across);
  if (scr_my_rank_world == 0) {
    scr_hash* nodes_hash = scr_hash_new();
    scr_hash_util_set_int(nodes_hash, SCR_NODES_KEY_NODES, num_nodes);
    scr_hash_write_path(scr_nodes_file, nodes_hash);
    scr_hash_delete(&nodes_hash);
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

  /* since we shuffle files around below, stop any ongoing async flush */
  if (scr_flush_async) {
    /* wait until transfer daemon is stopped */
    scr_flush_async_stop();
    /* clear out the file */
    if (scr_storedesc_cntl->rank == 0) {
      scr_file_unlink(scr_transfer_file);
    }
  }

  /* exit right now if we need to halt */
  scr_bool_check_halt_and_decrement(SCR_TEST_AND_HALT, 0);

  /* if the code is restarting from the parallel file system,
   * disable fetch and enable flush_on_restart */
  if (scr_global_restart) {
    scr_flush_on_restart = 1;
    scr_fetch = 0;
  }

  /* allocate a new global filemap object */
  scr_map = scr_filemap_new();

  /* master on each node reads all filemaps and distributes them to other ranks
   * on the node, we take this step in case the number of ranks on this node
   * has changed since the last run */
  scr_scatter_filemaps(scr_map);

  /* attempt to distribute files for a restart */
  int rc = SCR_FAILURE;
  if (rc != SCR_SUCCESS && scr_distribute) {
    /* distribute and rebuild files in cache,
     * sets scr_dataset_id and scr_checkpoint_id upon success */
    rc = scr_cache_rebuild(scr_map);

    /* if distribute succeeds, check whether we should flush on restart */
    if (rc == SCR_SUCCESS) {
      /* since the flush file is not deleted between job allocations,
       * we need to rebuild it based on what's currently in cache,
       * if the rebuild failed, we'll delete the flush file after purging the cache below */
      scr_flush_file_rebuild(scr_map);

      /* check whether we need to flush data */
      if (scr_flush_on_restart) {
        /* always flush on restart if scr_flush_on_restart is set */
        scr_flush_sync(scr_map, scr_checkpoint_id);
      } else {
        /* otherwise, flush only if we need to flush */
        scr_check_flush(scr_map);
      }
    }
  }

  /* TODO: there is some risk here of cleaning the cache when we shouldn't
   * if given a badly placed nodeset for a restart job step within an
   * allocation with lots of spares. */

  /* if the distribute fails, or if the code must restart from the parallel
   * file system, clear the cache */
  if (rc != SCR_SUCCESS || scr_global_restart) {
    /* clear the cache of all files */
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
    rc = scr_fetch_sync(scr_map, &fetch_attempted);
    if (scr_my_rank_world == 0) {
      scr_dbg(2, "scr_fetch_sync attempted on restart");
    }
  }

  /* TODO: there is some risk here of cleaning the cache when we shouldn't
   * if given a badly placed nodeset for a restart job step within an
   * allocation with lots of spares. */

  /* if the fetch fails, lets clear the cache */
  if (rc != SCR_SUCCESS) {
    /* clear the cache of all files */
    scr_cache_purge(scr_map);
    scr_dataset_id    = 0;
    scr_checkpoint_id = 0;
  }

  /* both the distribute and the fetch failed */
  if (rc != SCR_SUCCESS) {
    /* if a fetch was attempted but failed, print a warning */
    if (scr_my_rank_world == 0 && fetch_attempted) {
      scr_err("Failed to fetch checkpoint set into cache. Restarting from the beginning @ %s:%d",
        __FILE__, __LINE__
      );
    }
    /* We are restarting from the trivial checkpoint (full restart) */
    rc = SCR_SUCCESS;
  }

  /* set flag depending on whether checkpoint_id is greater than 0,
   * we'll take this to mean that we have a checkpoint in cache */
  scr_have_restart = (scr_checkpoint_id > 0);

  /* sync everyone before returning to ensure that subsequent
   * calls to SCR functions are valid */
  MPI_Barrier(scr_comm_world);

  /* start the clocks for measuring the compute time and time of last checkpoint */
  if (scr_my_rank_world == 0) {
    /* set the checkpoint end time, we use this time in Need_checkpoint */
    scr_time_checkpoint_end = MPI_Wtime();

    /* start the clock for measuring the compute time */
    scr_time_compute_start = scr_time_checkpoint_end;

    /* log the start time of this compute phase */
    if (scr_log_enable) {
      int compute_id = scr_dataset_id + 1;
      scr_timestamp_compute_start = scr_log_seconds();
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
    scr_halt(SCR_FINALIZE_CALLED);
  }

  /* TODO: flush any output sets and latest checkpoint set if needed */

  /* handle any async flush */
  if (scr_flush_async_in_progress) {
    if (scr_flush_async_dataset_id == scr_dataset_id) {
#ifdef HAVE_LIBCPPR
      /* if we have CPPR, async flush is faster than sync flush, so let it finish */
      scr_flush_async_wait(scr_map);
#else
      /* we're going to sync flush this same checkpoint below, so kill it if it's from POSIX */
      /* else wait */
      /* get the TYPE of the store for checkpoint */
      /* neither strdup nor free */
      scr_reddesc* reddesc = scr_reddesc_for_checkpoint(scr_dataset_id,
							scr_nreddescs,
							scr_reddescs);
      int storedesc_index = scr_storedescs_index_from_name(reddesc->base);
      char* type = scr_storedescs[storedesc_index].type;
      if(!strcmp(type, "DATAWARP") || !strcmp(type, "DW")){
	scr_flush_async_wait(scr_map);
      }else{//if type posix
	scr_flush_async_stop();
      }
#endif
    } else {
      /* the async flush is flushing a different checkpoint, so wait for it */
      scr_flush_async_wait(scr_map);
    }
  }

  /* flush checkpoint set if we need to */
  if (scr_bool_need_flush(scr_checkpoint_id)) {
    if (scr_my_rank_world == 0) {
      scr_dbg(2, "Sync flush in SCR_Finalize @ %s:%d", __FILE__, __LINE__);
    }
    scr_flush_sync(scr_map, scr_checkpoint_id);
  }

  if(scr_flush_async){
    scr_flush_async_shutdown();
  }

  /* disconnect from database */
  if (scr_my_rank_world == 0 && scr_log_enable) {
    scr_log_finalize();
  }

  /* TODO MEMFS: unmount storage */

  /* free off the memory allocated for our descriptors */
  scr_storedescs_free();
  scr_groupdescs_free();
  scr_reddescs_free();

  /* delete the descriptor hashes */
  scr_hash_delete(&scr_storedesc_hash);
  scr_hash_delete(&scr_groupdesc_hash);
  scr_hash_delete(&scr_reddesc_hash);

  /* Free memory cache of a halt file */
  scr_hash_delete(&scr_halt_hash);

  /* free off our global filemap object */
  scr_filemap_delete(&scr_map);

  /* free off the library's communicators */
  if (scr_comm_node_across != MPI_COMM_NULL) {
    MPI_Comm_free(&scr_comm_node_across);
  }
  if (scr_comm_node != MPI_COMM_NULL) {
    MPI_Comm_free(&scr_comm_node);
  }
  if (scr_comm_world != MPI_COMM_NULL) {
    MPI_Comm_free(&scr_comm_world);
  }

  /* free memory allocated for variables */
  scr_free(&scr_username);
  scr_free(&scr_jobid);
  scr_free(&scr_jobname);
  scr_free(&scr_clustername);
  scr_free(&scr_group);
  scr_free(&scr_transfer_file);
  scr_free(&scr_prefix_scr);
  scr_free(&scr_prefix);
  scr_free(&scr_cntl_prefix);
  scr_free(&scr_cntl_base);
  scr_free(&scr_cache_base);
  scr_free(&scr_my_hostname);

  scr_path_delete(&scr_map_file);
  scr_path_delete(&scr_master_map_file);
  scr_path_delete(&scr_nodes_file);
  scr_path_delete(&scr_flush_file);
  scr_path_delete(&scr_halt_file);
  scr_path_delete(&scr_prefix_path);

  /* we're no longer in an initialized state */
  scr_initialized = 0;

#ifdef HAVE_LIBDTCMP
  /* shut down the DTCMP library if we're using it */
  int dtcmp_rc = DTCMP_Finalize();
  if (dtcmp_rc != DTCMP_SUCCESS) {
    scr_abort(-1, "Failed to finalized DTCMP library @ %s:%d",
      __FILE__, __LINE__
    );
  }
#endif /* HAVE_LIBDTCMP */

#ifdef HAVE_LIBPMIX
  /* sync procs in pmix before shutdown */
  int retval = PMIx_Fence(NULL, 0, NULL, 0);
  if (retval != PMIX_SUCCESS) {
    scr_err("PMIx_Fence failed: rc=%d, rank: %d @ %s:%d",
      retval, scr_pmix_proc.rank, __FILE__, __LINE__
    );
  }

  /* shutdown pmix */
  retval = PMIx_Finalize(NULL, 0);
  if (retval != PMIX_SUCCESS) {
    scr_err("PMIx_Finalize failed: rc=%d, rank: %d @ %s:%d",
      retval, scr_pmix_proc.rank, __FILE__, __LINE__
    );
  }
#endif /* HAVE_LIBPMIX */

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
    scr_abort(-1, "SCR has not been initialized @ %s:%d",
      __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* track the number of times a user has called SCR_Need_checkpoint */
  scr_need_checkpoint_count++;

  /* assume we don't need to checkpoint */
  *flag = 0;

  /* check whether a halt condition is active (don't halt,
   * just be sure to return 1 in this case) */
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

    /* check whether we can afford to checkpoint based on the max allowed
     * checkpoint overhead, if set */
    if (!*flag && scr_checkpoint_overhead > 0) {
      /* TODO: could init the cost estimate via environment variable or
       * stats from previous run */
      if (scr_time_checkpoint_count == 0) {
        /* if we haven't taken a checkpoint, we need to take one in order
         * to get a cost estimate */
        *flag = 1;
      } else if (scr_time_checkpoint_count > 0) {
        /* based on average time of checkpoint, current time, and time
         * that last checkpoint ended, determine overhead of checkpoint
         * if we took one right now */
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

/* inform library that a new output dataset is starting */
int SCR_Start_output(char* name, int flags)
{
  /* if not enabled, bail with an error */
  if (! scr_enabled) {
    return SCR_FAILURE;
  }

  /* bail out if not initialized -- will get bad results */
  if (! scr_initialized) {
    scr_abort(-1, "SCR has not been initialized @ %s:%d",
      __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* bail out if user called Start_output twice without Complete_output in between */
  if (scr_in_output) {
    scr_abort(-1, "SCR_Complete_output must be called before SCR_Start_output is called again @ %s:%d",
      __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* make sure everyone is ready to start before we delete any existing checkpoints */
  MPI_Barrier(scr_comm_world);

  /* set the output flag to indicate we have started a new output dataset */
  scr_in_output = 1;

  /* stop clock recording compute time */
  if (scr_my_rank_world == 0) {
    /* stop the clock for measuring the compute time */
    scr_time_compute_end = MPI_Wtime();

    /* log the end of this compute phase */
    if (scr_log_enable) {
      int compute_id = scr_dataset_id + 1;
      double time_diff = scr_time_compute_end - scr_time_compute_start;
      time_t now = scr_log_seconds();
      scr_log_event("COMPUTE COMPLETED", NULL, &compute_id, &now, &time_diff);
    }
  }

  /* increment our dataset and checkpoint counters */
  scr_dataset_id++;
  if (flags & SCR_FLAG_CHECKPOINT) {
    scr_checkpoint_id++;
  }

  /* TODO: if we know of an existing dataset with the same name
   * delete all files */

  /* TODO: if name or flags differ across ranks, error out */

  /* check that we got  valid name */
  char* dataset_name = name;
  char dataset_name_default[SCR_MAX_FILENAME];
  if (name == NULL || strcmp(name, "") == 0) {
    /* caller didn't provide a name, so build our default */
    snprintf(dataset_name_default, sizeof(dataset_name_default), "scr.dataset.%d", scr_dataset_id);
    dataset_name = dataset_name_default;
  }
  
  /* TODO: pick different redundancy descriptors depending on
   * whether this dataset is a checkpoint or not */

  /* get the redundancy descriptor for this checkpoint id */
  scr_reddesc* reddesc = scr_reddesc_for_checkpoint(scr_checkpoint_id, scr_nreddescs, scr_reddescs);

  /* TODO: add timers for non-checkpoint output */

  /* start the clock to record how long it takes to write output */
  if (scr_my_rank_world == 0) {
    scr_time_checkpoint_start = MPI_Wtime();

    /* log the start of this checkpoint phase */
    if (scr_log_enable) {
      scr_timestamp_checkpoint_start = scr_log_seconds();
      scr_log_event("OUTPUT STARTED", reddesc->base, &scr_dataset_id, &scr_timestamp_checkpoint_start, NULL);
    }
  }

  /* get an ordered list of the datasets currently in cache */
  int ndsets;
  int* dsets = NULL;
  scr_filemap_list_datasets(scr_map, &ndsets, &dsets);

  /* lookup the number of datasets we're allowed to keep in
   * the base for this dataset */
  int size = 0;
  int store_index = scr_storedescs_index_from_name(reddesc->base);
  if (store_index >= 0) {
    size = scr_storedescs[store_index].max_count;
  }

  int i;
  char* base = NULL;

  /* run through each of our datasets and count how many we have in this base */
  int nckpts_base = 0;
  for (i=0; i < ndsets; i++) {
    /* TODODSET: need to check whether this dataset is really a checkpoint */

    /* get base for this dataset and increase count if it matches the target base */
    base = scr_reddesc_base_from_filemap(scr_map, dsets[i], scr_my_rank_world);
    if (base != NULL) {
      if (strcmp(base, reddesc->base) == 0) {
        nckpts_base++;
      }
      scr_free(&base);
    }
  }

  /* run through and delete datasets from base until we make room for the current one */
  int flushing = -1;
  for (i=0; i < ndsets && nckpts_base >= size; i++) {
    /* TODODSET: need to check whether this dataset is really a checkpoint */

    base = scr_reddesc_base_from_filemap(scr_map, dsets[i], scr_my_rank_world);
    if (base != NULL) {
      if (strcmp(base, reddesc->base) == 0) {
        if (! scr_bool_is_flushing(dsets[i])) {
          /* this dataset is in our base, and it's not being flushed, so delete it */
          scr_cache_delete(scr_map, dsets[i]);
          nckpts_base--;
        } else if (flushing == -1) {
          /* this dataset is in our base, but we're flushing it, don't delete it */
          flushing = dsets[i];
        }
      }
      scr_free(&base);
    }
  }

  /* if we still don't have room and we're flushing, the dataset we need to delete
   * must be flushing, so wait for it to finish */
  if (nckpts_base >= size && flushing != -1) {
    /* TODO: we could increase the transfer bandwidth to reduce our wait time */

    /* wait for this dataset to complete its flush */
    scr_flush_async_wait(scr_map);

    /* now dataset is no longer flushing, we can delete it and continue on */
    scr_cache_delete(scr_map, flushing);
    nckpts_base--;
  }

  /* free the list of datasets */
  scr_free(&dsets);

  /* rank 0 builds dataset object and broadcasts it out to other ranks */
  scr_dataset* dataset = scr_dataset_new();
  if (scr_my_rank_world == 0) {
    /* capture time and build name of dataset */
    int64_t dataset_time = scr_time_usecs();

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
    if (flags & SCR_FLAG_CHECKPOINT) {
      scr_dataset_set_ckpt(dataset, scr_checkpoint_id);
    }
  }
  scr_hash_bcast(dataset, 0, scr_comm_world);
  scr_filemap_set_dataset(scr_map, scr_dataset_id, scr_my_rank_world, dataset);
  scr_dataset_delete(&dataset);

  /* TODO: may want to allow user to specify these values per dataset */
  /* store variables needed for scavenge */
  scr_hash* flushdesc = scr_hash_new();
  scr_hash_util_set_int(flushdesc, SCR_SCAVENGE_KEY_PRESERVE,  scr_preserve_directories);
  scr_hash_util_set_int(flushdesc, SCR_SCAVENGE_KEY_CONTAINER, scr_use_containers);
  scr_filemap_set_flushdesc(scr_map, scr_dataset_id, scr_my_rank_world, flushdesc);
  scr_hash_delete(&flushdesc);

  /* store the redundancy descriptor in the filemap, so if we die before completing
   * the dataset, we'll have a record of the new directory we're about to create */
  scr_hash* my_desc_hash = scr_hash_new();
  scr_reddesc_store_to_hash(reddesc, my_desc_hash);
  scr_filemap_set_desc(scr_map, scr_dataset_id, scr_my_rank_world, my_desc_hash);
  scr_filemap_write(scr_map_file, scr_map);
  scr_hash_delete(&my_desc_hash);

  /* make directory in cache to store files for this dataset */
  scr_cache_dir_create(reddesc, scr_dataset_id);

  /* print a debug message to indicate we've started the dataset */
  if (scr_my_rank_world == 0) {
    scr_dbg(1, "Starting dataset %s", dataset_name);
  }

  return SCR_SUCCESS;
}

/* informs SCR that a fresh checkpoint set is about to start */
int SCR_Start_checkpoint()
{
  /* delegate the rest to Start_output */
  return SCR_Start_output(NULL, SCR_FLAG_CHECKPOINT);
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
    scr_abort(-1, "SCR has not been initialized @ %s:%d",
      __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* get the redundancy descriptor for the current checkpoint */
  scr_reddesc* reddesc = scr_reddesc_for_checkpoint(scr_checkpoint_id, scr_nreddescs, scr_reddescs);

  /* route the file */
  int n = SCR_MAX_FILENAME;
  if (scr_route_file(reddesc, scr_dataset_id, file, newfile, n) != SCR_SUCCESS) {
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
    scr_meta_set_filetype(meta, SCR_META_FILE_USER);
    scr_meta_set_complete(meta, 0);
    /* TODODSET: move the ranks field elsewhere, for now it's needed by scr_index.c */
    scr_meta_set_ranks(meta, scr_ranks_world);
    scr_meta_set_orig(meta, file);

    /* build absolute path to file */
    scr_path* path_abs = scr_path_from_str(file);
    if (scr_preserve_directories) {
      if (! scr_path_is_absolute(path_abs)) {
        /* the path is not absolute, so prepend the current working directory */
        char cwd[SCR_MAX_FILENAME];
        if (scr_getcwd(cwd, sizeof(cwd)) == SCR_SUCCESS) {
          scr_path_prepend_str(path_abs, cwd);
        } else {
          /* problem acquiring current working directory */
          scr_abort(-1, "Failed to build absolute path to %s @ %s:%d",
            file, __FILE__, __LINE__
          );
        }
      }
    } else {
      /* we're not preserving directories,
       * so drop file in prefix/scr.dataset.id/filename */
      scr_path_basename(path_abs);
      scr_path_prepend_strf(path_abs, "scr.dataset.%d", scr_dataset_id);
      scr_path_prepend(path_abs, scr_prefix_path);
    }

    /* simplify the absolute path (removes "." and ".." entries) */
    scr_path_reduce(path_abs);

    /* check that file is somewhere under prefix */
    if (! scr_path_is_child(scr_prefix_path, path_abs)) {
      /* found a file that's outside of prefix, throw an error */
      char* path_abs_str = scr_path_strdup(path_abs);
      scr_abort(-1, "File `%s' must be under SCR_PREFIX `%s' @ %s:%d",
        path_abs_str, scr_prefix, __FILE__, __LINE__
      );
    }

    /* cut absolute path into direcotry and file name */
    scr_path* path_name = scr_path_cut(path_abs, -1);

    /* store the full path and name of the original file */
    char* path = scr_path_strdup(path_abs);
    char* name = scr_path_strdup(path_name);
    scr_meta_set_origpath(meta, path);
    scr_meta_set_origname(meta, name);
    scr_free(&name);
    scr_free(&path);

    /* free directory and file name paths */
    scr_path_delete(&path_name);
    scr_path_delete(&path_abs);

    /* record the meta data for this file */
    scr_filemap_set_meta(scr_map, scr_dataset_id, scr_my_rank_world, newfile, meta);

    /* write out the filemap */
    scr_filemap_write(scr_map_file, scr_map);

    /* delete the meta data object */
    scr_meta_delete(&meta);
  } else {
    /* if we can't read the file, return an error */
    if (scr_file_is_readable(newfile) != SCR_SUCCESS) {
      return SCR_FAILURE;
    }
  }

  return SCR_SUCCESS;
}

/* inform library that the current dataset is complete */
int SCR_Complete_output(int valid)
{
  /* if not enabled, bail with an error */
  if (! scr_enabled) {
    return SCR_FAILURE;
  }

  /* bail out if not initialized -- will get bad results */
  if (! scr_initialized) {
    scr_abort(-1, "SCR has not been initialized @ %s:%d",
      __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* bail out if there is no active call to Start_output */
  if (! scr_in_output) {
    scr_abort(-1, "SCR_Start_output must be called before SCR_Complete_output @ %s:%d",
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
    unsigned long filesize = scr_file_size(file);
    my_counts[1] += filesize;

   /* TODO: record permissions and/or timestamps? */

    /* fill in filesize and complete flag in the meta data for the file */
    scr_meta* meta = scr_meta_new();
    scr_filemap_get_meta(scr_map, scr_dataset_id, scr_my_rank_world, file, meta);
    scr_meta_set_filesize(meta, filesize);
    scr_meta_set_complete(meta, valid);
    scr_filemap_set_meta(scr_map, scr_dataset_id, scr_my_rank_world, file, meta);
    scr_meta_delete(&meta);
  }

  /* TODODSET: we may want to delay setting COMPLETE in the dataset until after copy call? */

  /* we execute a sum as a logical allreduce to determine whether everyone is valid
   * we interpret the result to be true only if the sum adds up to the number of processes */
  if (valid) {
    my_counts[2] = 1;
  }

  /* execute allreduce */
  unsigned long total_counts[3];
  MPI_Allreduce(my_counts, total_counts, 3, MPI_UNSIGNED_LONG, MPI_SUM, scr_comm_world);

  /* get dataset from filemap */
  scr_dataset* dataset = scr_dataset_new();
  scr_filemap_get_dataset(scr_map, scr_dataset_id, scr_my_rank_world, dataset);

  /* store total number of files, total number of bytes, and complete flag in dataset */
  scr_dataset_set_files(dataset, (int) total_counts[0]);
  scr_dataset_set_size(dataset,        total_counts[1]);
  if (total_counts[2] == scr_ranks_world) {
    scr_dataset_set_complete(dataset, 1);
  } else {
    scr_dataset_set_complete(dataset, 0);
  }
  scr_filemap_set_dataset(scr_map, scr_dataset_id, scr_my_rank_world, dataset);

  /* write out info to filemap */
  scr_filemap_write(scr_map_file, scr_map);

  /* record name of dataset in flush file */
  char* dset_name;
  scr_dataset_get_name(dataset, &dset_name);
  scr_flush_file_name_set(scr_dataset_id, dset_name);

  /* done with dataset */
  scr_dataset_delete(&dataset);

  /* TODO: PRESERVE preprocess info needed for flush/scavenge, e.g., container offsets,
   * list of directories to create, etc. we should also apply redundancy to this info,
   * this could be done in flush, but it's hard to do in scavenge */
  if (scr_flush_verify(scr_map, scr_dataset_id) != SCR_SUCCESS) {
    scr_abort(-1, "Dataset cannot be flushed @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* TODO: pick different redundancy scheme depending on whether dataset is a checkpoint */

  /* apply redundancy scheme */
  double bytes_copied = 0.0;
  scr_reddesc* reddesc = scr_reddesc_for_checkpoint(scr_checkpoint_id, scr_nreddescs, scr_reddescs);
  int rc = scr_reddesc_apply(scr_map, reddesc, scr_dataset_id, &bytes_copied);

  /* TODO: set size of dataset and complete flag */

  /* TODO: add timers for non-checkpoint output */

  /* record the cost of the output and log its completion */
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
      scr_log_event("CHECKPOINT COMPLETED", reddesc->base, &scr_dataset_id, &now, &time_diff);

      /* log the transfer details */
      char* dir = scr_cache_dir_get(reddesc, scr_dataset_id);
      scr_log_transfer("CHECKPOINT", reddesc->base, dir, &scr_dataset_id,
        &scr_timestamp_checkpoint_start, &cost, &bytes_copied
      );
      scr_free(&dir);
    }

    /* print out a debug message with the result of the copy */
    scr_dbg(2, "Completed dataset %d with return code %d",
      scr_dataset_id, rc
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
    if (scr_flush_async_test(scr_map, scr_flush_async_dataset_id, &bytes) == SCR_SUCCESS) {
      /* async flush has finished, go ahead and complete it */
      scr_flush_async_complete(scr_map, scr_flush_async_dataset_id);
    } else {
      /* not done yet, just print a progress message to the screen */
      if (scr_my_rank_world == 0) {
        scr_dbg(1, "Flush of dataset %d is %d%% complete",
          scr_flush_async_dataset_id, (int) (bytes / scr_flush_async_bytes * 100.0)
        );
      }
    }
  }

  /* make sure everyone is ready before we exit */
  MPI_Barrier(scr_comm_world);

  /* unset the output flag to indicate we have exited the current output phase */
  scr_in_output = 0;

  /* start the clock for measuring the compute time */
  if (scr_my_rank_world == 0) {
    scr_time_compute_start = MPI_Wtime();

    /* log the start time of this compute phase */
    if (scr_log_enable) {
      int compute_id = scr_dataset_id + 1;
      scr_timestamp_compute_start = scr_log_seconds();
      scr_log_event("COMPUTE STARTED", NULL, &compute_id, &scr_timestamp_compute_start, NULL);
    }
  }

  return rc;
}

/* completes the checkpoint set and marks it as valid or not */
int SCR_Complete_checkpoint(int valid)
{
  return SCR_Complete_output(valid);
}

/* determine whether SCR has a restart available to read,
 * and get name of restart if one is available */
int SCR_Have_restart(int* flag, char* name)
{
  /* if not enabled, bail with an error */
  if (! scr_enabled) {
    *flag = 0;
    return SCR_FAILURE;
  }

  /* say no if not initialized */
  if (! scr_initialized) {
    *flag = 0;
    scr_abort(-1, "SCR has not been initialized @ %s:%d",
      __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* TODO: a more proper check would be to examine the filemap, perhaps across ranks */

  /* set flag depending on whether checkpoint_id is greater than 0,
   * we'll take this to mean that we have a checkpoint in cache */
  *flag = scr_have_restart;

  /* TODO: look up name by checkpoint id (in addition to dataset id) */

  /* assume scr_checkpoint_id == scr_dataset_id for now */
  /* read dataset name from filemap */
  if (scr_have_restart) {
    if (name != NULL) {
      char* dset_name;
      scr_dataset* dataset = scr_dataset_new();
      scr_filemap_get_dataset(scr_map, scr_dataset_id, scr_my_rank_world, dataset);
      scr_dataset_get_name(dataset, &dset_name);
      strncpy(name, dset_name, SCR_MAX_FILENAME);
      scr_dataset_delete(&dataset);
    }
  }

  return SCR_SUCCESS;
}

/* inform library that restart is starting,
 * get name of restart that is available */
int SCR_Start_restart(char* name)
{
  /* if not enabled, bail with an error */
  if (! scr_enabled) {
    return SCR_FAILURE;
  }

  /* bail out if not initialized -- will get bad results */
  if (! scr_initialized) {
    scr_abort(-1, "SCR has not been initialized @ %s:%d",
      __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* bail out if there is no checkpoint to restart from */
  if (! scr_have_restart) {
    scr_abort(-1, "SCR has no checkpoint for restart @ %s:%d",
      __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* TODO: look up name by checkpoint id (in addition to dataset id) */

  /* assume scr_checkpoint_id == scr_dataset_id for now */
  /* read dataset name from filemap */
  if (name != NULL) {
    char* dset_name;
    scr_dataset* dataset = scr_dataset_new();
    scr_filemap_get_dataset(scr_map, scr_dataset_id, scr_my_rank_world, dataset);
    scr_dataset_get_name(dataset, &dset_name);
    strncpy(name, dset_name, SCR_MAX_FILENAME);
    scr_dataset_delete(&dataset);
  }

  return SCR_SUCCESS;
}

/* inform library that the current restart is complete */
int SCR_Complete_restart(int valid)
{
  /* if not enabled, bail with an error */
  if (! scr_enabled) {
    return SCR_FAILURE;
  }

  /* bail out if not initialized -- will get bad results */
  if (! scr_initialized) {
    scr_abort(-1, "SCR has not been initialized @ %s:%d",
      __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* turn off our restart flag */
  scr_have_restart = 0;

  return SCR_SUCCESS;
}

/* get and return the SCR version */
char* SCR_Get_version()
{
  return SCR_VERSION;
}
