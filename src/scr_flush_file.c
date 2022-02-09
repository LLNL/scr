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

/* This is a utility program that checks various values in the flush
 * file. */

#include "scr_keys.h"
#include "scr.h"
#include "scr_io.h"
#include "scr_err.h"
#include "scr_util.h"
#include "scr_dataset.h"
#include "scr_cache_index.h"
#include "scr_flush_sync.h"
#include "scr_flush_nompi.h"

#include "spath.h"
#include "kvtree.h"
#include "kvtree_util.h"
#include "axl.h"

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/file.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <unistd.h>
#include <getopt.h>

#define PROG ("scr_flush_file")

#ifdef SCR_GLOBALS_H
#error "globals.h accessed from tools"
#endif

/* Given a path to a prefix directory, the contents of the flush file,
 * an id, and the path to the flush_file, generate the summary.scr file and
 * update flush.scr to show that we're no longer flushing.
 *
 * Returns 0 on success.  Returns 1 if there's no
 * corresponding dataset_id in the flush file. */
int write_summary_file(
  char* prefix,
  kvtree* flush_file,
  int dataset_id,
  spath* flush_file_spath)
{
  /* get dataset descriptor for summary file */
  kvtree* flush_key_dataset = kvtree_get_kv_int(flush_file, SCR_FLUSH_KEY_DATASET, dataset_id);
  kvtree* summary = kvtree_get(flush_key_dataset, SCR_FLUSH_KEY_DSETDESC);
  if (summary == NULL) {
    scr_err("%s: No flush file entry for dataset %d @ %s:%d", PROG, dataset_id, __FILE__, __LINE__);
    return 1; /* No entry for this dataset */
  }

  /* define path to summary file for this dataset */
  spath* summary_file_spath = spath_from_strf("%s/.scr/scr.dataset.%d/summary.scr", prefix, dataset_id);
  char* summary_file = spath_strdup(summary_file_spath);
  spath_delete(&summary_file_spath);

  /* write summary file out and indicate that dataset is complete */
  int rc = scr_flush_summary_file(summary, 1, summary_file);

  scr_free(&summary_file);

  /* All done flushing, remove flushing marker from flush file */
  scr_flush_file_dataset_remove_with_path(dataset_id, flush_file_spath);

  rc = (rc == SCR_SUCCESS) ? 0 : 1;
  return rc;
}

/* Given a path to a state_file, resume and finalize all transfers for all
 * files in the state_file.  Returns 0 on success, 1 on error. */
int resume_transfer(char* state_file_path)
{
  int rc = 0;

  AXL_Init();

  int id = AXL_Create(AXL_XFER_STATE_FILE, "scr", state_file_path);
  if (id < 0) {
    scr_err("%s: AXL_Create() = %d @ %s:%d", PROG, id, __FILE__, __LINE__);
    rc = 1;
    goto end;
  }

  int axl_rc = AXL_Resume(id);
  if (axl_rc != AXL_SUCCESS) {
    scr_err("%s: AXL_Resume(%d) = %d @ %s:%d", PROG, id, rc, __FILE__, __LINE__);
    rc = 1;
    goto end;
  }

  axl_rc = AXL_Wait(id);
  if (axl_rc != AXL_SUCCESS) {
    scr_err("%s: AXL_Wait(%d) = %d @ %s:%d", PROG, id, rc, __FILE__, __LINE__);
    rc = 1;
    goto end;
  }

  axl_rc = AXL_Free(id);
  if (axl_rc != AXL_SUCCESS) {
    scr_err("%s: AXL_Free(%d) = %d @ %s:%d", PROG, id, rc, __FILE__, __LINE__);
    rc = 1;
    goto end;
  }

  axl_rc = AXL_Finalize();
  if (axl_rc != AXL_SUCCESS) {
    scr_err("%s: AXL_Finalize() = %d @ %s:%d", PROG, rc, __FILE__, __LINE__);
  }

end:
  return rc;
}

/* Resume and wait for any previous transfers to complete, and finalize them.
 *
 * This only resumes/waits for the AXL transfers to complete.  It does not
 * update SCR's flush file nor write the summary file.
 *
 * Returns 0 on success, non-zero otherwise. */
int resume_transfers(char* prefix, int dataset_id)
{
  /* define path to top-level rank2file map file */
  spath* rank2file_path_spath = spath_from_strf("%s/.scr/scr.dataset.%d/rank2file", prefix, dataset_id);
  char* rank2file_path = spath_strdup(rank2file_path_spath);
  spath_delete(&rank2file_path_spath);

  /* read the rank2file map file */
  kvtree* ranks_tree = kvtree_new();
  int rc = kvtree_read_scatter_single(rank2file_path, ranks_tree);
  if (rc != KVTREE_SUCCESS) {
    scr_err("%s: kvtree_read_scatter_single(%s) = %d @ %s:%d",
      PROG, rank2file_path, rc, __FILE__, __LINE__);
    return 1;
  }

  /* assume we'll succeed */
  rc = 0;

  /*
   * 'ranks_tree' is a kvtree that looks like:
   *
   *    79
   *      FILE
   *        ckpt.1/rank_79.ckpt
   *    73
   *      FILE
   *        ckpt.1/rank_73.ckpt
   *    74
   *      FILE
   *        ckpt.1/rank_74.ckpt
   */
  unsigned long i;
  int ranks = kvtree_size(ranks_tree);
  for (i = 0; i < ranks; i++) {
   /*
    * Verify there's a dataset entry for each rank.  Some ranks may not have
    * a checkpoint, and that's totally valid (like 12 below), but we should
    * sanity check for at least the existence of an entry.  Note that we
    * "resume" even those entries without any checkpoints since they will
    * still have a state_file (with no src/dst files) that  we need to get rid
    * of.
    *
    * 13
    *   FILE
    *     timestep.11/rank_13.0.ckpt
    * 12
    * 15
    *   FILE
    *     timestep.11/rank_15.0.ckpt
    *     timestep.11/rank_15.1.ckpt
    *     timestep.11/rank_15.2.ckpt
    *...
    */
    kvtree* rank_subtree = kvtree_getf(ranks_tree, "%lu", i);
    if (rank_subtree == NULL) {
      scr_err("%s: Couldn't get RANK subtree for rank = %d @ %s:%d", PROG, i, __FILE__, __LINE__);
      rc = 1;
      goto out;
    }

    spath* state_file_spath = spath_from_strf("%s/.scr/scr.dataset.%d/rank_%d.state_file",
        prefix, dataset_id, i);
    char* state_file = spath_strdup(state_file_spath);
    spath_delete(&state_file_spath);

    rc = resume_transfer(state_file);
    scr_free(&state_file);
    if (rc != 0) {
      break;
    }
  }

out:
  scr_free(&rank2file_path);
  kvtree_delete(&ranks_tree);

  return rc;
}

int print_usage()
{
  printf("\n");
  printf("  Usage:  %s --dir <dir> OPTIONS\n", PROG);
  printf("\n");
  printf("  OPTIONS:\n");
  printf("\n");
  printf("  --dir <dir>        Specify prefix directory (required)\n");
  printf("  --list-output      Return list of output dataset ids in ascending order\n");
  printf("  --list-ckpt        Return list of checkpoint dataset ids in descending order\n");
  printf("  --before <id>      Filter list of ids to those before given id\n");
  printf("  --need-flush <id>  Exit with 0 if checkpoint needs to be flushed, 1 otherwise\n");
  printf("  --location <id>    Print location of specified id\n");
  printf("  --name <id>        Print name of specified id\n");
  printf("  --resume -r        Resume/finalize a previous or ongoing transfer\n");
  printf("  --summary -S       Manually mark a transfer as complete and generate summary.scr\n");
  printf("\n");
  exit(1);
}

struct arglist {
  char* dir;      /* direcotry containing flush file */
  int list_out;   /* list output ids in ascending order */
  int list_ckpt;  /* list checkpoint ids in descending order */
  int before;     /* filter ids to those below given value */
  int need_flush; /* check whether a certain dataset id needs to be flushed */
  int latest;     /* return the id of the latest (most recent) dataset in cache */
  int location;   /* return the location of dataset with specified id in cache */
  int name;       /* dataset name (label) */
  int summary;    /* Generate a summary file for a dataset.  This is useful
                   * when you've manually transferred the dataset files
                   * outside of SCR, and need to tell SCR that they're complete. */
  int resume;     /* Resume a previous or ongoing transfer */
};

int process_args(int argc, char **argv, struct arglist* args)
{
  int tmp_dset;
  int opCount = 0;

  /* define our options */
  static struct option long_options[] = {
    {"dir",         required_argument, NULL, 'd'},
    {"list-output", no_argument,       NULL, 'o'},
    {"list-ckpt",   no_argument,       NULL, 'c'},
    {"before",      required_argument, NULL, 'b'},
    {"need-flush",  required_argument, NULL, 'n'},
    {"latest",      no_argument,       NULL, 'l'},
    {"location",    required_argument, NULL, 'L'},
    {"name",        required_argument, NULL, 's'},
    {"help",        no_argument,       NULL, 'h'},
    {"summary",     no_argument,       NULL, 'S'},
    {"resume",      no_argument,       NULL, 'r'},
    {0, 0, 0, 0}
  };

  /* set our options to default values */
  args->dir        = NULL;
  args->list_out   = 0;
  args->list_ckpt  = 0;
  args->before     = 0;
  args->need_flush = -1;
  args->latest     = 0;
  args->location   = -1;
  args->name       = -1;
  args->summary    = 0;
  args->resume     = 0;

  /* loop through and process all options */
  int c;
  do {
    /* read in our next option */
    int option_index = 0;
    c = getopt_long(argc, argv, "d:on:lL:hrSs:", long_options, &option_index);
    switch (c) {
      case 'd':
        /* directory containing flush file */
        args->dir = optarg;
        break;
      case 'o':
        /* list output sets in ascending order */
        args->list_out = 1;
        ++opCount;
        break;
      case 'c':
        /* list checkpoint sets in descending order */
        args->list_ckpt = 1;
        ++opCount;
        break;
      case 'b':
        /* filter ids to those before given value */
        args->before = atoi(optarg);
        break;
      case 'n':
        /* check whether specified dataset id needs to be flushed */
        tmp_dset = atoi(optarg);
        if (tmp_dset <= 0) {
          return 0;
        }
        args->need_flush = tmp_dset;
        ++opCount;
        break;
      case 'l':
        /* return the id of the latest (most recent) dataset in cache */
        args->latest = 1;
        ++opCount;
        break;
      case 'L':
        /* print location of specified dataset */
        tmp_dset = atoi(optarg);
        if (tmp_dset <= 0) {
          return 0;
        }
        args->location = tmp_dset;
        ++opCount;
        break;
      case 'r':
        args->resume = 1;
        ++opCount;
        break;
      case 's':
        /* dataset name */
        tmp_dset = atoi(optarg);
        if (tmp_dset <= 0) {
          return 0;
        }
        args->name = tmp_dset;
        ++opCount;
        break;
      case 'S':
        args->summary = 1;
        ++opCount;
        break;
      case 'h':
        /* print help message and exit */
        print_usage();
        break;
      case '?':
        /* getopt_long printed an error message */
        break;
      default:
        if (c != -1) {
          /* missed an option */
          scr_err("%s: Option '%s' specified but not processed",
            PROG, argv[option_index]
          );
        }
    }
  } while (c != -1);

  /* check that we got a directory name */
  if (args->dir == NULL) {
    scr_err("%s: Must specify directory containing flush file via '--dir <dir>'",
      PROG
    );
    return 0;
  }
  if (opCount > 2){
    scr_err("%s: Must specify only a single operation per invocation, e.g. not both --location and --needflush'",
      PROG
    );
    return 0;
  }

  return 1;
}

int main (int argc, char *argv[])
{
  /* process command line arguments */
  struct arglist args;
  if (! process_args(argc, argv, &args)) {
    return 1;
  }

  /* TODO: use cwd if prefix directory is not specified */

  /* build path to flush file */
  spath* file_path = spath_from_str(args.dir);
  spath_append_str(file_path, ".scr");
  spath_append_str(file_path, "flush.scr");
  spath_reduce(file_path);

  /* assume we'll fail */
  int rc = 1;

  /* create a new hash to hold the file data */
  kvtree* hash = kvtree_new();

  /* read in our flush file */
  if (kvtree_read_path(file_path, hash) != KVTREE_SUCCESS) {
    /* failed to read the flush file */
    char* file_path_str = spath_strdup(file_path);
    scr_err("%s: Failed to read flush file '%s' @ %s:%d",
      PROG, file_path_str, __FILE__, __LINE__
    );
    scr_free(&file_path_str);
    goto cleanup;
  }

  /* list output sets (if any) in ascending order */
  if (args.list_out == 1) {
    /* first, see if we have this dataset */
    kvtree* dset_hash = kvtree_get(hash, SCR_FLUSH_KEY_DATASET);
    if (dset_hash != NULL) {
      /* get ids in ascending order */
      int num;
      int* list;
      kvtree_list_int(dset_hash, &num, &list);

      /* print list of ids in ascending order */
      int i;
      int found_one = 0;
      for (i = 0; i < num; i++) {
        int id = list[i];
        if (args.before == 0 || id < args.before) {
          kvtree* dhash = kvtree_getf(dset_hash, "%d", id);
          int flag;
          if (kvtree_util_get_int(dhash, SCR_FLUSH_KEY_OUTPUT, &flag) == KVTREE_SUCCESS) {
            if (flag == 1) {
              if (found_one) {
                printf(" ");
              }
              printf("%d", id);
              found_one = 1;
            }
          }
        }
      }
      if (found_one) {
        printf("\n");
        rc = 0;
      }

      /* free sorted list of ints */
      scr_free(&list);
    }
    goto cleanup;
  }

  /* list checkpoint sets (if any) in descending order */
  if (args.list_ckpt == 1) {
    /* first, see if we have this dataset */
    kvtree* dset_hash = kvtree_get(hash, SCR_FLUSH_KEY_DATASET);
    if (dset_hash != NULL) {
      /* get ids in ascending order */
      int num;
      int* list;
      kvtree_list_int(dset_hash, &num, &list);

      /* print list of ids in reverse order */
      int i;
      int found_one = 0;
      for (i = num-1; i >= 0; i--) {
        int id = list[i];
        if (args.before == 0 || id < args.before) {
          kvtree* dhash = kvtree_getf(dset_hash, "%d", id);
          int flag;
          if (kvtree_util_get_int(dhash, SCR_FLUSH_KEY_CKPT, &flag) == KVTREE_SUCCESS) {
            if (flag == 1) {
              if (found_one) {
                printf(" ");
              }
              printf("%d", id);
              found_one = 1;
            }
          }
        }
      }
      if (found_one) {
        printf("\n");
        rc = 0;
      }

      /* free sorted list of ints */
      scr_free(&list);
    }
    goto cleanup;
  }

  /* check whether a specified dataset id needs to be flushed */
  if (args.need_flush != -1) {
    /* first, see if we have this dataset */
    kvtree* dset_hash = kvtree_get_kv_int(hash, SCR_FLUSH_KEY_DATASET, args.need_flush);
    if (dset_hash != NULL) {
      /* now check for the PFS location marker */
      kvtree* location_hash = kvtree_get(dset_hash, SCR_FLUSH_KEY_LOCATION);
      kvtree_elem* pfs_elem = kvtree_elem_get(location_hash, SCR_FLUSH_KEY_LOCATION_PFS);
      if (pfs_elem == NULL) {
        /* we have the dataset, but we didn't find the PFS marker,
         * so return success to indicate that we do need to flush it */
        rc = 0;
      }
    }
    goto cleanup;
  }

  if (args.location != -1) {
    /* report the location of the specified data set */
    kvtree* dset_hash = kvtree_get_kv_int(hash, SCR_FLUSH_KEY_DATASET, args.location);
    if (dset_hash != NULL) {
      /* now check for the location marker */
      kvtree* location_hash = kvtree_get(dset_hash, SCR_FLUSH_KEY_LOCATION);
      if (location_hash != NULL) {
         rc = 0;
         kvtree_elem* loc_elem = kvtree_elem_first(location_hash);
         if (loc_elem != NULL) {
            /* if the location exists in the file, print it */
            char* loc = kvtree_elem_key(loc_elem);
            if (loc != NULL) {
              printf("%s\n", loc);
            }
         } else {
            /* if there is no location information for some reason,
             * print none */
            printf("NONE\n");
         }
      }
      /* if specified dataset is not found, we return error in rc (1) */
    }
    goto cleanup;
  }

  if (args.resume) {
     if (args.name == -1) {
      scr_err("-r requires you to specify dataset ID with '-s <id>'.");
      goto cleanup;
    }

    rc = resume_transfers(args.dir, args.name);
    if (rc != 0) {
      goto cleanup;
    }
  }

  if (args.summary) {
    if (args.name == -1) {
      scr_err("-S requires you to specify dataset ID with '-s <id>'.");
      goto cleanup;
    }
    rc = write_summary_file(args.dir, hash, args.name, file_path);
    if (rc != 0) {
      scr_err("%s: Couldn't write summary file, rc = %d @ %s:%d", PROG, rc, __FILE__, __LINE__);
      goto cleanup;
    }
  }

  /* check whether we should report name for dataset */
  if (args.name != -1) {
    /* first check whether we have the requested dataset */
    kvtree* dset_hash = kvtree_get_kv_int(hash, SCR_FLUSH_KEY_DATASET, args.name);
    if (dset_hash != NULL) {
      /* now check for the name */
      char* name;
      if (kvtree_util_get_str(dset_hash, SCR_FLUSH_KEY_NAME, &name) == KVTREE_SUCCESS) {
        /* got name, print it and return success */
        printf("%s\n", name);
        rc = 0;
      }
    }
    goto cleanup;
  }

  /* print the latest dataset id to stdout */
  if (args.latest) {
    /* scan through the dataset ids to find the most recent */
    int latest_dset = -1;
    kvtree_elem* elem;
    kvtree* latest_hash = kvtree_get(hash, SCR_FLUSH_KEY_DATASET);
    for (elem = kvtree_elem_first(latest_hash);
         elem != NULL;
         elem = kvtree_elem_next(elem))
    {
      /* update our latest dataset id if this dataset is more recent */
      int dset = kvtree_elem_key_int(elem);
      if (dset > latest_dset) {
        latest_dset = dset;
      }
    }

    /* if we found a dataset, print its id and return success */
    if (latest_dset != -1) {
      printf("%d\n", latest_dset);
      rc = 0;
    }
    goto cleanup;
  }

cleanup:
  /* delete the hash holding the flush file data */
  kvtree_delete(&hash);

  /* return appropriate exit code */
  return rc;
}
