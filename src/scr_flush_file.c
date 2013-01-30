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

#include "scr.h"
#include "scr_io.h"
#include "scr_err.h"
#include "scr_util.h"
#include "scr_hash.h"
#include "scr_hash_util.h"

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

int print_usage()
{
  printf("\n");
  printf("  Usage:  %s --dir <dir> [--latest || --needflush <id> | --location <id> | --subdir <id>]\n", PROG);
  printf("\n");
  exit(1);
}

struct arglist {
  char* dir;      /* direcotry containing flush file */
  int need_flush; /* check whether a certain dataset id needs to be flushed */
  int latest;     /* return the id of the latest (most recent) dataset in cache */
  int location;   /* return the location of dataset with specified id in cache */
  int subdir;     /* subdirectory (relative to prefix) to create to hold dataset */
};

int process_args(int argc, char **argv, struct arglist* args)
{
  int tmp_dset;
  int opCount = 0;

  /* define our options */
  static struct option long_options[] = {
    {"dir",       required_argument, NULL, 'd'},
    {"needflush", required_argument, NULL, 'n'},
    {"latest",    no_argument,       NULL, 'l'},
    {"location",  required_argument, NULL, 'L'},
    {"subdir",    required_argument, NULL, 's'},
    {"help",      no_argument,       NULL, 'h'},
    {0, 0, 0, 0}
  };

  /* set our options to default values */
  args->dir        = NULL;
  args->need_flush = -1;
  args->latest     = 0;
  args->location   = -1;
  args->subdir     = -1;

  /* loop through and process all options */
  int c;
  do {
    /* read in our next option */
    int option_index = 0;
    c = getopt_long(argc, argv, "d:n:lL:h", long_options, &option_index);
    switch (c) {
      case 'd':
        /* directory containing flush file */
        args->dir = optarg;
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
        /* check whether specified dataset id needs to be flushed */
        tmp_dset = atoi(optarg);
        if (tmp_dset <= 0) {
          return 0;
        }
        args->location = tmp_dset;
        ++opCount;
        break;
      case 's':
        /* subdirectory (relative to prefix) to create to hold dataset */
        tmp_dset = atoi(optarg);
        if (tmp_dset <= 0) {
          return 0;
        }
        args->subdir = tmp_dset;
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
  if (opCount > 1){
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

  /* build path to flush file */
  scr_path* file_path = scr_path_from_str(args.dir);
  scr_path_append_str(file_path, ".scr");
  scr_path_append_str(file_path, "flush.scr");
  scr_path_reduce(file_path);
  char* file = scr_path_strdup(file_path);
  scr_path_delete(&file_path);
  if (file == NULL) {
    scr_err("%s: Failed to allocate storage to store nodes file name @ %s:%d",
      PROG, __FILE__, __LINE__
    );
    return 1;
  }

  /* assume we'll fail */
  int rc = 1;

  /* create a new hash to hold the file data */
  scr_hash* hash = scr_hash_new();

  /* read in our flush file */
  if (scr_hash_read(file, hash) != SCR_SUCCESS) {
    /* failed to read the flush file */
    goto cleanup;
  }

  /* check whether a specified dataset id needs to be flushed */
  if (args.need_flush != -1) {
    /* first, see if we have this dataset */
    scr_hash* dset_hash = scr_hash_get_kv_int(hash, SCR_FLUSH_KEY_DATASET, args.need_flush);
    if (dset_hash != NULL) {
      /* now check for the PFS location marker */
      scr_hash* location_hash = scr_hash_get(dset_hash, SCR_FLUSH_KEY_LOCATION);
      scr_hash_elem* pfs_elem = scr_hash_elem_get(location_hash, SCR_FLUSH_KEY_LOCATION_PFS);
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
    scr_hash* dset_hash = scr_hash_get_kv_int(hash, SCR_FLUSH_KEY_DATASET, args.location);
    if (dset_hash != NULL) {
      /* now check for the location marker */
      scr_hash* location_hash = scr_hash_get(dset_hash, SCR_FLUSH_KEY_LOCATION);
      if (location_hash != NULL) {
         rc = 0;
         scr_hash_elem* loc_elem = scr_hash_elem_first(location_hash);
         if (loc_elem != NULL) {
            /* if the location exists in the file, print it */
            char* loc = scr_hash_elem_key(loc_elem);
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

  /* check whether we should report subdirectory for dataset */
  if (args.subdir != -1) {
    /* first check whether we have the requested dataset */
    scr_hash* dset_hash = scr_hash_get_kv_int(hash, SCR_FLUSH_KEY_DATASET, args.subdir);
    if (dset_hash != NULL) {
      /* now check for the subdir */
      char* subdir;
      if (scr_hash_util_get_str(dset_hash, SCR_FLUSH_KEY_DIRECTORY, &subdir) == SCR_SUCCESS) {
        /* got subdirectory, print it and return success */
        printf("%s\n", subdir);
        rc = 0;
      }
    }
    goto cleanup;
  }

  /* print the latest dataset id to stdout */
  if (args.latest) {
    /* scan through the dataset ids to find the most recent */
    int latest_dset = -1;
    scr_hash_elem* elem;
    scr_hash* latest_hash = scr_hash_get(hash, SCR_FLUSH_KEY_DATASET);
    for (elem = scr_hash_elem_first(latest_hash);
         elem != NULL;
         elem = scr_hash_elem_next(elem))
    {
      /* update our latest dataset id if this dataset is more recent */
      int dset = scr_hash_elem_key_int(elem);
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
  scr_hash_delete(&hash);

  /* free off our file name storage */
  scr_free(&file);

  /* return appropriate exit code */
  return rc;
}
