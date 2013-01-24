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

/* This is a utility program that reads the nodes file and prints the
 * number of nodes used by the previous job. */

#include "scr.h"
#include "scr_io.h"
#include "scr_err.h"
#include "scr_path.h"
#include "scr_util.h"
#include "scr_hash.h"
#include "scr_hash_util.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>

#define PROG ("scr_nodes_file")

int print_usage()
{
  printf("\n");
  printf("  Usage:  %s --dir <dir>\n", PROG);
  printf("\n");
  exit(1);
}

struct arglist {
  char* dir; /* direcotry containing nodes file */
};

int process_args(int argc, char **argv, struct arglist* args)
{
  /* define our options */
  static struct option long_options[] = {
    {"dir",       required_argument, NULL, 'd'},
    {"help",      no_argument,       NULL, 'h'},
    {0, 0, 0, 0}
  };

  /* set our options to default values */
  args->dir = NULL;

  /* loop through and process all options */
  int c;
  do {
    /* read in our next option */
    int option_index = 0;
    c = getopt_long(argc, argv, "d:h", long_options, &option_index);
    switch (c) {
      case 'd':
        /* directory containing nodes file */
        args->dir = optarg;
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
          scr_err("%s: Option '%s' specified but not processed", PROG, argv[option_index]);
        }
    }
  } while (c != -1);

  /* check that we got a directory name */
  if (args->dir == NULL) {
    scr_err("%s: Must specify directory containing nodes file via '--dir <dir>'", PROG);
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

  /* build the full file name */
  scr_path* file_path = scr_path_from_str(args.dir);
  scr_path_append_str(file_path, ".scr");
  scr_path_append_str(file_path, "nodes.scr");

  /* assume we'll fail */
  int rc = 1;

  /* create a new hash to hold the file data */
  scr_hash* hash = scr_hash_new();

  /* read in our nodes file */
  if (scr_hash_read_path(file_path, hash) != SCR_SUCCESS) {
    /* failed to read the nodes file */
    goto cleanup;
  }

  /* lookup the value associated with the NODES key */
  char* nodes_str;
  if (scr_hash_util_get_str(hash, SCR_NODES_KEY_NODES, &nodes_str) == SCR_SUCCESS) {
    printf("%s\n", nodes_str);
    rc = 0;
  } else {
    printf("0\n");
  }

cleanup:
  /* delete the hash holding the nodes file data */
  scr_hash_delete(&hash);

  /* free off our file name storage */
  scr_path_delete(&file_path);

  /* return appropriate exit code */
  return rc;
}
