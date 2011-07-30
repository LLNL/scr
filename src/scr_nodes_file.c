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
#include "scr_hash.h"

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
  int ckpt;

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
  if (!process_args(argc, argv, &args)) {
    return 1;
  }

  /* determine the number of bytes we need to hold the full name of the nodes file */
  int filelen = snprintf(NULL, 0, "%s/nodes.scr", args.dir);
  filelen++; /* add one for the terminating NUL char */

  /* allocate space to store the filename */
  char* file = NULL;
  if (filelen > 0) {
    file = (char*) malloc(filelen);
  }
  if (file == NULL) {
    scr_err("%s: Failed to allocate storage to store nodes file name @ %s:%d",
            PROG, __FILE__, __LINE__
    );
    return 1;
  }

  /* build the full file name */
  int n = snprintf(file, filelen, "%s/nodes.scr", args.dir);
  if (n >= filelen) {
    scr_err("%s: Flush file name is too long (need %d bytes, %d byte buffer) @ %s:%d",
            PROG, n, filelen, __FILE__, __LINE__
    );
    free(file);
    return 1;
  }

  /* assume we'll fail */
  int rc = 1;

  /* create a new hash to hold the file data */
  scr_hash* hash = scr_hash_new();

  /* read in our nodes file */
  if (scr_hash_read(file, hash) != SCR_SUCCESS) {
    /* failed to read the nodes file */
    goto cleanup;
  }

  /* lookup the value associated with the NODES key */
  char* nodes_str = scr_hash_elem_get_first_val(hash, SCR_NODES_KEY_NODES);
  if (nodes_str != NULL) {
    printf("%s\n", nodes_str);
    rc = 0;
  } else {
    printf("0\n");
  }

cleanup:
  /* delete the hash holding the nodes file data */
  scr_hash_delete(hash);

  /* free off our file name storage */
  if (file != NULL) {
    free(file);
    file = NULL;
  }

  /* return appropriate exit code */
  return rc;
}
