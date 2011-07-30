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

/* This is a utility program that checks various conditions in the halt
 * file to determine whether the job should issue another run. */

#include "scr.h"
#include "scr_io.h"
#include "scr_err.h"
#include "scr_hash.h"
#include "scr_param.h"
#include "scr_halt.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <getopt.h>

#define PROG ("scr_retries_halt")
#define NAME ("halt.scr")
#define NEED_HALT (0)
#define DONT_HALT (1)

int print_usage()
{
  printf("\n");
  printf("  Usage:  %s --dir <dir>\n", PROG);
  printf("\n");
  exit(1);
}

struct arglist {
  char* dir; /* direcotry containing halt file */
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
        /* directory containing halt file */
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
    scr_err("%s: Must specify directory containing halt file via '--dir <dir>'", PROG);
    return 0;
  }

  return 1;
}

/* returns 0 if we need to halt, returns 1 otherwise */
int main (int argc, char *argv[])
{
  /* process command line arguments */
  struct arglist args;
  if (!process_args(argc, argv, &args)) {
    /* failed to process command line, to be safe, assume we need to halt */
    return NEED_HALT;
  }

  /* determine the number of bytes we need to hold the full name of the halt file */
  int filelen = snprintf(NULL, 0, "%s/%s", args.dir, NAME);
  filelen++; /* add one for the terminating NUL char */

  /* allocate space to store the filename */
  char* file = NULL;
  if (filelen > 0) {
    file = (char*) malloc(filelen);
  }
  if (file == NULL) {
    /* failed to allocate memory to hold halt file name, to be safe,
     * assume we need to halt */
    scr_err("%s: Failed to allocate storage to store halt file name @ %s:%d",
            PROG, __FILE__, __LINE__
    );
    return NEED_HALT;
  }

  /* build the full file name */
  int n = snprintf(file, filelen, "%s/%s", args.dir, NAME);
  if (n >= filelen) {
    /* failed to write halt file name into buffer, to be safe, assume we need to halt */
    scr_err("%s: Flush file name is too long (need %d bytes, %d byte buffer) @ %s:%d",
            PROG, n, filelen, __FILE__, __LINE__
    );
    free(file);
    return NEED_HALT;
  }

  /* if we don't have a halt file, we're ok to continue */
  if (scr_file_exists(file) != SCR_SUCCESS) {
    printf("%s: CONTINUE RUN: No halt file found.\n", PROG);
    return DONT_HALT;
  }

  /* otherwise, assume that we don't need to halt, and check for valid condition */
  int rc = DONT_HALT;

  /* create a new hash to hold the file data */
  scr_hash* scr_halt_hash = scr_hash_new();

  /* read in our halt file */
  if (scr_halt_read(file, scr_halt_hash) != SCR_SUCCESS) {
    /* failed to read the halt file -- to be safe, assume we need to halt */
    printf("%s: HALT RUN: Failed to open existing halt file.\n", PROG);
    rc = NEED_HALT;
    goto cleanup;
  }

  /* TODO: all epochs are stored in ints, should be in unsigned ints? */
  /* get current epoch seconds */
  struct timeval tv;
  gettimeofday(&tv, NULL);
  int now = tv.tv_sec;

  char* value = NULL;

  /* initialize our halt seconds */
  int halt_seconds = 0;

  /* adjust our halt seconds based on what we find in the parameters */
  scr_param_init();
  if ((value = scr_param_get("SCR_HALT_SECONDS")) != NULL) {
    halt_seconds = atoi(value);
  }
  scr_param_finalize();

  /* if halt seconds is set in halt file, use this value instead */
  value = scr_hash_elem_get_first_val(scr_halt_hash, SCR_HALT_KEY_SECONDS);
  if (value != NULL) {
    halt_seconds = atoi(value);
  }

  /* check whether a reason has been specified */
  value = scr_hash_elem_get_first_val(scr_halt_hash, SCR_HALT_KEY_EXIT_REASON);
  if (value != NULL) {
    if (strcmp(value, "") != 0) {
      printf("%s: HALT RUN: Reason: %s.\n", PROG, value);
      rc = NEED_HALT;
    }
  }

  /* check whether we are out of checkpoints */
  value = scr_hash_elem_get_first_val(scr_halt_hash, SCR_HALT_KEY_CHECKPOINTS);
  if (value != NULL) {
    int checkpoints_left = atoi(value);
    if (checkpoints_left == 0) {
      printf("%s: HALT RUN: No checkpoints remaining.\n", PROG);
      rc = NEED_HALT;
    }
  }

  /* check whether we need to exit before a specified time */
  value = scr_hash_elem_get_first_val(scr_halt_hash, SCR_HALT_KEY_EXIT_BEFORE);
  if (value != NULL) {
    int exit_before = atoi(value);
    if (now >= (exit_before - halt_seconds)) {
      time_t time_now  = (time_t) now;
      time_t time_exit = (time_t) exit_before - halt_seconds;
      char str_now[256];
      char str_exit[256];
      strftime(str_now,  sizeof(str_now),  "%c", localtime(&time_now));
      strftime(str_exit, sizeof(str_exit), "%c", localtime(&time_exit));
      printf("%s: HALT RUN: Current time (%s) is past ExitBefore-HaltSeconds time (%s).\n",
             PROG, str_now, str_exit
      );
      rc = NEED_HALT;
    }
  }

  /* check whether we need to exit after a specified time */
  value = scr_hash_elem_get_first_val(scr_halt_hash, SCR_HALT_KEY_EXIT_AFTER);
  if (value != NULL) {
    int exit_after = atoi(value);
    if (now >= exit_after) {
      time_t time_now  = (time_t) now;
      time_t time_exit = (time_t) exit_after;
      char str_now[256];
      char str_exit[256];
      strftime(str_now,  sizeof(str_now),  "%c", localtime(&time_now));
      strftime(str_exit, sizeof(str_exit), "%c", localtime(&time_exit));
      printf("%s: HALT RUN: Current time (%s) is past ExitAfter time (%s).\n", PROG, str_now, str_exit);
      rc = NEED_HALT;
    }
  }

cleanup:
  /* delete the hash holding the halt file data */
  scr_hash_delete(scr_halt_hash);

  /* free off our file name storage */
  if (file != NULL) {
    free(file);
    file = NULL;
  }

  /* return appropriate exit code */
  return rc;
}
