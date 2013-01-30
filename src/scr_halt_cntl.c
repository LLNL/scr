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

/* This is a utility program that lets one list, set, and unset values
 * in the halt file.  It's a small C program which must run on the
 * same node where rank 0 runs -- it's coordinates access to the halt
 * file with rank 0 via flock(), which does not work across NFS.
 *
 * One will typically call some other script, which in turn identifies
 * the rank 0 node and issues a remote shell command to run this utility. */

#include "scr.h"
#include "scr_io.h"
#include "scr_path.h"
#include "scr_err.h"
#include "scr_util.h"
#include "scr_halt.h"
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

struct arglist {
  char* file;
  int list;
  int set_checkpoints;
  int set_before;
  int set_after;
  int set_seconds;
  int set_reason;
  int unset_checkpoints;
  int unset_before;
  int unset_after;
  int unset_seconds;
  int unset_reason;
  int value_checkpoints;
  int value_before;
  int value_after;
  int value_seconds;
  char* value_reason;
};

int processArgs(int argc, char **argv, struct arglist* args)
{
  int i, j;
  char *argptr;
  char flag;

  /* set to default values */
  args->file              = NULL;
  args->list              = 0;
  args->set_checkpoints   = 0;
  args->set_before        = 0;
  args->set_after         = 0;
  args->set_seconds       = 0;
  args->set_reason        = 0;
  args->unset_checkpoints = 0;
  args->unset_before      = 0;
  args->unset_after       = 0;
  args->unset_seconds     = 0;
  args->unset_reason      = 0;
  args->value_checkpoints = -1;
  args->value_before      = -1;
  args->value_after       = -1;
  args->value_seconds     = -1;
  args->value_reason      = NULL;

  for (i=1; i<argc; i++) {
    /* check for options */
    j = 0;
    int unset = 0;
    if (argv[i][j++] == '-') {
      /* flag is the first char following the '-' */
      flag = argv[i][j++];
      if (flag == 'x') {
        unset = 1;
        flag = argv[i][j++];
      }
      argptr = NULL;

      if (strchr("l", flag)) {
        switch(flag) {
        case 'l':
          args->list = 1;
          break;
        }
        continue;
      }

      /* handles "-i#" or "-i #" */
      if (argv[i][j] != 0 || unset) {
        argptr = &(argv[i][j]);
      } else {
        argptr = argv[i+1];
        i++;
      }

      /* single argument parameters */
      if (strchr("fcbasr", flag)) {
        switch(flag) {
        case 'f':
          args->file = strdup(argptr);
          break;
        case 'c':
          if (unset) {
            args->unset_checkpoints = 1;
          } else {
            args->set_checkpoints = 1;
            args->value_checkpoints = atoi(argptr);
          }
          break;
        case 'b':
          if (unset) {
            args->unset_before = 1;
          } else {
            args->set_before = 1;
            args->value_before = atoi(argptr);
          }
          break;
        case 'a':
          if (unset) {
            args->unset_after = 1;
          } else {
            args->set_after = 1;
            args->value_after = atoi(argptr);
          }
          break;
        case 's':
          if (unset) {
            args->unset_seconds = 1;
          } else {
            args->set_seconds = 1;
            args->value_seconds = atoi(argptr);
          }
          break;
        case 'r':
          if (unset) {
            args->unset_reason = 1;
          } else {
            args->set_reason = 1;
            args->value_reason = strdup(argptr);
            /* TODO: remove all whitespace from reason */
          }
          break;
        }
        continue;
      } else {
        scr_err("Invalid flag -%c", flag);
        return 0;
      }
    } else {
      scr_err("Unknown argument %s", argv[i]);
      return 0;
    }
  }

  /* check that we got a filename */
  if (args->file == NULL) {
    scr_err("Must specify full path to haltfile via '-f <haltfile>'");
    return 0;
  }

  return 1;
}

/* read in halt file (which program may have changed), update internal data structure,
 * set & unset any fields, and write out halt file all while locked */
int scr_halt_sync_and_set(const scr_path* file_path, struct arglist* args, scr_hash* data)
{
  /* convert path to string */
  char* file = scr_path_strdup(file_path);

  /* TODO: sleep and try the open several times if the first fails */
  /* open the halt file for reading */
  mode_t mode_file = scr_getmode(1, 1, 0);
  int fd = scr_open(file, O_RDWR | O_CREAT, mode_file);
  if (fd < 0) {
    scr_err("Opening file for write: scr_open(%s) errno=%d %s @ %s:%d",
      file, errno, strerror(errno), __FILE__, __LINE__
    );
    /* restore the normal file mask */
    scr_free(&file);
    return SCR_FAILURE;
  }

  /* acquire an exclusive file lock before reading */
  int ret = scr_file_lock_write(file,fd);
  if (ret != SCR_SUCCESS) {
    scr_close(file,fd);
    scr_free(&file);
    return ret;
  }

  /* read in the current data from the file */
  scr_hash_read_fd(file, fd, data);

  /* set / unset values in file */
  if (args->set_reason) {
    scr_hash_unset(data, SCR_HALT_KEY_EXIT_REASON);
    scr_hash_set_kv(data, SCR_HALT_KEY_EXIT_REASON, args->value_reason);
  } else if (args->unset_reason) {
    scr_hash_unset(data, SCR_HALT_KEY_EXIT_REASON);
  }

  if (args->set_checkpoints) {
    scr_hash_unset(data, SCR_HALT_KEY_CHECKPOINTS);
    scr_hash_setf(data, NULL, "%s %lu", SCR_HALT_KEY_CHECKPOINTS, args->value_checkpoints);
  } else if (args->unset_checkpoints) {
    scr_hash_unset(data, SCR_HALT_KEY_CHECKPOINTS);
  }

  if (args->set_before) {
    scr_hash_unset(data, SCR_HALT_KEY_EXIT_BEFORE);
    scr_hash_setf(data, NULL, "%s %lu", SCR_HALT_KEY_EXIT_BEFORE, args->value_before);
  } else if (args->unset_before) {
    scr_hash_unset(data, SCR_HALT_KEY_EXIT_BEFORE);
  }

  if (args->set_after) {
    scr_hash_unset(data, SCR_HALT_KEY_EXIT_AFTER);
    scr_hash_setf(data, NULL, "%s %lu", SCR_HALT_KEY_EXIT_AFTER, args->value_after);
  } else if (args->unset_after) {
    scr_hash_unset(data, SCR_HALT_KEY_EXIT_AFTER);
  }

  if (args->set_seconds) {
    scr_hash_unset(data, SCR_HALT_KEY_SECONDS);
    scr_hash_setf(data, NULL, "%s %lu", SCR_HALT_KEY_SECONDS, args->value_seconds);
  } else if (args->unset_seconds) {
    scr_hash_unset(data, SCR_HALT_KEY_SECONDS);
  }

  /* wind file pointer back to the start of the file */
  lseek(fd, 0, SEEK_SET);

  /* write our updated data */
  ssize_t bytes_written = scr_hash_write_fd(file, fd, data);

  /* truncate the file to the correct size (may be smaller than it was before) */
  if (bytes_written >= 0) {
    ftruncate(fd, (off_t) bytes_written);
  }

  /* release the file lock */
  ret = scr_file_unlock(file, fd);
  if (ret != SCR_SUCCESS) {
    scr_close(file,fd);
    scr_free(&file);
    return ret;
  }

  /* close file */
  scr_close(file, fd);

  /* free file name string */
  scr_free(&file);

  /* write current values to halt file */
  return SCR_SUCCESS;
}
      
int main (int argc, char *argv[])
{
  /* process command line arguments */
  struct arglist args;
  if (! processArgs(argc, argv, &args)) {
    return 1;
  }

  /* create a new hash to hold the file data */
  scr_hash* data = scr_hash_new();

  /* create path to halt file */
  scr_path* halt_file = scr_path_from_str(args.file);

  if (args.list) {
    /* if the user wants to list the values, just read the file, print the values, and exit */
    scr_halt_read(halt_file, data);
  } else {
    /* otherwise, we must be setting something */
    if (args.set_checkpoints) {
      printf("Setting CheckpointsLeft\n");
    } else if (args.unset_checkpoints) {
      printf("Unsetting CheckpointsLeft\n");
    }

    if (args.set_after) {
      printf("Setting ExitAfter\n");
    } else if (args.unset_after) {
      printf("Unsetting ExitAfter\n");
    }

    if (args.set_before) {
      printf("Setting ExitBefore\n");
    } else if (args.unset_before) {
      printf("Unsetting ExitBefore\n");
    }

    if (args.set_seconds) {
      printf("Setting HaltSeconds\n");
    } else if (args.unset_seconds) {
      printf("Unsetting HaltSeconds\n");
    }

    if (args.set_reason) {
      printf("Setting ExitReason\n");
    } else if (args.unset_reason) {
      printf("Unsetting ExitReason\n");
    }

    printf("\n");

    scr_halt_sync_and_set(halt_file, &args, data);
  }

  /* print the current settings */
  time_t secs;
  char* value = NULL;
  printf("Halt file settings for %s:\n", args.file);
  int have_one = 0;

  if (scr_hash_util_get_str(data, SCR_HALT_KEY_EXIT_REASON, &value) == SCR_SUCCESS) {
    printf("  ExitReason:      %s\n", value);
    have_one = 1;
  }

  int checkpoints_left;
  if (scr_hash_util_get_int(data, SCR_HALT_KEY_CHECKPOINTS, &checkpoints_left) == SCR_SUCCESS) {
    printf("  CheckpointsLeft: %d\n", checkpoints_left);
    have_one = 1;
  }

  int exit_after = -1;
  if (scr_hash_util_get_int(data, SCR_HALT_KEY_EXIT_AFTER, &exit_after) == SCR_SUCCESS) {
    secs = (time_t) exit_after;
    printf("  ExitAfter:       %s", asctime(localtime(&secs)));
    have_one = 1;
  }

  int exit_before = -1;
  if (scr_hash_util_get_int(data, SCR_HALT_KEY_EXIT_BEFORE, &exit_before) == SCR_SUCCESS) {
    secs = (time_t) exit_before;
    printf("  ExitBefore:      %s", asctime(localtime(&secs)));
    have_one = 1;
  }

  int halt_seconds = -1;
  if (scr_hash_util_get_int(data, SCR_HALT_KEY_SECONDS, &halt_seconds) == SCR_SUCCESS) {
    printf("  HaltSeconds:     %d\n", halt_seconds);
    have_one = 1;
  }

  if (halt_seconds != -1 && exit_before != -1) {
    secs = (time_t) exit_before - halt_seconds;
    printf("  ExitBefore - HaltSeconds: %s", asctime(localtime(&secs)));
    have_one = 1;
  }

  if (!have_one) {
    printf("  None\n");
  }

  /* delete path to halt file */
  scr_path_delete(&halt_file);

  /* delete the hash holding the file data */
  scr_hash_delete(&data);

  return 0;
}
