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

/* Implements an interface to read and write a halt file. */

#include "scr.h"
#include "scr_io.h"
#include "scr_err.h"
#include "scr_halt.h"

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/file.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>

/* blank out a halt data structure */
int scr_halt_init(struct scr_haltdata* data)
{
  strcpy(data->exit_reason, "");
  data->checkpoints_left = -1;
  data->exit_before      = -1;
  data->exit_after       = -1;
  data->halt_seconds     = -1;
  return SCR_SUCCESS;
}

/* given an opened file descriptor, read the fields for a halt file and fill in data */
int scr_halt_read_fd(int fd, struct scr_haltdata* data)
{
  /* read field / value pairs from file */
  char line[SCR_MAX_FILENAME];
  char field[SCR_MAX_FILENAME];
  char value[SCR_MAX_FILENAME];
  int n;
  do {
    /* read in a line and break on: eof, newline, or comma */
    char* c = line;
    while (1) {
      n = scr_read(fd, c, 1); 
      if (n == 0 || *c == '\n' || *c == ',') {
        break;
      }
      c++;
    }

    /* terminate the line with a NULL */
    *c = '\0';

    /* if we got a line, check for a setting */
    if (strlen(line) > 0) {
      sscanf(line, "%s %s\n", field, value);
      if (strcmp(field, "ExitReason:") == 0)      { strcpy(data->exit_reason, value); }
      if (strcmp(field, "CheckpointsLeft:") == 0) { data->checkpoints_left = atoi(value); }
      if (strcmp(field, "ExitBefore:")  == 0)     { data->exit_before  = atoi(value); }
      if (strcmp(field, "ExitAfter:")   == 0)     { data->exit_after   = atoi(value); }
      if (strcmp(field, "HaltSeconds:") == 0)     { data->halt_seconds = atoi(value); }
    }
  } while (n != 0);

  return SCR_SUCCESS;
}

/* given an opened file descriptor, write halt fields to it, sync, and truncate */
int scr_halt_write_fd(int fd, struct scr_haltdata* data)
{
  unsigned int size = 0;
  char buf[SCR_MAX_FILENAME];

  /* write field / value pairs to file if set */
  if (strcmp(data->exit_reason, "") != 0) {
    sprintf(buf, "ExitReason: %s\n", data->exit_reason);
    scr_write(fd, buf, strlen(buf));
    size += strlen(buf);
  }
  if (data->checkpoints_left != -1) {
    sprintf(buf, "CheckpointsLeft: %d\n", data->checkpoints_left);
    scr_write(fd, buf, strlen(buf));
    size += strlen(buf);
  }
  if (data->exit_before != -1) {
    sprintf(buf, "ExitBefore: %d\n", data->exit_before);
    scr_write(fd, buf, strlen(buf));
    size += strlen(buf);
  }
  if (data->exit_after  != -1) {
    sprintf(buf, "ExitAfter: %d\n", data->exit_after);
    scr_write(fd, buf, strlen(buf));
    size += strlen(buf);
  }
  if (data->halt_seconds != -1) {
    sprintf(buf, "HaltSeconds: %d\n", data->halt_seconds);
    scr_write(fd, buf, strlen(buf));
    size += strlen(buf);
  }

  /* now truncate the file (the size may be smaller than it was before) */
  ftruncate(fd, size);

  /* force our changes to disc */
  fsync(fd);

  return SCR_SUCCESS;
}

/* returns SCR_SUCCESS if halt file exists */
int scr_halt_exists(const char* file)
{
  /* check whether halt file exists */
  if (access(file, R_OK) < 0) {
    return SCR_FAILURE;
  }
  return SCR_SUCCESS;
}

/* given the name of a halt file, read it and fill in data */
int scr_halt_read(const char* file, struct scr_haltdata* data)
{
  /* blank out the data structure */
  scr_halt_init(data);

  /* check whether halt file even exists */
  if (scr_halt_exists(file) != SCR_SUCCESS) {
    return SCR_FAILURE;
  }

  /* TODO: sleep and try the open several times if the first fails */
  /* open the halt file for reading */
  int fd = scr_open(file, O_RDONLY);
  if (fd < 0) {
    scr_err("Opening file for read: scr_open(%s) errno=%d %m @ %s:%d",
            file, errno, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* acquire a file lock before reading */
  /* since the file is opened for reading, use a shared lock
   * (AIX flock man page seems to imply this requirement) */
  if (flock(fd, LOCK_SH) != 0) {
    scr_err("Failed to acquire file lock on %s: flock(%d, %d) errno=%d %m @ %s:%d",
            file, fd, LOCK_EX, errno, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* read in the data */
  scr_halt_read_fd(fd, data);

  /* release the file lock */
  if (flock(fd, LOCK_UN) != 0) {
    scr_err("Failed to release file lock on %s: flock(%d, %d) errno=%d %m @ %s:%d",
            file, fd, LOCK_UN, errno, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* close file */
  scr_close(fd);

  return SCR_SUCCESS;
}

/* read in halt file (which user may have changed via scr_halt), update internal data structure,
 * optionally decrement the checkpoints_left field, and write out halt file all while locked */
int scr_halt_sync_and_decrement(const char* file, struct scr_haltdata* data, int dec_count)
{
  /* blank out the data structure */
  struct scr_haltdata data_file;
  scr_halt_init(&data_file);

  /* set the mode on the file to be readable/writable by all
   * (enables a sysadmin to halt a user's job via scr_halt --all) */
  mode_t old_mode = umask(0000);

  /* record whether file already exists before we open it */
  int exists = (scr_halt_exists(file) == SCR_SUCCESS);

  /* TODO: sleep and try the open several times if the first fails */
  /* open the halt file for reading */
  int fd = scr_open(file, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH);
  if (fd < 0) {
    scr_err("Opening file for write: scr_open(%s) errno=%d %m @ %s:%d",
            file, errno, __FILE__, __LINE__
    );
    /* restore the normal file mask */
    umask(old_mode);
    return SCR_FAILURE;
  }

  /* acquire an exclusive file lock before read/modify/write */
  if (flock(fd, LOCK_EX) != 0) {
    scr_err("Failed to acquire file lock on %s: flock(%d, %d) errno=%d %m @ %s:%d",
            file, fd, LOCK_EX, errno, __FILE__, __LINE__
    );
    /* restore the normal file mask */
    umask(old_mode);
    return SCR_FAILURE;
  }

  /* read in the file data */
  scr_halt_read_fd(fd, &data_file);

  /* if the file already existed before we opened it, override our current settings with its values */
  if (exists) {
    /* for the exit reason, only override if we don't have a setting but the file does,
     * otherwise the running program could never set this value */
    if (strcmp(data->exit_reason, "") == 0 && strcmp(data_file.exit_reason, "") != 0) {
      strcpy(data->exit_reason, data_file.exit_reason);
    }
    data->checkpoints_left = data_file.checkpoints_left;
    data->exit_before      = data_file.exit_before;
    data->exit_after       = data_file.exit_after;
    data->halt_seconds     = data_file.halt_seconds;
  }

  /* decrement the remaining checkpoint count */
  if (data->checkpoints_left > 0) {
    data->checkpoints_left -= dec_count;
  }

  /* seek back to the start of the file and write our updated data */
  lseek(fd, 0, SEEK_SET);
  scr_halt_write_fd(fd, data);

  /* release the file lock */
  if (flock(fd, LOCK_UN) != 0) {
    scr_err("Failed to release file lock on %s: flock(%d, %d) errno=%d %m @ %s:%d",
            file, fd, LOCK_UN, errno, __FILE__, __LINE__
    );
    /* restore the normal file mask */
    umask(old_mode);
    return SCR_FAILURE;
  }

  /* close file */
  scr_close(fd);

  /* restore the normal file mask */
  umask(old_mode);

  /* write current values to halt file */
  return SCR_SUCCESS;
}
