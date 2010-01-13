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

/* given the name of a halt file, read it and fill in hash */
int scr_halt_read(const char* file, struct scr_hash* hash)
{
  /* check whether halt file even exists */
  if (scr_file_exists(file) != SCR_SUCCESS) {
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

  /* read in the hash */
  scr_hash_read_fd(file, fd, hash);

  /* release the file lock */
  if (flock(fd, LOCK_UN) != 0) {
    scr_err("Failed to release file lock on %s: flock(%d, %d) errno=%d %m @ %s:%d",
            file, fd, LOCK_UN, errno, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* close file */
  scr_close(file, fd);

  return SCR_SUCCESS;
}

/* read in halt file (which user may have changed via scr_halt), update internal data structure,
 * optionally decrement the checkpoints_left field, and write out halt file all while locked */
int scr_halt_sync_and_decrement(const char* file, struct scr_hash* hash, int dec_count)
{
  /* set the mode on the file to be readable/writable by all
   * (enables a sysadmin to halt a user's job via scr_halt --all) */
  mode_t old_mode = umask(0000);

  /* record whether file already exists before we open it */
  int exists = (scr_file_exists(file) == SCR_SUCCESS);

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

  /* get a new blank hash to read in file */
  struct scr_hash* file_hash = scr_hash_new();

  /* read in the file data */
  scr_hash_read_fd(file, fd, file_hash);

  /* if the file already existed before we opened it, override our current settings with its values */
  if (exists) {
    /* for the exit reason, only override our current value if the file has a setting but we don't,
     * otherwise the running program could never set this value */
    /* if we have an exit reason set, but the file doesn't, make a copy before we unset out hash */
    char* save_reason = NULL;
    char* reason      = scr_hash_elem_get_first_val(hash,      SCR_HALT_KEY_EXIT_REASON);
    char* file_reason = scr_hash_elem_get_first_val(file_hash, SCR_HALT_KEY_EXIT_REASON);
    if (reason != NULL && file_reason == NULL) {
      save_reason = strdup(reason);
    }

    /* set our hash to match the file */
    scr_hash_unset_all(hash);
    scr_hash_merge(hash, file_hash);

    /* restore our exit reason */
    if (save_reason != NULL) {
      scr_hash_unset(hash, SCR_HALT_KEY_EXIT_REASON);
      scr_hash_set_kv(hash, SCR_HALT_KEY_EXIT_REASON, save_reason);
      free(save_reason);
      save_reason = NULL;
    }
  }

  /* free the file_hash */
  scr_hash_delete(file_hash);

  /* decrement the number of remaining checkpoints */
  char* ckpts_str = scr_hash_elem_get_first_val(hash, SCR_HALT_KEY_CHECKPOINTS);
  if (ckpts_str != NULL) {
    /* get number of checkpoints and decrement by dec_count */
    int ckpts = atoi(ckpts_str);
    ckpts -= dec_count;

    /* write this new value back to the hash */
    scr_hash_unset(hash, SCR_HALT_KEY_CHECKPOINTS);
    scr_hash_setf(hash, NULL, "%s %d", SCR_HALT_KEY_CHECKPOINTS, ckpts);
  }

  /* wind file pointer back to the start of the file */
  lseek(fd, 0, SEEK_SET);

  /* write our updated hash */
  ssize_t bytes_written = scr_hash_write_fd(file, fd, hash);

  /* truncate the file to the correct size (may be smaller than it was before) */
  if (bytes_written >= 0) {
    ftruncate(fd, (off_t) bytes_written);
  }

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
  scr_close(file, fd);

  /* restore the normal file mask */
  umask(old_mode);

  /* write current values to halt file */
  return SCR_SUCCESS;
}
