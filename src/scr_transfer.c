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

/*
 * The scr_transfer program is a deamon process that SCR launches as
 * one process per compute node.  It sleeps in the background, waking
 * periodically to read the transfer.scrinfo file from cache, which
 * the library will fill with info regarding asynchronous flushes.
 * When it detects a START flag in this file, it slowly copies files
 * from cache to the parallel file system.
*/

#include "scr_conf.h"
#include "scr.h"
#include "scr_io.h"
#include "scr_err.h"
#include "scr_util.h"
#include "scr_hash.h"
#include "scr_hash_util.h"
#include "scr_param.h"

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

/* TODO: use direct I/O for improved performance */
/* TODO: compute crc32 during transfer */

#define STOPPED (1)
#define RUNNING (2)

static char*  scr_transfer_file = NULL;
static int    keep_running      = 1;
static int    state             = STOPPED;
static double bytes_per_second  = 0.0;
static double percent_runtime   = 0.0;

static size_t scr_file_buf_size = SCR_FILE_BUF_SIZE;
static double scr_transfer_secs = SCR_TRANSFER_SECS;

int close_files(char* src, int* fd_src, char* dst, int* fd_dst)
{
  /* close the source file, if it's open */
  if (*fd_src >= 0) {
    scr_close(src, *fd_src);
    *fd_src = -1;
  }

  /* close the destination file, if it's open */
  if (*fd_dst >= 0) {
    scr_close(dst, *fd_dst);
    *fd_dst = -1;
  }

  return SCR_SUCCESS;
}

/* free strings pointed to by src and dst (if any) and set them to NULL,
 * set position to 0 */
int clear_parameters(char** src, char** dst, off_t* position)
{
  /* free the string src points to (if any) and set the pointer to
   * NULL */
  if (src != NULL) {
    /* free the string */
    if (*src != NULL) {
      free(*src);
    }

    /* set string to NULL to indicate there is no file */
    *src = NULL;
  }

  /* free the string dst points to (if any) and set the pointer to
   * NULL */
  if (dst != NULL) {
    /* free the string */
    if (*dst != NULL) {
      free(*dst);
    }

    /* set string to NULL to indicate there is no file */
    *dst = NULL;
  }

  /* set position to 0 */
  if (position != NULL) {
    *position = 0;
  }

  return SCR_SUCCESS;
}

/* given a hash of files and a file name, check whether the named
 * file needs data transfered, if so, strdup its destination name
 * and set its position and filesize */
int need_transfer(scr_hash* files, char* src, char** dst, off_t* position, off_t* filesize)
{
  /* check that we got a hash of files and a file name */
  if (files == NULL || src == NULL) {
    return SCR_FAILURE;
  }

  /* lookup the specified file in the hash */
  scr_hash* file_hash = scr_hash_get(files, src);
  if (file_hash == NULL) {
    return SCR_FAILURE;
  }

  /* extract the values for file size, bytes written, and destination */
  unsigned long size, written;
  char* dest;
  if (scr_hash_util_get_bytecount(file_hash, SCR_TRANSFER_KEY_SIZE, &size) == SCR_SUCCESS &&
      scr_hash_util_get_bytecount(file_hash, SCR_TRANSFER_KEY_WRITTEN, &written) == SCR_SUCCESS &&
      scr_hash_util_get_str(file_hash, SCR_TRANSFER_KEY_DESTINATION, &dest) == SCR_SUCCESS)
  {
    /* if the bytes written value is less than the file size,
     * we've got a valid file */
    if (written < size) {
      /* got our file, fill in output parameters */
      *dst = strdup(dest);
      /* TODO: check for error */
      *position = (off_t) written;
      *filesize = (off_t) size;
      return SCR_SUCCESS;
    }
  }

  return SCR_FAILURE;
}

/* given a hash of transfer file data, look for a file which needs to
 * be transfered. If src file is set, try to continue with that file,
 * otherwise, pick the first available file */
int find_file(scr_hash* hash, char** src, char** dst, off_t* position, off_t* filesize)
{
  int found_a_file = 0;

  scr_hash* files = scr_hash_get(hash, SCR_TRANSFER_KEY_FILES);
  if (files != NULL) {
    /* if we're given a file name, try to continue with that file */
    if (!found_a_file && src != NULL && *src != NULL) {
      /* src was set, so assume dst is also set, create a dummy dst
       * variable to hold the string which may be strdup'd in
       * need_transfer call */
      char* tmp_dst = NULL;
      if (need_transfer(files, *src, &tmp_dst, position, filesize) == SCR_SUCCESS) {
        /* can continue with the same file (position may have been
         * updated though) */
        found_a_file = 1;

        /* free the dummy */
        /* TODO: note that if destination has been updated, we're
         * ignoring that change */
        free(tmp_dst);
      } else {
        /* otherwise, this file no longer needs transfered,
         * so free the strings */
        clear_parameters(src, dst, position);
      }
    }

    /* if we still don't have a file, scan the hash and use the first
     * file we find */
    if (!found_a_file) {
      scr_hash_elem* elem;
      for (elem = scr_hash_elem_first(files);
           elem != NULL;
           elem = scr_hash_elem_next(elem))
      {
        /* get the filename */
        char* name = scr_hash_elem_key(elem);

        /* check whether this file needs transfered */
        if (name != NULL &&
            need_transfer(files, name, dst, position, filesize) == SCR_SUCCESS)
        {
          /* found a file, copy its name (the destination and postion
           * are set in need_transfer) */
          *src = strdup(name);
          found_a_file = 1;
          break;
        }
      }
    }
  }

  /* if we didn't find a file, set src and dst to NULL and set
   * position to 0 */
  if (!found_a_file) {
    clear_parameters(src, dst, position);
    return SCR_FAILURE;
  }

  return SCR_SUCCESS;
}

/* writes the specified command to the transfer file */
int set_transfer_file_state(char* s, int done)
{
  /* get a hash to store file data */
  scr_hash* hash = scr_hash_new();

  /* attempt to read the file transfer file */
  int fd = -1;
  if (scr_hash_lock_open_read(scr_transfer_file, &fd, hash) == SCR_SUCCESS) {
    /* set the state */
    scr_hash_util_set_str(hash, SCR_TRANSFER_KEY_STATE, s);

    /* set the flag if we're done */
    if (done) {
      scr_hash_set_kv(hash, SCR_TRANSFER_KEY_FLAG, SCR_TRANSFER_KEY_FLAG_DONE);
    }

    /* write the hash back to the file */
    scr_hash_write_close_unlock(scr_transfer_file, &fd, hash);
  }

  /* delete the hash */
  scr_hash_delete(&hash);

  return SCR_SUCCESS;
}

/* read the transfer file and set our global variables to match */
scr_hash* read_transfer_file()
{
  char* value = NULL;

  /* get a new hash to store the file data */
  scr_hash* hash = scr_hash_new();

  /* open transfer file with lock */
  scr_hash_read_with_lock(scr_transfer_file, hash);

  /* read in our allowed bandwidth value */
  value = scr_hash_elem_get_first_val(hash, SCR_TRANSFER_KEY_BW);
  if (value != NULL) {
    double bw;
    if (scr_atod(value, &bw) == SCR_SUCCESS) {
      /* got a new bandwidth value, set our global variable */
      bytes_per_second = bw;
    } else {
      /* could not interpret bandwidth value */
      scr_err("scr_transfer: Ignoring invalid BW value in %s @ %s:%d",
              scr_transfer_file, __FILE__, __LINE__
      );
    }
  } else {
    /* couldn't find a BW field, so disable this limit */
    bytes_per_second = 0.0;
  }

  /* read in our allowed percentage of runtime value */
  value = scr_hash_elem_get_first_val(hash, SCR_TRANSFER_KEY_PERCENT);
  if (value != NULL) {
    double percent;
    if (scr_atod(value, &percent) == SCR_SUCCESS) {
      /* got a new bandwidth value, set our global variable */
      percent_runtime = percent / 100.0;
    } else {
      /* could not interpret bandwidth value */
      scr_err("scr_transfer: Ignoring invalid PERCENT value in %s @ %s:%d",
              scr_transfer_file, __FILE__, __LINE__
      );
    }
  } else {
    /* couldn't find a PERCENT field, so disable this limit */
    percent_runtime = 0.0;
  }

  /* check for DONE flag */
  int done = 0;
  scr_hash* done_hash = scr_hash_get_kv(hash, SCR_TRANSFER_KEY_FLAG, SCR_TRANSFER_KEY_FLAG_DONE);
  if (done_hash != NULL) {
    done = 1;
  }

  /* check for latest command */
  state = STOPPED;
  value = scr_hash_elem_get_first_val(hash, SCR_TRANSFER_KEY_COMMAND);
  if (value != NULL) {
    if (strcmp(value, SCR_TRANSFER_KEY_COMMAND_EXIT) == 0) {
      /* close files and exit */
      keep_running = 0;
    } else if (strcmp(value, SCR_TRANSFER_KEY_COMMAND_STOP) == 0) {
      /* just stop, nothing else to do here */
    } else if (strcmp(value, SCR_TRANSFER_KEY_COMMAND_RUN) == 0) {
      /* found the RUN command, if the DONE flag is not set,
       * set our state to running and update the transfer file */
      if (!done) {
        state = RUNNING;
        set_transfer_file_state(SCR_TRANSFER_KEY_STATE_RUN, 0);
      }
    } else {
      scr_err("scr_transfer: Unknown command %s in %s @ %s:%d",
              value, scr_transfer_file, __FILE__, __LINE__
      );
    }
  }

  /* ensure that our current state is always recorded in the file
   * (the file may have been deleted since we last wrote our state
   * to it) */
  value = scr_hash_elem_get_first_val(hash, SCR_TRANSFER_KEY_STATE);
  if (value == NULL) {
    if (state == STOPPED) {
      set_transfer_file_state(SCR_TRANSFER_KEY_STATE_STOP, 0);
    } else if (state == RUNNING) {
      set_transfer_file_state(SCR_TRANSFER_KEY_STATE_RUN, 0);
    } else {
      scr_err("scr_transfer: Unknown state %d @ %s:%d",
              state, __FILE__, __LINE__
      );
    }
  }

  return hash;
}

/* given src and dst, find the matching entry in the transfer file and
 * update the WRITTEN field to the given position in that file */
int update_transfer_file(char* src, char* dst, off_t position)
{
  /* create a hash to store data from file */
  scr_hash* hash = scr_hash_new();

  /* attempt to open transfer file with lock,
   * if we fail to open the file, don't bother writing to it */
  int fd = -1;
  if (scr_hash_lock_open_read(scr_transfer_file, &fd, hash) == SCR_SUCCESS) {
    /* search for the source file, and update the bytes written if found */
    if (src != NULL) {
      scr_hash* file_hash = scr_hash_get_kv(hash, SCR_TRANSFER_KEY_FILES, src);
      if (file_hash != NULL) {
        /* update the bytes written field */
        scr_hash_util_set_bytecount(file_hash, "WRITTEN", (uint64_t) position);
      }
    }

    /* close the transfer file and release the lock */
    scr_hash_write_close_unlock(scr_transfer_file, &fd, hash);
  }

  /* free the hash */
  scr_hash_delete(&hash);

  return SCR_SUCCESS;
}

/* returns 1 if file1 and file2 are different
 * (NULL pointers are allowed) */
int bool_diff_files(char* file1, char* file2)
{
  /* if file2 is set but file1 is not, they're different */
  if (file1 != NULL && file2 == NULL) {
    return 1;
  }

  /* if file1 is set but file2 is not, they're different */
  if (file1 == NULL && file2 != NULL) {
    return 1;
  }

  /* if file1 and file2 and both set,
   * check whether the strings are the same */
  if (file1 != NULL && file2 != NULL && strcmp(file1, file2) != 0) {
    return 1;
  }

  /* otherwise, consider file1 to be the same as file2 */
  return 0;
}

int read_params()
{
  /* read our parameters */
  char* value = NULL;
  scr_param_init();

  /* set file copy buffer size (file chunk size) */
  if ((value = scr_param_get("SCR_FILE_BUF_SIZE")) != NULL) {
    scr_file_buf_size = atoi(value);
  }

  /* set number of secs to sleep between reading file */
  if ((value = scr_param_get("SCR_TRANSFER_SECS")) != NULL) {
    float secs = 0.0;
    sscanf(value, "%f", &secs);
    scr_transfer_secs = (double) secs;
  }

  scr_param_finalize();

  return SCR_SUCCESS;
}

int main (int argc, char *argv[])
{
  /* check that we were given at least one argument
   * (the transfer file name) */
  if (argc != 2) {
    printf("Usage: scr_transfer <transferfile>\n");
    return 1;
  }

  /* record the name of the transfer file */
  scr_transfer_file = strdup(argv[1]);
  if (scr_transfer_file == NULL) {
    scr_err("scr_transfer: Copying transfer file name @ %s:%d",
            __FILE__, __LINE__
    );
    return 1;
  }

  /* initialize our tracking variables */
  read_params();

  /* get file io mode */
  mode_t mode_file = scr_getmode(1, 1, 0);

  /* we cache the opened file descriptors to avoid extra opens,
   * seeks, and closes */
  int fd_src = -1;
  int fd_dst = -1;

  char* new_file_src = NULL;
  char* old_file_src = NULL;
  char* new_file_dst = NULL;
  char* old_file_dst = NULL;

  off_t new_position = 0;
  off_t old_position = 0;

  /* start in the stopped state */
  state = STOPPED;
  set_transfer_file_state(SCR_TRANSFER_KEY_STATE_STOP, 0);

  /* TODO: enable this value to be set from config file */
  /* TODO: page-align this buffer for faster performance */
  /* allocate our file copy buffer */
  size_t bufsize = scr_file_buf_size;
  char* buf = malloc(bufsize);
  if (buf == NULL) {
    scr_err("scr_transfer: Failed to allocate %llu bytes for file copy buffer @ %s:%d",
            (unsigned long long) bufsize, __FILE__, __LINE__
    );
    return 1;
  }

  int nread = 0;
  double secs_run   = 0.0;
  double secs_slept = 0.0;
  double secs_run_start  = scr_seconds();
  double secs_run_end    = secs_run_start;
  double secs_last_write = secs_run_start;
  scr_hash* hash = scr_hash_new();
  while (keep_running) {
    /* loop here sleeping and checking transfer file periodically
     * until state changes and / or some time elapses */
    /* reset our timer for our last write */
    double secs_remain = scr_transfer_secs;
    while (keep_running && (state == STOPPED || secs_remain > 0.0)) {
      /* remember our current state before reading transfer file */
      int old_state = state;

      /* read the transfer file, which fills in our hash and
       * also updates state and bytes_per_second */
      scr_hash_delete(&hash);
      hash = read_transfer_file();

      /* compute time we should sleep before writing more data based
       * on bandwidth and percent of runtime limits */
      if (state == RUNNING) {
        /* get the current time */
        double secs_now = scr_seconds();

        /* based on the amount we last wrote and our allocated bandwidth,
         * compute time we need to sleep before attempting our next write */
        double secs_remain_bw = 0.0;
        if (nread > 0 && bytes_per_second > 0.0) {
          double secs_to_wait_bw = (double) nread / bytes_per_second;
          double secs_waited_bw = secs_now - secs_last_write;
          secs_remain_bw = secs_to_wait_bw - secs_waited_bw;
        }

        /* based on the percentage of time we are allowed to be running,
         * compute time we need to sleep before attempting our next write */
        double secs_remain_runtime = 0.0;
        if (percent_runtime > 0.0) {
          /* stop the run clock, add to the run time,
           * and restart the run clock */
          secs_run_end = secs_now;
          secs_run += secs_run_end - secs_run_start;
          secs_run_start = secs_run_end;

          /* compute our total time, and the time we need to sleep */
          double secs_total = secs_run + secs_slept;
          secs_remain_runtime = secs_run / percent_runtime - secs_total;
        }

        /* take the maximum of these two values */
        secs_remain = secs_remain_bw;
        if (secs_remain_runtime > secs_remain) {
          secs_remain = secs_remain_runtime;
        }
      }

      /* check for a state transition */
      if (state != old_state) {
        if (state == RUNNING) {
          /* if we switched to RUNNING, kick out without sleeping and
           * reset the total run and sleep times */
          secs_remain = 0.0;
          secs_run    = 0.0;
          secs_slept  = 0.0;
        } else if (state == STOPPED) {
          /* if we switched to STOPPED, close our files if open */
          close_files(new_file_src, &fd_src, new_file_dst, &fd_dst);
          clear_parameters(&new_file_src, &new_file_dst, &new_position);
          clear_parameters(&old_file_src, &old_file_dst, &old_position);

          /* after closing our files, update our state in the transfer file */
          set_transfer_file_state(SCR_TRANSFER_KEY_STATE_STOP, 0);
        }
      }

      /* assume we can sleep for the full remainder of the time */
      double secs = secs_remain;

      /* if we're not running, always sleep for the full time */
      if (state != RUNNING) {
        secs = scr_transfer_secs;
      }

      /* set a maximum time to sleep before we read the hash file again
       * (ensures some responsiveness) */
      if (secs > scr_transfer_secs) {
        secs = scr_transfer_secs;
      }

      /* sleep if we need to */
      if (secs > 0.0) {
        /* stop the run clock and add to the total run time */
        secs_run_end = scr_seconds();
        secs_run += secs_run_end - secs_run_start;

        /* sleep */
        usleep((unsigned long) (secs * 1000000.0));
        secs_slept += secs;
        secs_remain -= secs;

        /* restart the run clock */
        secs_run_start = scr_seconds();
      }
    }

    /* write data out */
    if (state == RUNNING) {
      /* look for a new file to transfer */
      off_t filesize = 0;
      find_file(hash, &new_file_src, &new_file_dst, &new_position, &filesize);

      /* if we got a new file, close the old one (if open),
       * open the new file */
      if (bool_diff_files(new_file_src, old_file_src)) {
        /* close the old descriptor if it's open */
        if (fd_src >= 0) {
          scr_close(old_file_src, fd_src);
          fd_src = -1;
        }

        /* delete the old file name if we have one */
        if (old_file_src != NULL) {
          free(old_file_src);
          old_file_src = NULL;
        }

        /* reset our position counter */
        old_position = 0;

        /* open the file and remember the filename if we have one */
        if (new_file_src != NULL) {
          fd_src = scr_open(new_file_src, O_RDONLY);
          /* TODO: check for errors here */
          old_file_src = strdup(new_file_src);
          /* TODO: check for errors here */
        }
      }

      /* if we got a new file, close the old one (if open),
       * open the new file */
      if (bool_diff_files(new_file_dst, old_file_dst)) {
        /* close the old descriptor if it's open */
        if (fd_dst >= 0) {
          scr_close(old_file_dst, fd_dst);
          fd_dst = -1;
        }

        /* delete the old file name if we have one */
        if (old_file_dst != NULL) {
          free(old_file_dst);
          old_file_dst = NULL;
        }

        /* reset our position counter */
        old_position = 0;

        /* open the file and remember the filename if we have one */
        if (new_file_dst != NULL) {
          fd_dst = scr_open(new_file_dst, O_RDWR | O_CREAT, mode_file);
          /* TODO: check for errors here */
          old_file_dst = strdup(new_file_dst);
          /* TODO: check for errors here */
        }
      }

      /* we may have the same file, but perhaps the position changed
       * (may need to seek) */
      if (new_position != old_position) {
        if (fd_src >= 0) {
          lseek(fd_src, new_position, SEEK_SET);
          /* TODO: check for errors here */
        }

        if (fd_dst >= 0) {
          lseek(fd_dst, new_position, SEEK_SET);
          /* TODO: check for errors here */
        }

        /* remember the new position */
        old_position = new_position;
      }

      /* if we have two open files,
       * copy a chunk from source file to destination file */
      nread = 0;
      if (fd_src >= 0 && fd_dst >= 0) {
        /* compute number of bytes to read from file */
        size_t count = (size_t) (filesize - new_position);
        if (count > bufsize) {
          count = bufsize;
        }

        /* read a chunk */
        nread = scr_read(new_file_src, fd_src, buf, count);

        /* if we read data, write it out */
        if (nread > 0) {
          /* record the time of our write */
          secs_last_write = scr_seconds();

          /* write the chunk and force it out with an fsync */
          scr_write(new_file_dst, fd_dst, buf, nread);
          fsync(fd_dst);

          /* update our position */
          new_position += (off_t) nread;
          old_position = new_position;

          /* record the updated position in the transfer file */
          update_transfer_file(new_file_src, new_file_dst, new_position);
        }

        /* if we've written all of the bytes, close the files */
        if (new_position == filesize) {
          close_files(new_file_src, &fd_src, new_file_dst, &fd_dst);
          clear_parameters(&new_file_src, &new_file_dst, &new_position);
          clear_parameters(&old_file_src, &old_file_dst, &old_position);
        }
      } else {
        /* TODO: we may have an error
         * (failed to open the source or dest file) */
        /* if we found no file to transfer, move to a STOPPED state */
        if (new_file_src == NULL) {
          state = STOPPED;
          set_transfer_file_state(SCR_TRANSFER_KEY_STATE_STOP, 1);
        }
      }
    }
  }

  /* free our file copy buffer */
  if (buf != NULL) {
    free(buf);
    buf = NULL;
  }

  /* free the strdup'd tranfer file name */
  if (scr_transfer_file != NULL) {
    free(scr_transfer_file);
    scr_transfer_file = NULL;
  }

  return 0;
}
