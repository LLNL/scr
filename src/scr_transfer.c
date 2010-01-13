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
#include "scr_err.h"
#include "scr_hash.h"

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

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#ifndef SCR_FILE_BUF_SIZE
#define SCR_FILE_BUF_SIZE (1024*1024)
//#define SCR_FILE_BUF_SIZE (1)
#endif

char scr_transfer_file[] = "transfer.scrinfo";
double bytes_per_second = 1.11;

int need_transfer(struct scr_hash* files, char* src, char** dst, off_t* position)
{
  /* check that we got a hash of files and a file name */
  if (files == NULL || src == NULL) {
    return SCR_FAILURE;
  }

  /* lookup the specified file in the hash */
  struct scr_hash* file_hash = scr_hash_get(files, src);
  if (file_hash == NULL) {
    return SCR_FAILURE;
  }

  /* extract the values for file size, bytes written, and destination */
  char* size    = scr_hash_elem_get_first_val(file_hash, "SIZE");
  char* written = scr_hash_elem_get_first_val(file_hash, "WRITTEN");
  char* dest    = scr_hash_elem_get_first_val(file_hash, "DESTINATION");

  /* if the bytes written value is less than the file size, we've got a valid file */
  if (size != NULL && written != NULL) {
    /* convert the file size and bytes written strings to numbers */
    off_t size_count    = strtoul(size,    NULL, 0);
    off_t written_count = strtoul(written, NULL, 0);

    if (written_count < size_count) {
      /* got our file, fill in output parameters */
      *dst = strdup(dest);
      *position = written_count;
      return SCR_SUCCESS;
    }
  }

  return SCR_FAILURE;
}

int get_transfer_file(char** src, char** dst, off_t* position)
{
  int found_a_file = 0;

  /* create a hash to store data from file */
  struct scr_hash* hash = scr_hash_new();

  /* open transfer file with lock */
  int fd = scr_open_with_lock(scr_transfer_file, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  if (fd >= 0) {
    /* read, close, and release the lock on the file */
    scr_hash_read_fd(scr_transfer_file, fd, hash);
    scr_close_with_unlock(scr_transfer_file, fd);

    /* search for a file to trasnfer */
    struct scr_hash* files = scr_hash_get(hash, "FILES");
    if (files != NULL) {
      /* if we're given a file name, try to continue with that file */
      if (!found_a_file && src != NULL && *src != NULL) {
        if (need_transfer(files, *src, dst, position) == SCR_SUCCESS) {
          /* can continue with the same file (position may have been updated though) */
          found_a_file = 1;
        } else {
          /* user's file no longer needs transfered, free the string and set it to NULL */
          free(*src);
          *src = NULL;
        }
      }

      /* otherwise, scan for a file and use the first file we find */
      if (!found_a_file) {
        struct scr_hash_elem* elem;
        for (elem = scr_hash_elem_first(files);
             elem != NULL;
             elem = scr_hash_elem_next(elem))
        {
          char* name = scr_hash_elem_key(elem);
          if (name != NULL &&
              need_transfer(files, name, dst, position) == SCR_SUCCESS)
          {
            /* found a file, copy its name (the destination and postion are set in need_transfer) */
            *src = strdup(name);
            found_a_file = 1;
            break;
          }
        }
      }
    }
  }

  /* free the hash */
  scr_hash_delete(hash);

  /* if we don't find a file, set src and dst to NULL and set position to 0 */
  if (!found_a_file) {
    if (src != NULL) {
      /* if the user named a file in the call, free the string */
      if (*src != NULL) {
        free(*src);
      }

      /* set string to NULL to indicate there is no file */
      *src = NULL;
    }

    if (dst != NULL) {
      /* set string to NULL to indicate there is no file */
      *dst = NULL;
    }

    if (position != NULL) {
      *position = 0;
    }

    return SCR_FAILURE;
  }

  printf("%s WRITTEN %lu DESTINATION %s\n", *src, *position, *dst);

  return SCR_SUCCESS;
}

int update_transfer_file(char* src, char* dst, off_t position)
{
  /* create a hash to store data from file */
  struct scr_hash* hash = scr_hash_new();

  /* open transfer file with lock */
  int fd = scr_open_with_lock(scr_transfer_file, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  if (fd >= 0) {
    /* read the file */
    scr_hash_read_fd(scr_transfer_file, fd, hash);

    /* search for the source file, and update the bytes written if found */
    int found = 0;
    if (src != NULL) {
      struct scr_hash* myfile = scr_hash_get_kv(hash, "FILES", src);
      if (myfile != NULL) {
        /* found the file */
        found = 1;

        /* update the bytes written field */
        scr_hash_unset(myfile, "WRITTEN");
        scr_hash_setf(myfile, NULL, "%s %lu", "WRITTEN", position);
      }
    }

    /* write transfer file back out with updated bytes written */
    if (found) {
      /* wind the file pointer back to the start of the file */
      lseek(fd, 0, SEEK_SET);

      /* write the updated hash back to the file */
      ssize_t bytes_written = scr_hash_write_fd(scr_transfer_file, fd, hash);

      /* truncate file to correct size (may be smaller than it was before) */
      if (bytes_written >= 0) {
        ftruncate(fd, (off_t) bytes_written);
      }
    }

    /* close the transfer file and release the lock */
    scr_close_with_unlock(scr_transfer_file, fd);
  }

  /* free the hash */
  scr_hash_delete(hash);

  return SCR_SUCCESS;
}

/* this code caches the opened file descriptors to avoid extra opens, seeks, and closes */
int main (int argc, char *argv[])
{
  /*
     initialize our internal state
     while(1)
       read transfer file, and sync our internal state
       if no file is open, but we need to write one
          open file, seek to pos
       if writing and within bw limit
         read, write, flush
       if done writing file
         close file and flush
       record updated state in transfer file (current bytes written, transfer status)
       sleep
  */
  
  /* initialize our tracking variables */
  int fd_src = -1;
  int fd_dst = -1;

  char* file_src     = NULL;
  char* old_file_src = NULL;
  char* file_dst     = NULL;
  char* old_file_dst = NULL;

  off_t position     = 0;
  off_t old_position = 0;

  while (1) {
    /* start timer */
    double start = scr_seconds();

    /* free off destination file name if we have one, this will get filled in by get_transfer_file */
    /* NOTE: we don't free file_src, since that's our way to tell get_transfer_file to continue
     * with the same file if possible */
    if (file_dst != NULL) {
      free(file_dst);
      file_dst = NULL;
    }

    /* find a file to copy data for */
    get_transfer_file(&file_src, &file_dst, &position);

    /* if we got a new file, close the old one (if open), open the new file */
    if ((file_src != NULL && old_file_src == NULL) ||
        (file_src == NULL && old_file_src != NULL) ||
        (file_src != NULL && old_file_src != NULL && strcmp(file_src, old_file_src) != 0))
    {
      /* close the old descriptor if its still open */
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
      if (file_src != NULL) {
        fd_src = scr_open(file_src, O_RDONLY);
        old_file_src = strdup(file_src);
      }
    }

    /* if we got a new file, close the old one (if open), open the new file */
    if ((file_dst != NULL && old_file_dst == NULL) ||
        (file_dst == NULL && old_file_dst != NULL) ||
        (file_dst != NULL && old_file_dst != NULL && strcmp(file_dst, old_file_dst) != 0))
    {
      /* close the old descriptor if its still open */
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
      if (file_dst != NULL) {
        fd_dst = scr_open(file_dst, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
        old_file_dst = strdup(file_dst);
      }
    }

    /* we may have the same file, but perhaps the position changed (may need to seek) */
    if (position != old_position) {
      if (fd_src >= 0) {
        /* TODO: check for errors here */
        lseek(fd_src, position, SEEK_SET);
      }

      if (fd_dst >= 0) {
        /* TODO: check for errors here */
        lseek(fd_dst, position, SEEK_SET);
      }

      /* remember the new position */
      old_position = position;
    }

    /* if we have two open files, copy a chunk from source file to destination file */
    int nread = 0;
    if (fd_src >= 0 && fd_dst >= 0) {
      /* read a chunk */
      char buf[SCR_FILE_BUF_SIZE];
      nread = scr_read(fd_src, buf, sizeof(buf));

      if (nread > 0) {
        /* write the chunk out with an fsync to know for sure it was written */
        scr_write(fd_dst, buf, nread);
        fsync(fd_dst);

        /* update our position */
        position += nread;

        /* write the new position in this file to the transfer file */
        update_transfer_file(file_src, file_dst, position);
        old_position = position;
      }

      /* if we hit the end of the source file, close the files and reinit our variables */
      if (nread < sizeof(buf)) {
        scr_close(file_src, fd_src);
        fd_src = -1;

        scr_close(file_dst, fd_dst);
        fd_dst = -1;

        if (file_src != NULL) {
          free(file_src);
          file_src     = NULL;
        }
        if (old_file_src != NULL) {
          free(old_file_src);
          old_file_src = NULL;
        }
        if (file_dst != NULL) {
          free(file_dst);
          file_dst     = NULL;
        }
        if (old_file_dst != NULL) {
          free(old_file_dst);
          old_file_dst = NULL;
        }

        position     = 0;
        old_position = 0;
      }
    }

    int usecs = 60*1000*1000; /* wait for a minute if we have nothing to do */

    /* stop timer and compute time to sleep before attempting next write */
    double end = scr_seconds();
    double time = end - start;
    printf("Bytes written %d, Time taken %f\n", nread, time);
    if (nread > 0 && time > 0) {
      usecs = (int) (((double) nread / (double) bytes_per_second) - time) * 1000000;
      if (usecs < 0) {
        usecs = 0;
      }
    }
usecs = 1000*1000;

    /* sleep before writing more data */
    if (usecs > 0) {
      printf("Sleeping for %lu usecs\n", (unsigned long) usecs);
      usleep((unsigned long) usecs);
    }
  }

  return 0;
}
