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

/* Implements a reliable open/read/write/close interface via open and close.
 * Implements directory manipulation functions. */

#include "scr.h"
#include "scr_err.h"
#include "scr_io.h"

#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <strings.h>

/* variable length args */
#include <stdarg.h>
#include <errno.h>

/* basename/dirname */
#include <unistd.h>
#include <libgen.h>

#define SCR_OPEN_TRIES  (5)
#define SCR_OPEN_USLEEP (100)

/*
=========================================
open/close/read/write functions
=========================================
*/

/* open file with specified flags and mode, retry open a few times on failure */
int scr_open(const char* file, int flags, ...)
{
  /* extract the mode (see man 2 open) */
  int mode_set = 0;
  mode_t mode = 0;
  if (flags & O_CREAT) {
    va_list ap;
    va_start(ap, flags);
    mode = va_arg(ap, mode_t);
    va_end(ap);
    mode_set = 1;
  }

  int fd = -1;
  if (mode_set) { 
    fd = open(file, flags, mode);
  } else {
    fd = open(file, flags);
  }
  if (fd < 0) {
    scr_dbg(1, "Opening file: open(%s) errno=%d %m @ %s:%d",
            file, errno, __FILE__, __LINE__
    );

    /* try again */
    int tries = SCR_OPEN_TRIES;
    while (tries && fd < 0) {
      usleep(SCR_OPEN_USLEEP);
      if (mode_set) { 
        fd = open(file, flags, mode);
      } else {
        fd = open(file, flags);
      }
      tries--;
    }

    /* if we still don't have a valid file, consider it an error */
    if (fd < 0) {
      scr_err("Opening file: open(%s) errno=%d %m @ %s:%d",
              file, errno, __FILE__, __LINE__
      );
    }
  }
  return fd;
}

/* close file with an fsync */
int scr_close(int fd)
{
  fsync(fd);
  return close(fd);
}

/* reliable read from file descriptor (retries, if necessary, until hard error) */
int scr_read(int fd, void* buf, size_t size)
{
  size_t n = 0;
  int retries = 10;
  while (n < size)
  {
    int rc = read(fd, (char*) buf + n, size - n);
    if (rc  > 0) {
      n += rc;
    } else if (rc == 0) {
      /* EOF */
      return n;
    } else { /* (rc < 0) */
      /* got an error, check whether it was serious */
      if(errno == EINTR || errno == EAGAIN) continue;

      /* something worth printing an error about */
      retries--;
      if (retries) {
        /* print an error and try again */
        scr_err("Error reading: read(%d, %x, %ld) errno=%d %m @ %s:%d",
                fd, (char*) buf + n, size - n, errno, __FILE__, __LINE__
               );
      } else {
        /* too many failed retries, give up */
        scr_err("Giving up read: read(%d, %x, %ld) errno=%d %m @ %s:%d",
	        fd, (char*) buf + n, size - n, errno, __FILE__, __LINE__
               );
        exit(1);
      }
    }
  }
  return size;
}

/* reliable write to file descriptor (retries, if necessary, until hard error) */
int scr_write(int fd, const void* buf, size_t size)
{
  size_t n = 0;
  int retries = 10;
  while (n < size)
  {
    int rc = write(fd, (char*) buf + n, size - n);
    if (rc > 0) {
      n += rc;
    } else if (rc == 0) {
      /* something bad happened, print an error and abort */
      scr_err("Error writing: write(%d, %x, %ld) returned 0 @ %s:%d",
	      fd, (char*) buf + n, size - n, __FILE__, __LINE__
             );
      exit(1);
    } else { /* (rc < 0) */
      /* got an error, check whether it was serious */
      if(errno == EINTR || errno == EAGAIN) continue;

      /* something worth printing an error about */
      retries--;
      if (retries) {
        /* print an error and try again */
        scr_err("Error writing: write(%d, %x, %ld) errno=%d %m @ %s:%d",
	        fd, (char*) buf + n, size - n, errno, __FILE__, __LINE__
               );
      } else {
        /* too many failed retries, give up */
        scr_err("Giving up write: write(%d, %x, %ld) errno=%d %m @ %s:%d",
	        fd, (char*) buf + n, size - n, errno, __FILE__, __LINE__
               );
        exit(1);
      }
    }
  }
  return size;
}

/* read count bytes from fd into buf starting from offset, pad with zero if file is too short */
int scr_read_pad(int fd, char* buf, unsigned long count, unsigned long offset, unsigned long filesize)
{
  off_t off_start = offset;
  off_t off_end   = offset + count;
  if (off_start < filesize) { 
    /* if our file has start of chunk, seek to it */
    lseek(fd, off_start, SEEK_SET);
    if (off_end > filesize) { 
      /* we have a partial chunk, read what we can, then pad with zero */
      size_t nread = filesize - off_start;
      scr_read(fd, buf, nread);
      memset(buf + nread, 0, count - nread);
    } else {
      /* we have the whole chunk, read it all in */
      scr_read(fd, buf, count);
    }
  } else {
    /* we don't have any of the chunk, set the whole buffer to zero */
    memset(buf, 0, count);
  }

  return SCR_SUCCESS;
}

/* like scr_read_pad, but this takes an array of open files and treats them as one single large file */
int scr_read_pad_n(int n, int* fds, char* buf, unsigned long count, unsigned long offset, unsigned long* filesizes)
{
  int i = 0;
  size_t pos = 0;
  off_t nseek = 0;
  off_t nread = 0;

  /* pass through files until we find the one containing our offset */
  while (i < n && (nseek + filesizes[i]) <= offset) {
    nseek += filesizes[i];
    i++;
  }

  /* seek to the proper position in the current file */
  if (i < n) {
      pos = offset - nseek;
      nseek += pos;
      lseek(fds[i], pos, SEEK_SET);
  }

  /* read data from files */
  while (nread < count && i < n) {
    /* assume we'll read the remainder of the current file */
    size_t num_to_read = filesizes[i] - pos;

    /* if we don't need to read the whole remainder of the file, adjust to the smaller amount */
    if (num_to_read > count - nread) { num_to_read = count - nread; }

    /* read data from file and add to the total read count */
    scr_read(fds[i], buf + nread, num_to_read);
    nread += num_to_read;

    /* advance to next file and seek to byte 0 */
    i++;
    if (i < n) {
      pos = 0;
      lseek(fds[i], pos, SEEK_SET);
    }
  }

  /* if count is bigger than all of our file data, pad with zeros on the end */
  if (nread < count) { memset(buf + nread, 0, count - nread); }

  return SCR_SUCCESS;
}

/* write to an array of open files with known filesizes treating them as one single large file */
int scr_write_pad_n(int n, int* fds, char* buf, unsigned long count, unsigned long offset, unsigned long* filesizes)
{
  int i = 0;
  size_t pos = 0;
  off_t nseek  = 0;
  off_t nwrite = 0;

  /* pass through files until we find the one containing our offset */
  while (i < n && (nseek + filesizes[i]) <= offset) {
    nseek += filesizes[i];
    i++;
  }

  /* seek to the proper position in the current file */
  if (i < n) {
      pos = offset - nseek;
      nseek += pos;
      lseek(fds[i], pos, SEEK_SET);
  }

  /* write data to files */
  while (nwrite < count && i < n) {
    /* assume we'll write the remainder of the current file */
    size_t num_to_write = filesizes[i] - pos;

    /* if we don't need to write the whole remainder of the file, adjust to the smaller amount */
    if (num_to_write > count - nwrite) { num_to_write = count - nwrite; }

    /* write data to file and add to the total write count */
    scr_write(fds[i], buf + nwrite, num_to_write);
    nwrite += num_to_write;

    /* advance to next file and seek to byte 0 */
    i++;
    if (i < n) {
      pos = 0;
      lseek(fds[i], pos, SEEK_SET);
    }
  }

  /* if count is bigger than all of our file data, just throw the data away */
  if (nwrite < count) {
    /* NOTHING TO DO */
  }

  return SCR_SUCCESS;
}

/* given a filename, return number of bytes in file */
unsigned long scr_filesize(const char* file)
{
  /* get file size in bytes */
  unsigned long bytes = 0;
  struct stat stat_buf;
  int stat_rc = stat(file, &stat_buf);
  if (stat_rc == 0) {
    /*
    mode = stat_buf.st_mode;
    */
    bytes = stat_buf.st_size;
  }
  return bytes;
}

/*
=========================================
Directory functions
=========================================
*/

/* split path and filename from fullpath on the rightmost '/'
 * assumes all filename if no '/' is found */
int scr_split_path (const char* file, char* path, char* filename)
{
  /* dirname and basename may modify their arguments, so we need to make a copy. */
  char* pcopy = strdup(file);
  char* ncopy = strdup(file);

  strcpy(path,     dirname(pcopy));
  strcpy(filename, basename(ncopy));

  free(ncopy);
  free(pcopy);
  return SCR_SUCCESS;
}

/* combine path and filename into a fullpath in file */
int scr_build_path (char* file, const char* path, const char* filename)
{
  /* first build in temp, then copy to file, which lets caller use same variable in input and output parameters */
  char temp[SCR_MAX_FILENAME];

  if ((path == NULL || strcmp(path, "")) && (filename == NULL || strcmp(filename, "") == 0)) {
    /* empty path and filename, just write an empty string to file */
    strcpy(temp, "");
  } else if (path == NULL || strcmp(path, "") == 0) {
    /* empty path, just return filename */
    strcpy(temp, filename);
  } else if (filename == NULL || strcmp(filename, "") == 0) {
    /* empty filename, just return path */
    strcpy(temp, path);
  } else {
    /* concatenate path and filename */
    sprintf(temp, "%s/%s", path, filename);
  }

  /* finally, copy from temp into file and return */
  strcpy(file, temp);
  return SCR_SUCCESS;
}

/* recursively create directory and subdirectories */
int scr_mkdir(const char* dir, mode_t mode)
{
  int rc = SCR_SUCCESS;

  /* With dirname, either the original string may be modified or the function may return a
   * pointer to static storage which will be overwritten by the next call to dirname,
   * so we need to strdup both the argument and the return string. */

  /* extract leading path from dir = full path - basename */
  char* dircopy = strdup(dir);
  char* path    = strdup(dirname(dircopy));

  /* if we can read path or path=="." or path=="/", then there's nothing to do,
   * otherwise, try to create it */
  if (access(path, R_OK) < 0 &&
      strcmp(path,".") != 0  &&
      strcmp(path,"/") != 0)
  {
    rc = scr_mkdir(path, mode);
  }

  /* if we can write to path, try to create subdir within path */
  if (access(path, W_OK) == 0 && rc == SCR_SUCCESS) {
    int tmp_rc = mkdir(dir, mode);
    if (tmp_rc < 0) {
      if (errno == EEXIST) {
        /* don't complain about mkdir for a directory that already exists */
        free(dircopy);
        free(path);
        return SCR_SUCCESS;
      } else {
        scr_err("Creating directory: mkdir(%s, %x) path=%s errno=%d %m @ %s:%d",
                dir, mode, path, errno, __FILE__, __LINE__
        );
        rc = SCR_FAILURE;
      }
    }
  } else {
    scr_err("Cannot write to directory: %s @ %s:%d",
            path, __FILE__, __LINE__
    );
    rc = SCR_FAILURE;
  }

  /* free our dup'ed string and return error code */
  free(dircopy);
  free(path);
  return rc;
}
