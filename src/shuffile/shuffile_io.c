/* All rights reserved. This program and the accompanying materials
 * are made available under the terms of the BSD-3 license which accompanies this
 * distribution in LICENSE.TXT
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the BSD-3  License in
 * LICENSE.TXT for more details.
 *
 * GOVERNMENT LICENSE RIGHTS-OPEN SOURCE SOFTWARE
 * The Government's rights to use, modify, reproduce, release, perform,
 * display, or disclose this software are subject to the terms of the BSD-3
 * License as provided in Contract No. B609815.
 * Any reproduction of computer software, computer software documentation, or
 * portions thereof marked with this legend must also reproduce the markings.
 *
 * Author: Christopher Holguin <christopher.a.holguin@intel.com>
 *
 * (C) Copyright 2015-2016 Intel Corporation.
 */

/* Implements a reliable open/read/write/close interface via open and close.
 * Implements directory manipulation functions. */

/* Please note todos in the cppr section; an optimization of using CPPR apis is
 * planned for upcoming work */

#include "shuffile_io.h"
#include "shuffile_util.h"

#include <stdlib.h>
#include <stdarg.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <strings.h>
#include <stdint.h>

/* variable length args */
#include <stdarg.h>
#include <errno.h>

/* basename/dirname */
#include <unistd.h>
#include <libgen.h>

/* compute crc32 */
#include <zlib.h>

/* flock */
#include <sys/file.h>

/* gettimeofday */
#include <sys/time.h>

#define SHUFFILE_OPEN_TRIES (5)
#define SHUFFILE_OPEN_USLEEP (1000)

/*
=========================================
open/lock/close/read/write functions
=========================================
*/

/* returns user's current mode as determine by his umask */
mode_t shuffile_getmode(int read, int write, int execute)
{
  /* lookup current mask and set it back */
  mode_t old_mask = umask(S_IWGRP | S_IWOTH);
  umask(old_mask);

  mode_t bits = 0;
  if (read) {
    bits |= (S_IRUSR | S_IRGRP | S_IROTH);
  }
  if (write) {
    bits |= (S_IWUSR | S_IWGRP | S_IWOTH);
  }
  if (execute) {
    bits |= (S_IXUSR | S_IXGRP | S_IXOTH);
  }

  /* convert mask to mode */
  mode_t mode = bits & ~old_mask & 0777;
  return mode;
}

/* open file with specified flags and mode, retry open a few times on failure */
int shuffile_open(const char* file, int flags, ...)
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
    shuffile_dbg(1, "Opening file: open(%s) errno=%d %s @ %s:%d",
      file, errno, strerror(errno), __FILE__, __LINE__
    );

    /* try again */
    int tries = SHUFFILE_OPEN_TRIES;
    while (tries && fd < 0) {
      usleep(SHUFFILE_OPEN_USLEEP);
      if (mode_set) {
        fd = open(file, flags, mode);
      } else {
        fd = open(file, flags);
      }
      tries--;
    }

    /* if we still don't have a valid file, consider it an error */
    if (fd < 0) {
      shuffile_err("Opening file: open(%s) errno=%d %s @ %s:%d",
        file, errno, strerror(errno), __FILE__, __LINE__
      );
    }
  }
  return fd;
}

/* fsync and close file */
int shuffile_close(const char* file, int fd)
{
  /* fsync first */
  if (fsync(fd) < 0) {
    /* print warning that fsync failed */
    shuffile_dbg(2, "Failed to fsync file descriptor: %s errno=%d %s @ %s:%d",
      file, errno, strerror(errno), __FILE__, __LINE__
    );
  }

  /* now close the file */
  if (close(fd) != 0) {
    /* hit an error, print message */
    shuffile_err("Closing file descriptor %d for file %s: errno=%d %s @ %s:%d",
      fd, file, errno, strerror(errno), __FILE__, __LINE__
    );
    return SHUFFILE_FAILURE;
  }

  return SHUFFILE_SUCCESS;
}

/* seek file descriptor to specified position */
int shuffile_lseek(const char* file, int fd, off_t pos, int whence)
{
  off_t rc = lseek(fd, pos, whence);
  if (rc == (off_t)-1) {
    shuffile_err("Error seeking %s: errno=%d %s @ %s:%d",
      file, errno, strerror(errno), __FILE__, __LINE__
    );
    return SHUFFILE_FAILURE;
  }
  return SHUFFILE_SUCCESS;
}

/* reliable read from file descriptor (retries, if necessary, until hard error) */
ssize_t shuffile_read(const char* file, int fd, void* buf, size_t size)
{
  ssize_t n = 0;
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
      if (errno == EINTR || errno == EAGAIN) {
        continue;
      }

      /* something worth printing an error about */
      retries--;
      if (retries) {
        /* print an error and try again */
        shuffile_err("Error reading %s: read(%d, %x, %ld) errno=%d %s @ %s:%d",
          file, fd, (char*) buf + n, size - n, errno, strerror(errno), __FILE__, __LINE__
        );
      } else {
        /* too many failed retries, give up */
        shuffile_err("Giving up read of %s: read(%d, %x, %ld) errno=%d %s @ %s:%d",
	  file, fd, (char*) buf + n, size - n, errno, strerror(errno), __FILE__, __LINE__
        );
        exit(1);
      }
    }
  }
  return n;
}

/* reliable write to opened file descriptor (retries, if necessary, until hard error) */
ssize_t shuffile_write(const char* file, int fd, const void* buf, size_t size)
{
  ssize_t n = 0;
  int retries = 10;
  while (n < size)
  {
    ssize_t rc = write(fd, (char*) buf + n, size - n);
    if (rc > 0) {
      n += rc;
    } else if (rc == 0) {
      /* something bad happened, print an error and abort */
      shuffile_err("Error writing %s: write(%d, %x, %ld) returned 0 @ %s:%d",
	file, fd, (char*) buf + n, size - n, __FILE__, __LINE__
      );
      exit(1);
    } else { /* (rc < 0) */
      /* got an error, check whether it was serious */
      if (errno == EINTR || errno == EAGAIN) {
        continue;
      }

      /* something worth printing an error about */
      retries--;
      if (retries) {
        /* print an error and try again */
        shuffile_err("Error writing %s: write(%d, %x, %ld) errno=%d %s @ %s:%d",
          file, fd, (char*) buf + n, size - n, errno, strerror(errno), __FILE__, __LINE__
        );
      } else {
        /* too many failed retries, give up */
        shuffile_err("Giving up write to %s: write(%d, %x, %ld) errno=%d %s @ %s:%d",
          file, fd, (char*) buf + n, size - n, errno, strerror(errno), __FILE__, __LINE__
        );
        exit(1);
      }
    }
  }
  return n;
}

/* make a good attempt to read from file (retries, if necessary, return error if fail) */
ssize_t shuffile_read_attempt(const char* file, int fd, void* buf, size_t size)
{
  ssize_t n = 0;
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
      if (errno == EINTR || errno == EAGAIN) {
        continue;
      }

      /* something worth printing an error about */
      retries--;
      if (retries) {
        /* print an error and try again */
        shuffile_err("Error reading file %s errno=%d %s @ %s:%d",
          file, errno, strerror(errno), __FILE__, __LINE__
        );
      } else {
        /* too many failed retries, give up */
        shuffile_err("Giving up read on file %s errno=%d %s @ %s:%d",
	  file, errno, strerror(errno), __FILE__, __LINE__
        );
        return -1;
      }
    }
  }
  return n;
}

/* make a good attempt to write to file (retries, if necessary, return error if fail) */
ssize_t shuffile_write_attempt(const char* file, int fd, const void* buf, size_t size)
{
  ssize_t n = 0;
  int retries = 10;
  while (n < size)
  {
    ssize_t rc = write(fd, (char*) buf + n, size - n);
    if (rc > 0) {
      n += rc;
    } else if (rc == 0) {
      /* something bad happened, print an error and abort */
      shuffile_err("Error writing file %s write returned 0 @ %s:%d",
        file, __FILE__, __LINE__
      );
      return -1;
    } else { /* (rc < 0) */
      /* got an error, check whether it was serious */
      if (errno == EINTR || errno == EAGAIN) {
        continue;
      }

      /* something worth printing an error about */
      retries--;
      if (retries) {
        /* print an error and try again */
        shuffile_err("Error writing file %s errno=%d %s @ %s:%d",
          file, errno, strerror(errno), __FILE__, __LINE__
        );
      } else {
        /* too many failed retries, give up */
        shuffile_err("Giving up write of file %s errno=%d %s @ %s:%d",
          file, errno, strerror(errno), __FILE__, __LINE__
        );
        return -1;
      }
    }
  }
  return n;
}

/* read line from file into buf with given size */
ssize_t shuffile_read_line(const char* file, int fd, char* buf, size_t size)
{
  /* read up to size-1 bytes from fd into buf until we find a newline or EOF */
  ssize_t n = 0;
  int found_end = 0;
  while (n < size-1 && !found_end) {
    /* read a character from the file */
    char c;
    ssize_t nread = shuffile_read(file, fd, &c, sizeof(c));

    if (nread > 0) {
      /* we read a character, copy it over to the buffer */
      buf[n] = c;
      n++;

      /* check whether we hit the end of the line */
      if (c == '\n') {
        found_end = 1;
      }
    } else if (nread == 0) {
      /* we hit the end of the file */
      found_end = 1;
    } else { /* nread < 0 */
      /* we hit an error */
      shuffile_err("Error reading from file %s @ %s:%d",
        file, __FILE__, __LINE__
      );
      return -1;
    }
  }

  /* tack on the NULL character */
  buf[n] = '\0';

  /* if we exit the while loop but didn't find the end of the line, the buffer was too small */
  if (!found_end) {
    shuffile_err("Buffer too small to read line from file %s @ %s:%d",
      file, __FILE__, __LINE__
    );
    return -1;
  }

  /* NOTE: we don't want to count the NULL which we added, but there is no need to adjust n here */
  return n;
}

/* write a formatted string to specified file descriptor */
ssize_t shuffile_writef(const char* file, int fd, const char* format, ...)
{
  /* write the formatted string to a buffer */
  char buf[SHUFFILE_MAX_LINE];
  va_list argp;
  va_start(argp, format);
  int n = vsnprintf(buf, sizeof(buf), format, argp);
  va_end(argp);

  /* check that our buffer was large enough */
  if (sizeof(buf) <= n) {
    /* TODO: instead of throwing a fatal error, we could allocate a bigger buffer and try again */

    shuffile_err("Buffer too small to hold formatted string for file %s @ %s:%d",
      file, __FILE__, __LINE__
    );
    exit(1);
  }

  /* write the string out to the file descriptor */
  ssize_t rc = shuffile_write(file, fd, buf, n);

  return rc;
}

/* logically concatenate n opened files and read count bytes from this logical file into buf starting
 * from offset, pad with zero on end if missing data */
int shuffile_read_pad_n(int n, const char** files, int* fds,
                   char* buf, unsigned long count, unsigned long offset, unsigned long* filesizes)
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
      if (lseek(fds[i], pos, SEEK_SET) == (off_t)-1) {
        /* our seek failed, return an error */
        shuffile_err("Failed to seek to byte %lu in %s @ %s:%d",
          pos, files[i], __FILE__, __LINE__
        );
        return SHUFFILE_FAILURE;
      }
  }

  /* read data from files */
  while (nread < count && i < n) {
    /* assume we'll read the remainder of the current file */
    size_t num_to_read = filesizes[i] - pos;

    /* if we don't need to read the whole remainder of the file, adjust to the smaller amount */
    if (num_to_read > count - nread) {
      num_to_read = count - nread;
    }

    /* read data from file and add to the total read count */
    if (shuffile_read_attempt(files[i], fds[i], buf + nread, num_to_read) != num_to_read) {
      /* our read failed, return an error */
      return SHUFFILE_FAILURE;
    }
    nread += num_to_read;

    /* advance to next file and seek to byte 0 */
    i++;
    if (i < n) {
      pos = 0;
      if (lseek(fds[i], pos, SEEK_SET) == (off_t)-1) {
        /* our seek failed, return an error */
        shuffile_err("Failed to seek to byte %lu in %s @ %s:%d",
          pos, files[i], __FILE__, __LINE__
        );
        return SHUFFILE_FAILURE;
      }
    }
  }

  /* if count is bigger than all of our file data, pad with zeros on the end */
  if (nread < count) {
    memset(buf + nread, 0, count - nread);
  }

  return SHUFFILE_SUCCESS;
}

/* write to an array of open files with known filesizes treating them as one single large file */
int shuffile_write_pad_n(int n, const char** files, int* fds,
                    char* buf, unsigned long count, unsigned long offset, unsigned long* filesizes)
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
      if (lseek(fds[i], pos, SEEK_SET) == (off_t)-1) {
        /* our seek failed, return an error */
        shuffile_err("Failed to seek to byte %lu in %s @ %s:%d",
          pos, files[i], __FILE__, __LINE__
        );
        return SHUFFILE_FAILURE;
      }
  }

  /* write data to files */
  while (nwrite < count && i < n) {
    /* assume we'll write the remainder of the current file */
    size_t num_to_write = filesizes[i] - pos;

    /* if we don't need to write the whole remainder of the file, adjust to the smaller amount */
    if (num_to_write > count - nwrite) {
      num_to_write = count - nwrite;
    }

    /* write data to file and add to the total write count */
    if (shuffile_write_attempt(files[i], fds[i], buf + nwrite, num_to_write) != num_to_write) {
      /* our write failed, return an error */
      return SHUFFILE_FAILURE;
    }
    nwrite += num_to_write;

    /* advance to next file and seek to byte 0 */
    i++;
    if (i < n) {
      pos = 0;
      if (lseek(fds[i], pos, SEEK_SET) == (off_t)-1) {
        /* our seek failed, return an error */
        shuffile_err("Failed to seek to byte %lu in %s @ %s:%d",
          pos, files[i], __FILE__, __LINE__
        );
        return SHUFFILE_FAILURE;
      }
    }
  }

  /* if count is bigger than all of our file data, just throw the data away */
  if (nwrite < count) {
    /* NOTHING TO DO */
  }

  return SHUFFILE_SUCCESS;
}

/* given a filename, return stat info */
int shuffile_stat(const char* file, struct stat* statbuf)
{
  int rc = stat(file, statbuf);
  if (rc < 0) {
    shuffile_err("Error stat'ing file %s: errno=%d %s @ %s:%d",
      file, errno, strerror(errno), __FILE__, __LINE__
    );
  }
  return rc;
}

/* given a filename, return number of bytes in file */
unsigned long shuffile_file_size(const char* file)
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

/* tests whether the file or directory exists */
int shuffile_file_exists(const char* file)
{
  /* check whether the file exists */
  if (access(file, F_OK) < 0) {
    /* TODO: would be nice to print a message here, but
     *       functions calling this expect it to be quiet
    shuffile_dbg(2, "File does not exist: %s errno=%d %s @ %s:%d",
      file, errno, strerror(errno), __FILE__, __LINE__
    );
    */
    return SHUFFILE_FAILURE;
  }
  return SHUFFILE_SUCCESS;
}

/* tests whether the file or directory is readable */
int shuffile_file_is_readable(const char* file)
{
  /* check whether the file can be read */
  if (access(file, R_OK) < 0) {
    /* TODO: would be nice to print a message here, but
     *       functions calling this expect it to be quiet
    shuffile_dbg(2, "File not readable: %s errno=%d %s @ %s:%d",
      file, errno, strerror(errno), __FILE__, __LINE__
    );
    */
    return SHUFFILE_FAILURE;
  }
  return SHUFFILE_SUCCESS;
}

/* tests whether the file or directory is writeable */
int shuffile_file_is_writeable(const char* file)
{
  /* check whether the file can be read */
  if (access(file, W_OK) < 0) {
    /* TODO: would be nice to print a message here, but
     *       functions calling this expect it to be quiet
    shuffile_dbg(2, "File not writeable: %s errno=%d %s @ %s:%d",
      file, errno, strerror(errno), __FILE__, __LINE__
    );
    */
    return SHUFFILE_FAILURE;
  }
  return SHUFFILE_SUCCESS;
}

/* delete a file */
int shuffile_file_unlink(const char* file)
{
  if (unlink(file) != 0) {
    /* hit an error deleting, but don't care if we failed
     * because there is no file at that path */
    if (errno != ENOENT) {
      shuffile_dbg(2, "Failed to delete file: %s errno=%d %s @ %s:%d",
        file, errno, strerror(errno), __FILE__, __LINE__
      );
      return SHUFFILE_FAILURE;
    }
  }
  return SHUFFILE_SUCCESS;
}

/* opens, reads, and computes the crc32 value for the given filename */
int shuffile_crc32(const char* filename, uLong* crc)
{
  /* check that we got a variable to write our answer to */
  if (crc == NULL) {
    return SHUFFILE_FAILURE;
  }

  /* initialize our crc value */
  *crc = crc32(0L, Z_NULL, 0);

  /* open the file for reading */
  int fd = shuffile_open(filename, O_RDONLY);
  if (fd < 0) {
    shuffile_dbg(1, "Failed to open file to compute crc: %s errno=%d @ %s:%d",
      filename, errno, __FILE__, __LINE__
    );
    return SHUFFILE_FAILURE;
  }

  /* read the file data in and compute its crc32 */
  int nread = 0;
  unsigned long buffer_size = 1024*1024;
  char buf[buffer_size];
  do {
    nread = shuffile_read(filename, fd, buf, buffer_size);
    if (nread > 0) {
      *crc = crc32(*crc, (const Bytef*) buf, (uInt) nread);
    }
  } while (nread == buffer_size);

  /* if we got an error, don't print anything and bailout */
  if (nread < 0) {
    shuffile_dbg(1, "Error while reading file to compute crc: %s @ %s:%d",
      filename, __FILE__, __LINE__
    );
    close(fd);
    return SHUFFILE_FAILURE;
  }

  /* close the file */
  shuffile_close(filename, fd);

  return SHUFFILE_SUCCESS;
}

/*
=========================================
Directory functions
=========================================
*/

/* recursively create directory and subdirectories */
int shuffile_mkdir(const char* dir, mode_t mode)
{
  /* consider it a success if we either create the directory
   * or we fail because it already exists */
  int tmp_rc = mkdir(dir, mode);
  if (tmp_rc == 0 || errno == EEXIST) {
    return SHUFFILE_SUCCESS;
  }

  /* failed to create the directory,
   * we'll check the parent dir and try again */
  int rc = SHUFFILE_SUCCESS;

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
    rc = shuffile_mkdir(path, mode);
  }

  /* if we can write to path, try to create subdir within path */
  if (access(path, W_OK) == 0 && rc == SHUFFILE_SUCCESS) {
    tmp_rc = mkdir(dir, mode);
    if (tmp_rc < 0) {
      if (errno == EEXIST) {
        /* don't complain about mkdir for a directory that already exists */
        shuffile_free(&dircopy);
        shuffile_free(&path);
        return SHUFFILE_SUCCESS;
      } else {
        shuffile_err("Creating directory: mkdir(%s, %x) path=%s errno=%d %s @ %s:%d",
          dir, mode, path, errno, strerror(errno), __FILE__, __LINE__
        );
        rc = SHUFFILE_FAILURE;
      }
    }
  } else {
    shuffile_err("Cannot write to directory: %s @ %s:%d",
      path, __FILE__, __LINE__
    );
    rc = SHUFFILE_FAILURE;
  }

  /* free our dup'ed string and return error code */
  shuffile_free(&dircopy);
  shuffile_free(&path);
  return rc;
}

/* remove directory */
int shuffile_rmdir(const char* dir)
{
  /* delete directory */
  int rc = rmdir(dir);
  if (rc < 0) {
    /* whoops, something failed when we tried to delete our directory */
    shuffile_err("Error deleting directory: %s (rmdir returned %d %s) @ %s:%d",
      dir, rc, strerror(errno), __FILE__, __LINE__
    );
    return SHUFFILE_FAILURE;
  }
  return SHUFFILE_SUCCESS;
}

/* write current working directory to buf */
int shuffile_getcwd(char* buf, size_t size)
{
  int rc = SHUFFILE_SUCCESS;
  if (getcwd(buf, size) == NULL) {
    shuffile_abort(-1, "Problem reading current working directory (getcwd() errno=%d %s) @ %s:%d",
              errno, strerror(errno), __FILE__, __LINE__
    );
    rc = SHUFFILE_FAILURE;
  }
  return rc;
}
