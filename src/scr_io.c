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

#include "scr_conf.h"
#include "scr.h"
#include "scr_err.h"
#include "scr_io.h"
#include "scr_util.h"

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

/*
=========================================
open/lock/close/read/write functions
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

/* fsync and close file */
int scr_close(const char* file, int fd)
{
  /* fsync first */
  if (fsync(fd) < 0) {
    /* print warning that fsync failed */
    scr_dbg(2, "Failed to fsync file descriptor: %s errno=%d %m @ file %s:%d",
            file, errno, __FILE__, __LINE__
    );
  }

  /* now close the file */
  if (close(fd) != 0) {
    /* hit an error, print message */
    scr_err("Closing file descriptor %d for file %s: errno=%d %m @ %s:%d",
            fd, file, errno, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  return SCR_SUCCESS;
}

int scr_file_lock_read(const char* file, int fd)
{
  #ifdef SCR_FILE_LOCK_USE_FLOCK
    if (flock(fd, LOCK_SH) != 0) {
      scr_err("Failed to acquire file lock on %s: flock(%d, %d) errno=%d %m @ %s:%d",
              file, fd, LOCK_SH, errno, __FILE__, __LINE__
      );
      return SCR_FAILURE;
    }
  #endif

  #ifdef SCR_FILE_LOCK_USE_FCNTL
    struct flock lck;
    lck.l_type = F_RDLCK;
    lck.l_whence = 0;
    lck.l_start = 0L;
    lck.l_len = 0L; //locking the entire file

    if(fcntl(fd, F_SETLK, &lck) < 0) {
      scr_err("Failed to acquire file read lock on %s: fnctl(%d, %d) errno=%d %m @ %s:%d",
              file, fd, F_RDLCK, errno, __FILE__, __LINE__
      );
      return SCR_FAILURE;
    }
  #endif

  return SCR_SUCCESS;
}

int scr_file_lock_write(const char* file, int fd)
{
  #ifdef SCR_FILE_LOCK_USE_FLOCK
    if (flock(fd, LOCK_EX) != 0) {
      scr_err("Failed to acquire file lock on %s: flock(%d, %d) errno=%d %m @ %s:%d",
              file, fd, LOCK_EX, errno, __FILE__, __LINE__
      );
      return SCR_FAILURE;
    }
  #endif

  #ifdef SCR_FILE_LOCK_USE_FCNTL
    struct flock lck;
    lck.l_type = F_WRLCK;
    lck.l_whence = 0;
    lck.l_start = 0L;
    lck.l_len = 0L; //locking the entire file

    if(fcntl(fd, F_SETLK, &lck) < 0) {
      scr_err("Failed to acquire file read lock on %s: fnctl(%d, %d) errno=%d %m @ %s:%d",
              file, fd, F_WRLCK, errno, __FILE__, __LINE__
      );
      return SCR_FAILURE;
    }
  #endif

  return SCR_SUCCESS;
}

int scr_file_unlock(const char* file, int fd)
{
  #ifdef SCR_FILE_LOCK_USE_FLOCK
    if (flock(fd, LOCK_UN) != 0) {
      scr_err("Failed to acquire file lock on %s: flock(%d, %d) errno=%d %m @ %s:%d",
              file, fd, LOCK_UN, errno, __FILE__, __LINE__
      );
      return SCR_FAILURE;
    }
  #endif

  #ifdef SCR_FILE_LOCK_USE_FCNTL
    struct flock lck;
    lck.l_type = F_UNLCK;
    lck.l_whence = 0;
    lck.l_start = 0L;
    lck.l_len = 0L; //locking the entire file

    if(fcntl(fd, F_SETLK, &lck) < 0) {
      scr_err("Failed to acquire file read lock on %s: fnctl(%d, %d) errno=%d %m @ %s:%d",
              file, fd, F_UNLCK, errno, __FILE__, __LINE__
      );
      return SCR_FAILURE;
    }
  #endif

  return SCR_SUCCESS;
}

/* opens specified file and waits on a lock before returning the file descriptor */
int scr_open_with_lock(const char* file, int flags, mode_t mode)
{
  /* open the file */
  int fd = scr_open(file, flags, mode);
  if (fd < 0) {
    scr_err("Opening file for write: scr_open(%s) errno=%d %m @ %s:%d",
            file, errno, __FILE__, __LINE__
    );
    return fd;
  }

  /* acquire an exclusive file lock */
  int ret = scr_file_lock_write(file, fd);
  if (ret != SCR_SUCCESS) {
    close(fd);
    return ret;
  }
     
  /* return the opened file descriptor */
  return fd;
}

/* unlocks the specified file descriptor and then closes the file */
int scr_close_with_unlock(const char* file, int fd)
{
  /* release the file lock */
  int ret = scr_file_unlock(file, fd);
  if (ret != SCR_SUCCESS) {
    return ret;
  }

  /* close the file */
  return scr_close(file, fd);
}

/* reliable read from file descriptor (retries, if necessary, until hard error) */
ssize_t scr_read(const char* file, int fd, void* buf, size_t size)
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
        scr_err("Error reading %s: read(%d, %x, %ld) errno=%d %m @ %s:%d",
                file, fd, (char*) buf + n, size - n, errno, __FILE__, __LINE__
        );
      } else {
        /* too many failed retries, give up */
        scr_err("Giving up read of %s: read(%d, %x, %ld) errno=%d %m @ %s:%d",
	        file, fd, (char*) buf + n, size - n, errno, __FILE__, __LINE__
        );
        exit(1);
      }
    }
  }
  return n;
}

/* reliable write to opened file descriptor (retries, if necessary, until hard error) */
ssize_t scr_write(const char* file, int fd, const void* buf, size_t size)
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
      scr_err("Error writing %s: write(%d, %x, %ld) returned 0 @ %s:%d",
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
        scr_err("Error writing %s: write(%d, %x, %ld) errno=%d %m @ %s:%d",
                file, fd, (char*) buf + n, size - n, errno, __FILE__, __LINE__
        );
      } else {
        /* too many failed retries, give up */
        scr_err("Giving up write to %s: write(%d, %x, %ld) errno=%d %m @ %s:%d",
                file, fd, (char*) buf + n, size - n, errno, __FILE__, __LINE__
        );
        exit(1);
      }
    }
  }
  return n;
}

/* make a good attempt to read from file (retries, if necessary, return error if fail) */
ssize_t scr_read_attempt(const char* file, int fd, void* buf, size_t size)
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
        scr_err("Error reading file %s errno=%d %m @ %s:%d",
                file, errno, __FILE__, __LINE__
        );
      } else {
        /* too many failed retries, give up */
        scr_err("Giving up read on file %s errno=%d %m @ %s:%d",
	        file, errno, __FILE__, __LINE__
        );
        return -1;
      }
    }
  }
  return n;
}

/* make a good attempt to write to file (retries, if necessary, return error if fail) */
ssize_t scr_write_attempt(const char* file, int fd, const void* buf, size_t size)
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
      scr_err("Error writing file %s write returned 0 @ %s:%d",
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
        scr_err("Error writing file %s errno=%d %m @ %s:%d",
                file, errno, __FILE__, __LINE__
        );
      } else {
        /* too many failed retries, give up */
        scr_err("Giving up write of file %s errno=%d %m @ %s:%d",
                file, errno, __FILE__, __LINE__
        );
        return -1;
      }
    }
  }
  return n;
}

/* read line from file into buf with given size */
ssize_t scr_read_line(const char* file, int fd, char* buf, size_t size)
{
  /* read up to size-1 bytes from fd into buf until we find a newline or EOF */
  ssize_t n = 0;
  int found_end = 0;
  while (n < size-1 && !found_end) {
    /* read a character from the file */
    char c;
    ssize_t nread = scr_read(file, fd, &c, sizeof(c));

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
      scr_err("Error reading from file %s @ %s:%d",
              file, __FILE__, __LINE__
      );
      return -1;
    }
  }

  /* tack on the NULL character */
  buf[n] = '\0';

  /* if we exit the while loop but didn't find the end of the line, the buffer was too small */
  if (!found_end) {
    scr_err("Buffer too small to read line from file %s @ %s:%d",
            file, __FILE__, __LINE__
    );
    return -1;
  }

  /* NOTE: we don't want to count the NULL which we added, but there is no need to adjust n here */
  return n;
}

/* write a formatted string to specified file descriptor */
ssize_t scr_writef(const char* file, int fd, const char* format, ...)
{
  /* write the formatted string to a buffer */
  char buf[SCR_MAX_LINE];
  va_list argp;
  va_start(argp, format);
  int n = vsnprintf(buf, sizeof(buf), format, argp);
  va_end(argp);

  /* check that our buffer was large enough */
  if (sizeof(buf) <= n) {
    /* TODO: instead of throwing a fatal error, we could allocate a bigger buffer and try again */

    scr_err("Buffer too small to hold formatted string for file %s @ %s:%d",
            file, __FILE__, __LINE__
    );
    exit(1);
  }

  /* write the string out to the file descriptor */
  ssize_t rc = scr_write(file, fd, buf, n);

  return rc;
}

/* logically concatenate n opened files and read count bytes from this logical file into buf starting
 * from offset, pad with zero on end if missing data */
int scr_read_pad_n(int n, char** files, int* fds,
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
        scr_err("Failed to seek to byte %lu in %s @ %s:%d",
                pos, files[i], __FILE__, __LINE__
        );
        return SCR_FAILURE;
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
    if (scr_read_attempt(files[i], fds[i], buf + nread, num_to_read) != num_to_read) {
      /* our read failed, return an error */
      return SCR_FAILURE;
    }
    nread += num_to_read;

    /* advance to next file and seek to byte 0 */
    i++;
    if (i < n) {
      pos = 0;
      if (lseek(fds[i], pos, SEEK_SET) == (off_t)-1) {
        /* our seek failed, return an error */
        scr_err("Failed to seek to byte %lu in %s @ %s:%d",
                pos, files[i], __FILE__, __LINE__
        );
        return SCR_FAILURE;
      }
    }
  }

  /* if count is bigger than all of our file data, pad with zeros on the end */
  if (nread < count) {
    memset(buf + nread, 0, count - nread);
  }

  return SCR_SUCCESS;
}

/* write to an array of open files with known filesizes treating them as one single large file */
int scr_write_pad_n(int n, char** files, int* fds,
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
        scr_err("Failed to seek to byte %lu in %s @ %s:%d",
                pos, files[i], __FILE__, __LINE__
        );
        return SCR_FAILURE;
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
    if (scr_write_attempt(files[i], fds[i], buf + nwrite, num_to_write) != num_to_write) {
      /* our write failed, return an error */
      return SCR_FAILURE;
    }
    nwrite += num_to_write;

    /* advance to next file and seek to byte 0 */
    i++;
    if (i < n) {
      pos = 0;
      if (lseek(fds[i], pos, SEEK_SET) == (off_t)-1) {
        /* our seek failed, return an error */
        scr_err("Failed to seek to byte %lu in %s @ %s:%d",
                pos, files[i], __FILE__, __LINE__
        );
        return SCR_FAILURE;
      }
    }
  }

  /* if count is bigger than all of our file data, just throw the data away */
  if (nwrite < count) {
    /* NOTHING TO DO */
  }

  return SCR_SUCCESS;
}

/* given a filename, return number of bytes in file */
unsigned long scr_file_size(const char* file)
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
int scr_file_exists(const char* file)
{
  /* check whether the file exists */
  if (access(file, F_OK) < 0) {
    /* TODO: would be nice to print a message here, but
     *       functions calling this expect it to be quiet
    scr_dbg(2, "File does not exist: %s errno=%d %m @ file %s:%d",
            file, errno, __FILE__, __LINE__
    );
    */
    return SCR_FAILURE;
  }
  return SCR_SUCCESS;
}

/* tests whether the file or directory is readable */
int scr_file_is_readable(const char* file)
{
  /* check whether the file can be read */
  if (access(file, R_OK) < 0) {
    /* TODO: would be nice to print a message here, but
     *       functions calling this expect it to be quiet
    scr_dbg(2, "File not readable: %s errno=%d %m @ file %s:%d",
            file, errno, __FILE__, __LINE__
    );
    */
    return SCR_FAILURE;
  }
  return SCR_SUCCESS;
}

/* tests whether the file or directory is writeable */
int scr_file_is_writeable(const char* file)
{
  /* check whether the file can be read */
  if (access(file, W_OK) < 0) {
    /* TODO: would be nice to print a message here, but
     *       functions calling this expect it to be quiet
    scr_dbg(2, "File not writeable: %s errno=%d %m @ file %s:%d",
            file, errno, __FILE__, __LINE__
    );
    */
    return SCR_FAILURE;
  }
  return SCR_SUCCESS;
}

/* delete a file */
int scr_file_unlink(const char* file)
{
  if (unlink(file) != 0) {
    scr_dbg(2, "Failed to delete file: %s errno=%d %m @ file %s:%d",
            file, errno, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }
  return SCR_SUCCESS;
}

/* opens, reads, and computes the crc32 value for the given filename */
int scr_crc32(const char* filename, uLong* crc)
{
  /* check that we got a variable to write our answer to */
  if (crc == NULL) {
    return SCR_FAILURE;
  }

  /* initialize our crc value */
  *crc = crc32(0L, Z_NULL, 0);

  /* open the file for reading */
  int fd = scr_open(filename, O_RDONLY);
  if (fd < 0) {
    scr_dbg(1, "Failed to open file to compute crc: %s errno=%d @ file %s:%d",
            filename, errno, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* read the file data in and compute its crc32 */
  int nread = 0;
  unsigned long buffer_size = 1024*1024;
  char buf[buffer_size];
  do {
    nread = scr_read(filename, fd, buf, buffer_size);
    if (nread > 0) {
      *crc = crc32(*crc, (const Bytef*) buf, (uInt) nread);
    }
  } while (nread == buffer_size);

  /* if we got an error, don't print anything and bailout */
  if (nread < 0) {
    scr_dbg(1, "Error while reading file to compute crc: %s @ file %s:%d",
            filename, __FILE__, __LINE__
    );
    close(fd);
    return SCR_FAILURE;
  }

  /* close the file */
  scr_close(filename, fd);

  return SCR_SUCCESS;
}

/*
=========================================
Directory functions
=========================================
*/

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
        scr_free(&dircopy);
        scr_free(&path);
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
  scr_free(&dircopy);
  scr_free(&path);
  return rc;
}

/* remove directory */
int scr_rmdir(const char* dir)
{
  /* delete directory */
  int rc = rmdir(dir);
  if (rc < 0) {
    /* whoops, something failed when we tried to delete our directory */
    scr_err("Error deleting directory: %s (rmdir returned %d %m) @ %s:%d",
      dir, rc, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }
  return SCR_SUCCESS;
}

/* write current working directory to buf */
int scr_getcwd(char* buf, size_t size)
{
  int rc = SCR_SUCCESS;
  if (getcwd(buf, size) == NULL) {
    scr_abort(-1, "Problem reading current working directory (getcwd() errno=%d %m) @ %s:%d",
              errno, __FILE__, __LINE__
    );
    rc = SCR_FAILURE;
  }
  return rc;
}

/* split path and filename from fullpath on the rightmost '/'
 * assumes all filename if no '/' is found */
int scr_path_split(const char* file, char* path, char* filename)
{
  /* dirname and basename may modify their arguments, so we need to make a copy. */
  char* pcopy = strdup(file);
  char* ncopy = strdup(file);

  strcpy(path,     dirname(pcopy));
  strcpy(filename, basename(ncopy));

  scr_free(&ncopy);
  scr_free(&pcopy);
  return SCR_SUCCESS;
}

/* combine path and file into a fullpath in buf */
int scr_path_build(char* buf, size_t size, const char* path, const char* file)
{
  int nwrite = 0;
  if ((path == NULL || strcmp(path, "") == 0) && (file == NULL || strcmp(file, "") == 0)) {
    /* empty path and file, just write an empty string to file */
    nwrite = 1;
    if (size > 0) {
      *buf = '\0';
    }
  } else if (path == NULL || strcmp(path, "") == 0) {
    /* empty path, just return file */
    nwrite = snprintf(buf, size, "%s", file);
  } else if (file == NULL || strcmp(file, "") == 0) {
    /* empty file, just return path */
    nwrite = snprintf(buf, size, "%s", path);
  } else {
    /* concatenate path and file */
    nwrite = snprintf(buf, size, "%s/%s", path, file);
  }

  /* return success or failure depending on whether we fit everything into the buffer */
  if (nwrite >= size) {
    scr_err("Output buffer too small to concatenate path and file name (have %d bytes, need %d bytes) @ %s:%d",
            size, nwrite, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }
  return SCR_SUCCESS;
}

/* returns the number of components (number of slashes + 1) */
int scr_path_length(const char* str, int* length)
{
  /* check that we got a length variable */
  if (length == NULL) {
    return SCR_FAILURE;
  }

  /* return 0 for a NULL pointer */
  if (str == NULL) {
    *length = 0;
    return SCR_SUCCESS;
  }

  /* otherwise count the number of slashes */
  size_t i;
  int count = 0;
  for (i = 0; str[i] != '\0'; i++) {
    if (str[i] == '/') {
      count++;
    }
  }

  /* number of components is number of slashes + 1 */
  *length = count + 1;
  return SCR_SUCCESS;
}

/* returns the substring starting at the specified component index
 * (0 through scr_path_length-1) and running for length components */
int scr_path_slice(
  const char* str,
  int start,
  int length,
  char* substr,
  size_t substrlen)
{
  /* check that parameters are ok */
  if (start < 0 || length < 0 || substr == NULL) {
    return SCR_FAILURE;
  }

  /* nothing to copy for a NULL string */
  if (str == NULL) {
    return SCR_FAILURE;
  }

  /* if start is 0, then first will be i=0 character
   * regardless whether it is a slash or not */
  int count = 0;
  size_t i = 0;
  while (str[i] != '\0' && count < start) {
    if (str[i] == '/') {
      count++;
    }
    i++;
  }

  /* at this point, i is sitting one char one past the starting '/'
   * or it is on the terminating NUL */
  size_t first = i;

  /* run until we've seen length more slashes or hit the end
   * of the string */
  while (str[i] != '\0' && count < start + length) {
    if (str[i] == '/') {
      count++;
    }
    i++;
  }

  /* determine the number of characters to copy from str, assume
   * we'll copy in the empty string until we figure out otherwise */
  size_t len = 0;
  if (count < start + length) {
    /* we ended because we hit the end of the string, we didn't
     * find the last slash, so just copy all bytes up to end of string */
    len = i - first;
  } else if (i > first + 1) {
    /* in this case, we counted the correct number of slashes
     * and our index is one char past the last slash */
    len = i - first - 1;
  }

  /* copy contents into substr and return */
  if (len < substrlen) {
    const char* ptr = &str[first];
    if (len > 0) {
      memcpy(substr, ptr, len);
    }
    substr[len] = '\0';
    return SCR_SUCCESS;
  }

  return SCR_FAILURE;
}

/* given a file or directory name, construct the full path by prepending
 * the current working directory if needed */
int scr_path_absolute(char* buf, size_t size, const char* file)
{
  /* check that we have valid buffers and a non-empty string */
  if (buf == NULL || file == NULL || strcmp(file, "") == 0) {
    return SCR_FAILURE;
  }

  int rc = SCR_SUCCESS;

  /* get an absolute path in tmp_buf */
  char tmp_buf[SCR_MAX_FILENAME];
  if (file[0] == '/') {
    /* the filename is already an absolute path, so just make a copy */
    size_t file_len = strlen(file) + 1;
    if (file_len <= sizeof(tmp_buf)) {
      strcpy(tmp_buf, file);
    } else {
      /* tmp buffer is too small */
      rc = SCR_FAILURE;
    }
  } else {
    /* the path is not absolute, so prepend the current working directory */
    char cwd[SCR_MAX_FILENAME];
    if (scr_getcwd(cwd, sizeof(cwd)) == SCR_SUCCESS) {
      if (scr_path_build(tmp_buf, sizeof(tmp_buf), cwd, file) != SCR_SUCCESS) {
        /* problem concatenating cwd with file */
        rc = SCR_FAILURE;
      }
    } else {
      /* problem acquiring current working directory */
      rc = SCR_FAILURE;
    }
  }

  /* now we have an absolute path in tmp_buf,
   * return a simplified version to caller */
  if (rc == SCR_SUCCESS) {
    if (scr_path_resolve(tmp_buf, buf, size) != SCR_SUCCESS) {
      rc = SCR_FAILURE;
    }
  }

  return rc;
}

/* TODO: handle symlinks */
/* remove double slashes, trailing slash, '.', and '..' */
int scr_path_resolve(const char* str, char* newstr, size_t newstrlen)
{
  /* check that we got valid input parameters */
  if (str == NULL || newstr == NULL) {
    return SCR_FAILURE;
  }

  /* TODO: what to do with things like lustre:/my/file? */
  /* require an absolute path, code below assumes string starts with a '/' */
  if (str[0] != '/') {
    return SCR_FAILURE;
  }

  /* scan from left char by char and copy into newstr */
  size_t src = 0;
  size_t dst = 0;
  while (str[src] != '\0') {
    /* make sure we don't overrun the length of the new string */
    if (dst >= newstrlen) {
      /* TODO: intermediate representation could be too long but final
       * representation may still fit */
      /* not enough room to copy path into newstr */
      return SCR_FAILURE;
    }

    /* copy character from path to newstr */
    char current = str[src];
    newstr[dst] = current;

    /* while last char is slash */
    while (str[src] == '/') {
      /* skip ahead until next char is not slash, this removes
       * consecutive slashes from path */
      while (str[src+1] == '/') {
        src++;
      }

      /* if next character is '.', look for '.' and '..' */
      char one_ahead = str[src+1];
      if (one_ahead == '.') {
        char two_ahead = str[src+2];
        if (two_ahead == '/' || two_ahead == '\0') {
          /* next char is '/' or '\0', we have ref to current
           * directory as in "foo/./" or "foo/." so skip two chars */
          src += 2;
        } else if (two_ahead == '.') {
          char three_ahead = str[src+3];
          if (three_ahead == '/' || three_ahead == '\0') {
            /* next char is '/' or '\0', pop off one component since
             * we found "foo/../" or foo/.." */
            src += 3;

            /* pop off last component from newstr */
            if (dst > 0) {
              /* remove the last slash we just added */
              dst--;

              /* now pop characters until we hit another slash */
              while (dst > 0 && newstr[dst] != '/') {
                dst--;
              }
            } else {
              /* we've tried to pop too far, as in "/.." */
              return SCR_FAILURE;
            }
          } else {
            /* '/..' is followed by some character other than a '/' or '\0' */
            break;
          }
        } else {
          /* '/.' is followed by some character other than a '.', '/', or '\0' */
          break;
        }
      } else {
        /* slash is followed by some character other than a '.' */
        break;
      }
    }

    /* advance new string pointer */
    dst++;

    /* advance src pointer */
    if (str[src] != '\0') {
      src++;
    }
  }

  /* remove any trailing slash */
  if (dst > 1 && newstr[dst-1] == '/') {
    dst--;
  }

  /* terminate our new path */
  if (dst < newstrlen) {
    newstr[dst] = '\0';
  } else {
    return SCR_FAILURE;
  }

  return SCR_SUCCESS;
}

/* returns relative path pointing to dst starting from src */
int scr_path_relative(const char* src, const char* dst, char* path, size_t path_size)
{
  /* check input parameters */
  if (src == NULL || dst == NULL || path == NULL) {
    return SCR_FAILURE;
  }

  /* resolve source path */
  char src_resolve[SCR_MAX_FILENAME];
  if (scr_path_resolve(src, src_resolve, sizeof(src_resolve)) != SCR_SUCCESS) {
    return SCR_FAILURE;
  }

  /* resolve destination path */
  char dst_resolve[SCR_MAX_FILENAME];
  if (scr_path_resolve(dst, dst_resolve, sizeof(dst_resolve)) != SCR_SUCCESS) {
    return SCR_FAILURE;
  }

  /* TODO: compute relative paths for arbitrary src and dst paths,
   * for now we just support this if dst is child of src */

  /* get number of chars in src */
  size_t src_size = strlen(src);

  /* ensure that dst is child of src */
  if (strncmp(src_resolve, dst_resolve, src_size) != 0) {
    return SCR_FAILURE;
  }

  /* get number of components in src directory */
  int src_components;
  if (scr_path_length(src_resolve, &src_components) != SCR_SUCCESS) {
    return SCR_FAILURE;
  }

  /* get number of components in dst directory */
  int dst_components;
  if (scr_path_length(dst_resolve, &dst_components) != SCR_SUCCESS) {
    return SCR_FAILURE;
  }

  /* check that number of dst components is greater than or equal to src */
  if (dst_components < src_components) {
    return SCR_FAILURE;
  }

  /* now strip src components from dst */
  int start = src_components;
  int remaining = dst_components - src_components;
  if (scr_path_slice(dst_resolve, start, remaining, path, path_size) != SCR_SUCCESS) {
    return SCR_FAILURE;
  }

  return SCR_SUCCESS;
}

/*
=========================================
File Copy Functions
=========================================
*/

/* TODO: could perhaps use O_DIRECT here as an optimization */
/* TODO: could apply compression/decompression here */
/* copy src_file (full path) to dest_path and return new full path in dest_file */
int scr_copy_to(const char* src, const char* dst_dir, unsigned long buf_size, char* dst, size_t dst_size, uLong* crc)
{
  /* check that we got something for a source file */
  if (src == NULL || strcmp(src, "") == 0) {
    scr_err("Invalid source file @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* check that we got something for a destination directory */
  if (dst_dir == NULL || strcmp(dst_dir, "") == 0) {
    scr_err("Invalid destination directory @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* check that we got a pointer to a destination buffer */
  if (dst == NULL) {
    scr_err("Invalid buffer for destination file name @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* split src_file into path and filename */
  char path[SCR_MAX_FILENAME];
  char name[SCR_MAX_FILENAME];
  scr_path_split(src, path, name);

  /* create dest_file using dest_path and filename */
  if (scr_path_build(dst, dst_size, dst_dir, name) != SCR_SUCCESS) {
    scr_err("Failed to build full filename for destination file @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* open src_file for reading */
  int fd_src = scr_open(src, O_RDONLY);
  if (fd_src < 0) {
    scr_err("Opening file to copy: scr_open(%s) errno=%d %m @ %s:%d",
            src, errno, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* open dest_file for writing */
  int fd_dst = scr_open(dst, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
  if (fd_dst < 0) {
    scr_err("Opening file for writing: scr_open(%s) errno=%d %m @ %s:%d",
            dst, errno, __FILE__, __LINE__
    );
    scr_close(src, fd_src);
    return SCR_FAILURE;
  }

  /* TODO:
  posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED | POSIX_FADV_SEQUENTIAL)
  that tells the kernel that you don't ever need the pages
  from the file again, and it won't bother keeping them in the page cache.
  */
  posix_fadvise(fd_src, 0, 0, POSIX_FADV_DONTNEED | POSIX_FADV_SEQUENTIAL);
  posix_fadvise(fd_dst, 0, 0, POSIX_FADV_DONTNEED | POSIX_FADV_SEQUENTIAL);

  /* allocate buffer to read in file chunks */
  char* buf = (char*) malloc(buf_size);
  if (buf == NULL) {
    scr_err("Allocating memory: malloc(%llu) errno=%d %m @ %s:%d",
            buf_size, errno, __FILE__, __LINE__
    );
    scr_close(dst, fd_dst);
    scr_close(src, fd_src);
    return SCR_FAILURE;
  }

  /* initialize crc values */
  if (crc != NULL) {
    *crc = crc32(0L, Z_NULL, 0);
  }

  int rc = SCR_SUCCESS;

  /* write chunks */
  int copying = 1;
  while (copying) {
    /* attempt to read buf_size bytes from file */
    int nread = scr_read_attempt(src, fd_src, buf, buf_size);

    /* if we read some bytes, write them out */
    if (nread > 0) {
      /* optionally compute crc value as we go */
      if (crc != NULL) {
        *crc = crc32(*crc, (const Bytef*) buf, (uInt) nread);
      }

      /* write our nread bytes out */
      int nwrite = scr_write_attempt(dst, fd_dst, buf, nread);

      /* check for a write error or a short write */
      if (nwrite != nread) {
        /* write had a problem, stop copying and return an error */
        copying = 0;
        rc = SCR_FAILURE;
      }
    }

    /* assume a short read means we hit the end of the file */
    if (nread < buf_size) {
      copying = 0;
    }

    /* check for a read error, stop copying and return an error */
    if (nread < 0) {
      /* read had a problem, stop copying and return an error */
      copying = 0;
      rc = SCR_FAILURE;
    }
  }

  /* free buffer */
  scr_free(&buf);

  /* close source and destination files */
  if (scr_close(dst, fd_dst) != SCR_SUCCESS) {
    rc = SCR_FAILURE;
  }
  if (scr_close(src, fd_src) != SCR_SUCCESS) {
    rc = SCR_FAILURE;
  }

  /* unlink the file if the copy failed */
  if (rc != SCR_SUCCESS) {
    unlink(dst);
  }

  return rc;
}
