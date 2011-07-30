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

/* fsync and close file */
int scr_close(const char* file, int fd)
{
  /* fsync first */
  fsync(fd);

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
  if (flock(fd, LOCK_EX) != 0) {
    scr_err("Failed to acquire file lock on %s: flock(%d, %d) errno=%d %m @ %s:%d",
            file, fd, LOCK_EX, errno, __FILE__, __LINE__
    );
    /* we opened the file ok, but the lock failed, close the file and return -1 */
    close(fd);
    return -1;
  }

  /* return the opened file descriptor */
  return fd;
}

/* unlocks the specified file descriptor and then closes the file */
int scr_close_with_unlock(const char* file, int fd)
{
  /* release the file lock */
  if (flock(fd, LOCK_UN) != 0) {
    scr_err("Failed to release file lock on %s: flock(%d, %d) errno=%d %m @ %s:%d",
            file, fd, LOCK_UN, errno, __FILE__, __LINE__
    );
    return SCR_FAILURE;
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

/* tests whether the file exists */
int scr_file_exists(const char* file)
{
  /* to test, check whether the file can be read */
  if (access(file, R_OK) < 0) {
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
    scr_dbg(1, "Failed to open file to compute crc: %s @ file %s:%d",
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

/* combine path and file into a fullpath in buf */
int scr_build_path (char* buf, size_t size, const char* path, const char* file)
{
  int nwrite = 0;
  if ((path == NULL || strcmp(path, "") == 0) && (file == NULL || strcmp(file, "") == 0)) {
    /* empty path and file, just write an empty string to file */
    nwrite = snprintf(buf, size, "");
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

/* given a file or directory name, construct the full path by prepending
 * the current working directory if needed */
int scr_build_absolute_path(char* buf, size_t size, const char* file)
{
  /* check that we have valid buffers and a non-empty string */
  if (buf == NULL || file == NULL || strcmp(file, "") == 0) {
    return SCR_FAILURE;
  }

  int rc = SCR_SUCCESS;
  if (file[0] == '/') {
    /* the filename is already an absolute path, so just make a copy */
    int n = strlen(file);
    if ((n+1) <= size) {
      strcpy(buf, file);
    } else {
      /* output buffer is too small */
      rc = SCR_FAILURE;
    }
  } else {
    /* the path is not absolute, so prepend the current working directory */
    char cwd[SCR_MAX_FILENAME];
    if (scr_getcwd(cwd, sizeof(cwd)) == SCR_SUCCESS) {
      int n = snprintf(buf, size, "%s/%s", cwd, file);
      if (n < 0 || (n+1) > size) {
        /* problem writing to buf */
        rc = SCR_FAILURE;
      }
    } else {
      /* problem acquiring current working directory */
      rc = SCR_FAILURE;
    }
  }

  return rc;
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
  scr_split_path(src, path, name);

  /* create dest_file using dest_path and filename */
  if (scr_build_path(dst, dst_size, dst_dir, name) != SCR_SUCCESS) {
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
  if (buf != NULL) {
    free(buf);
    buf = NULL;
  }

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

/*
=========================================
File compression functions
=========================================
*/

#define SCR_FILE_MAGIC                (0x951fc3f5)
#define SCR_FILE_TYPE_COMPRESSED      (2)
#define SCR_FILE_VERSION_COMPRESSED_1 (1)

#define SCR_FILE_COMPRESSED_HEADER_SIZE (44)
/* (4) uint32_t magic number
 * (2) uint16_t type
 * (2) uint16_t type version
 *
 * (8) uint64_t header size (this value includes the block table)
 * (8) uint64_t file size
 * (8) uint64_t block size
 * (8) uint64_t number of blocks
 *
 * <variable length block table> (excluded from SCR_FILE_COMPRESSED_HEADER_SIZE constant)
 *
 * (4) uint32_t header crc (from first byte of magic number to last byte of block table) */

#if 0
sierra0{x3user}81: pwd
/p/lscratchb/x3user/pf3d/N091204/doub_3d03

sierra0{x3user}80: ls -lt
total 1209740
-rw-r-x--- 1 x3user f3d 301520706 Jun 19 23:51 t2_18.9323ps_0162.pdb.copy2.bz2
-rw-r-x--- 1 x3user f3d 333150982 Jun 19 23:47 t2_18.9323ps_0162.pdb.copy.gz
-rw-rws--- 1   3230 f3d 604070931 Mar 24 15:41 t2_18.9323ps_0162.pdb

#endif

/* compress the specified file using blocks of size block_size and store as file_dst */
int scr_compress_in_place(const char* file_src, const char* file_dst, unsigned long block_size, int level)
{
  /* set compression level 0=none, 9=max */
//  int compression_level = Z_DEFAULT_COMPRESSION;
  int compression_level = level;

  /* check that we have valid file names */
  if (file_src == NULL || file_dst == NULL) {
    scr_err("NULL filename @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* check that the source file exists and that we can read and write to it */
  if (access(file_src, F_OK | R_OK | W_OK) != 0) {
    scr_err("File %s does not exist or does not have read/write permission @ %s:%d",
            file_src, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* get the page size (used to align buffers) */
  int page_size = getpagesize();
  if (page_size <= 0) {
    scr_err("Call to getpagesize failed when compressing %s @ %s:%d",
            file_src, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* get the size of the file */
  unsigned long filesize = scr_filesize(file_src);

  /* determine the number of blocks that we'll write */
  unsigned long num_blocks = filesize / block_size;
  if (num_blocks * block_size < filesize) {
    num_blocks++;
  }

  /* compute the size of the header */
  unsigned long header_size = SCR_FILE_COMPRESSED_HEADER_SIZE + num_blocks * (2 * sizeof(uint64_t) + 2 * sizeof(uint32_t));

  /* allocate buffer to hold file header */
  void* header = malloc(header_size);
  if (header == NULL) {
    scr_err("Allocating header buffer when compressing %s: malloc(%ld) errno=%d %m @ %s:%d",
            file_src, header_size, errno, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* allocate buffer to read data into */
  void* buf_src = scr_align_malloc((size_t) block_size, page_size);
  if (buf_src == NULL) {
    scr_err("Allocating source buffer when compressing %s: malloc(%ld) errno=%d %m @ %s:%d",
            file_src, block_size, errno, __FILE__, __LINE__
    );
    free(header);
    return SCR_FAILURE;
  }

  /* allocate buffer to write compressed data into */
  void* buf_dst = scr_align_malloc((size_t) block_size, page_size);
  if (buf_dst == NULL) {
    scr_err("Allocating compress buffer when compressing %s: malloc(%ld) errno=%d %m @ %s:%d",
            file_src, block_size, errno, __FILE__, __LINE__
    );
    free(buf_src);
    free(header);
    return SCR_FAILURE;
  }

  /* open original file for read/write access */
  int fd_src = scr_open(file_src, O_RDWR);
  if (fd_src < 0) {
    scr_err("Opening file: %s errno=%d %m @ %s:%d",
            file_src, errno, __FILE__, __LINE__
    );
    free(buf_dst);
    free(buf_src);
    free(header);
    return SCR_FAILURE;
  }

  /* these pointers will track our location within the file,
   * we must make sure that we never overrun the original data when compressing */
  off_t pos_src = 0;
  off_t pos_dst = 0;

  int rc = SCR_SUCCESS;

  /* write the SCR file magic number, file type, and version number */
  size_t header_offset = 0;
  scr_pack_uint32_t(header, header_size, &header_offset, (uint32_t) SCR_FILE_MAGIC);
  scr_pack_uint16_t(header, header_size, &header_offset, (uint16_t) SCR_FILE_TYPE_COMPRESSED);
  scr_pack_uint16_t(header, header_size, &header_offset, (uint16_t) SCR_FILE_VERSION_COMPRESSED_1);

  /* write the size of the header, the original file size, block size, and number of blocks */
  scr_pack_uint64_t(header, header_size, &header_offset, (uint64_t) header_size);
  scr_pack_uint64_t(header, header_size, &header_offset, (uint64_t) filesize);
  scr_pack_uint64_t(header, header_size, &header_offset, (uint64_t) block_size);
  scr_pack_uint64_t(header, header_size, &header_offset, (uint64_t) num_blocks);

  /* seek to end of header */
  pos_dst = header_size;

  /* read block from source file, compress, write to destination file */
  unsigned long block_offset_cmp = 0;
  int compressing = 1;
  while (compressing && rc == SCR_SUCCESS) {
    /* seek to current location for reading */
    if (lseek(fd_src, (off_t) pos_src, SEEK_SET) == (off_t) -1) {
      scr_err("Seek to read position failed in %s @ %s:%d",
              file_src, __FILE__, __LINE__
      );
      rc = SCR_FAILURE;
    }

    /* read a block in from the file */
    ssize_t nread = scr_read(file_src, fd_src, buf_src, block_size);

    /* compress data and write it to file */
    if (nread > 0) {
      /* update our read position */
      pos_src += nread;

      /* record size of compressed block,
       * crc of compressed block, and crc of original block */
      unsigned long block_size_cmp = 0;
      uLong crc_cmp  = crc32(0L, Z_NULL, 0);
      uLong crc_orig = crc32(0L, Z_NULL, 0);

      /* compute crc for block */
      crc_orig = crc32(crc_orig, (const Bytef*) buf_src, (uInt) nread);

      /* initialize compression stream */
      z_stream strm;
      strm.zalloc = Z_NULL;
      strm.zfree  = Z_NULL;
      strm.opaque = Z_NULL;
      int ret = deflateInit(&strm, compression_level);
      if (ret != Z_OK) {
        rc = SCR_FAILURE;
      }

      /* compress data */
      strm.avail_in = nread;
      strm.next_in  = buf_src;
      do {
        size_t have = 0;
        strm.avail_out = block_size;
        strm.next_out  = buf_dst;
        do {
          ret = deflate(&strm, Z_FINISH);
          if (ret == Z_OK || ret == Z_BUF_ERROR || ret == Z_STREAM_END) {
            /* compute number of bytes written by this call to deflate */
            have = block_size - strm.avail_out;
          } else {
            /* hit an error of some sort */
            scr_err("Error during compression in %s (ret=%d) @ %s:%d",
                    file_src, ret, __FILE__, __LINE__
            );
            rc = SCR_FAILURE;
          }
        } while (strm.avail_in !=0 && strm.avail_out != 0 && ret != Z_BUF_ERROR && rc == SCR_SUCCESS);

        /* TODO: if compression produces very small blocks, this will be inefficient,
         * would be better to use a buffered write like fwrite here */

        /* write data */
        if (have > 0 && rc == SCR_SUCCESS) {
          /* compute crc of compressed block */
          crc_cmp = crc32(crc_cmp, (const Bytef*) buf_dst, (uInt) have);

          /* check that we won't overrun our read position when we write out this data */
          off_t pos_end = pos_dst + have;
          if (pos_end > pos_src && pos_src != filesize) {
            /* TODO: unwind what compression we have done if any,
             * for now we just make this a fatal error */
            scr_err("Failed to compress file in place %s @ %s:%d",
                    file_src, __FILE__, __LINE__
            );
            rc = SCR_FAILURE;
          }

          /* seek to correct location in file to write data */
          if (lseek(fd_src, (off_t) pos_dst, SEEK_SET) == (off_t) -1) {
            scr_err("Seek to write position failed in %s @ %s:%d",
                    file_src, __FILE__, __LINE__
            );
            rc = SCR_FAILURE;
          }

          /* write compressed data to file */
          ssize_t nwrite = scr_write(file_src, fd_src, buf_dst, have);
          if (nwrite != have) {
            scr_err("Error writing compressed file %s @ %s:%d",
                    file_src, __FILE__, __LINE__
            );
            rc = SCR_FAILURE;
          }

          /* update our write position */
          if (nwrite > 0) {
            pos_dst += nwrite;
          }

          /* add count to our total compressed block size */
          block_size_cmp += have;
        }
      } while ((ret == Z_OK || ret == Z_BUF_ERROR) && rc == SCR_SUCCESS); /* strm.avail_out == 0 */

      /* check that we compressed the entire block */
      if (strm.avail_in != 0 || ret != Z_STREAM_END) {
        scr_err("Failed to compress file %s @ %s:%d",
                file_src, __FILE__, __LINE__
        );
        rc = SCR_FAILURE;
      }

      /* finalize the compression stream */
      deflateEnd(&strm);

      /* add entry for block in header: length, crc cmp, crc orig */
      scr_pack_uint64_t(header, header_size, &header_offset, (uint64_t) block_offset_cmp);
      scr_pack_uint64_t(header, header_size, &header_offset, (uint64_t) block_size_cmp);
      scr_pack_uint32_t(header, header_size, &header_offset, (uint32_t) crc_cmp);
      scr_pack_uint32_t(header, header_size, &header_offset, (uint32_t) crc_orig);
      block_offset_cmp += block_size_cmp;
    }

    /* check whether we've read all of the input file */
    if (nread < block_size) {
      compressing = 0;
    }
  }

  /* compute crc over length of the header and write it to header */
  uLong crc = crc32(0L, Z_NULL, 0);
  crc = crc32(crc, (const Bytef*) header, (uInt) header_offset);
  scr_pack_uint32_t(header, header_size, &header_offset, (uint32_t) crc);

  /* seek to beginning of file */
  if (lseek(fd_src, (off_t) 0, SEEK_SET) == (off_t) -1) {
    scr_err("Seek to beginning of header failed in %s @ %s:%d",
            file_src, __FILE__, __LINE__
    );
    rc = SCR_FAILURE;
  }

  /* write header to file */
  ssize_t nwrite_header = scr_write(file_src, fd_src, header, header_size);
  if (nwrite_header != header_size) {
    scr_err("Failed to write header to file %s @ %s:%d",
            file_src, __FILE__, __LINE__
    );
    rc = SCR_FAILURE;
  }

  /* close file */
  scr_close(file_src, fd_src);

  /* truncate file */
  truncate(file_src, pos_dst);

  /* rename file */
  rename(file_src, file_dst);

  /* free our buffers */
  if (buf_dst != NULL) {
    scr_align_free(buf_dst);
    buf_dst = NULL;
  }
  if (buf_src != NULL) {
    scr_align_free(buf_src);
    buf_src = NULL;
  }
  if (header != NULL) {
    free(header);
    header = NULL;
  }

  return rc;
}

/* compress the specified file using blocks of size block_size and store as file_dst */
int scr_compress(const char* file_src, const char* file_dst, unsigned long block_size, int level)
{
  /* set compression level 0=none, 9=max */
//  int compression_level = Z_DEFAULT_COMPRESSION;
  int compression_level = level;

  /* check that we have valid file names */
  if (file_src == NULL || file_dst == NULL) {
    scr_err("NULL filename @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* check that the source file exists and that we can read it */
  if (access(file_src, F_OK | R_OK) != 0) {
    scr_err("File %s does not exist or does not have read permission @ %s:%d",
            file_src, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* get the page size (used to align buffers) */
  int page_size = getpagesize();
  if (page_size <= 0) {
    scr_err("Call to getpagesize failed when compressing %s @ %s:%d",
            file_src, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* get the size of the file */
  unsigned long filesize = scr_filesize(file_src);

  /* determine the number of blocks that we'll write */
  unsigned long num_blocks = filesize / block_size;
  if (num_blocks * block_size < filesize) {
    num_blocks++;
  }

  /* compute the size of the header */
  unsigned long header_size = SCR_FILE_COMPRESSED_HEADER_SIZE + num_blocks * (2 * sizeof(uint64_t) + 2 * sizeof(uint32_t));

  /* allocate buffer to hold file header */
  void* header = malloc(header_size);
  if (header == NULL) {
    scr_err("Allocating header buffer when compressing %s: malloc(%ld) errno=%d %m @ %s:%d",
            file_src, header_size, errno, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* allocate buffer to read data into */
  void* buf_src = scr_align_malloc((size_t) block_size, page_size);
  if (buf_src == NULL) {
    scr_err("Allocating source buffer when compressing %s: malloc(%ld) errno=%d %m @ %s:%d",
            file_src, block_size, errno, __FILE__, __LINE__
    );
    free(header);
    return SCR_FAILURE;
  }

  /* allocate buffer to write compressed data into */
  void* buf_dst = scr_align_malloc((size_t) block_size, page_size);
  if (buf_dst == NULL) {
    scr_err("Allocating compress buffer when compressing %s: malloc(%ld) errno=%d %m @ %s:%d",
            file_src, block_size, errno, __FILE__, __LINE__
    );
    free(buf_src);
    free(header);
    return SCR_FAILURE;
  }

  /* open original file */
  int fd_src = scr_open(file_src, O_RDONLY);
  if (fd_src < 0) {
    scr_err("Opening file for reading: %s errno=%d %m @ %s:%d",
            file_src, errno, __FILE__, __LINE__
    );
    free(buf_dst);
    free(buf_src);
    free(header);
    return SCR_FAILURE;
  }

  /* open compressed file for writing */
  int fd_dst = scr_open(file_dst, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
  if (fd_dst < 0) {
    scr_err("Opening file for writing: %s errno=%d %m @ %s:%d",
            file_dst, errno, __FILE__, __LINE__
    );
    scr_close(file_src, fd_src);
    free(buf_dst);
    free(buf_src);
    free(header);
    return SCR_FAILURE;
  }

  int rc = SCR_SUCCESS;

  /* write the SCR file magic number, file type, and version number */
  size_t header_offset = 0;
  scr_pack_uint32_t(header, header_size, &header_offset, (uint32_t) SCR_FILE_MAGIC);
  scr_pack_uint16_t(header, header_size, &header_offset, (uint16_t) SCR_FILE_TYPE_COMPRESSED);
  scr_pack_uint16_t(header, header_size, &header_offset, (uint16_t) SCR_FILE_VERSION_COMPRESSED_1);

  /* write the size of the header, the original file size, block size, and number of blocks */
  scr_pack_uint64_t(header, header_size, &header_offset, (uint64_t) header_size);
  scr_pack_uint64_t(header, header_size, &header_offset, (uint64_t) filesize);
  scr_pack_uint64_t(header, header_size, &header_offset, (uint64_t) block_size);
  scr_pack_uint64_t(header, header_size, &header_offset, (uint64_t) num_blocks);

  /* seek to end of header */
  if (lseek(fd_dst, (off_t) header_size, SEEK_SET) == (off_t) -1) {
    scr_err("Seek to end of header failed in %s @ %s:%d",
            file_dst, __FILE__, __LINE__
    );
    rc = SCR_FAILURE;
  }

  /* read block from source file, compress, write to destination file */
  unsigned long block_offset_cmp = 0;
  int compressing = 1;
  while (compressing && rc == SCR_SUCCESS) {
    /* read a block in from the file */
    ssize_t nread = scr_read(file_src, fd_src, buf_src, block_size);

    /* compress data and write it to file */
    if (nread > 0) {
      /* record size of compressed block,
       * crc of compressed block, and crc of original block */
      unsigned long block_size_cmp = 0;
      uLong crc_cmp  = crc32(0L, Z_NULL, 0);
      uLong crc_orig = crc32(0L, Z_NULL, 0);

      /* compute crc for block */
      crc_orig = crc32(crc_orig, (const Bytef*) buf_src, (uInt) nread);

      /* initialize compression stream */
      z_stream strm;
      strm.zalloc = Z_NULL;
      strm.zfree  = Z_NULL;
      strm.opaque = Z_NULL;
      int ret = deflateInit(&strm, compression_level);
      if (ret != Z_OK) {
        rc = SCR_FAILURE;
      }

      /* compress data */
      strm.avail_in = nread;
      strm.next_in  = buf_src;
      do {
        size_t have = 0;
        strm.avail_out = block_size;
        strm.next_out  = buf_dst;
        do {
          ret = deflate(&strm, Z_FINISH);
          if (ret == Z_OK || ret == Z_BUF_ERROR || ret == Z_STREAM_END) {
            /* compute number of bytes written by this call to deflate */
            have = block_size - strm.avail_out;
          } else {
            /* hit an error of some sort */
            scr_err("Error during compression in %s (ret=%d) @ %s:%d",
                    file_src, ret, __FILE__, __LINE__
            );
            rc = SCR_FAILURE;
          }
        } while (strm.avail_in !=0 && strm.avail_out != 0 && ret != Z_BUF_ERROR && rc == SCR_SUCCESS);

        /* TODO: if compression produces very small blocks, this will be inefficient,
         * would be better to use a buffered write like fwrite here */

        /* write data */
        if (have > 0 && rc == SCR_SUCCESS) {
          /* compute crc of compressed block */
          crc_cmp = crc32(crc_cmp, (const Bytef*) buf_dst, (uInt) have);

          /* write compressed data to file */
          ssize_t nwrite = scr_write(file_dst, fd_dst, buf_dst, have);
          if (nwrite != have) {
            scr_err("Error writing compressed file %s @ %s:%d",
                    file_dst, __FILE__, __LINE__
            );
            rc = SCR_FAILURE;
          }

          /* add count to our total compressed block size */
          block_size_cmp += have;
        }
      } while ((ret == Z_OK || ret == Z_BUF_ERROR) && rc == SCR_SUCCESS); /* strm.avail_out == 0 */

      /* check that we compressed the entire block */
      if (strm.avail_in != 0 || ret != Z_STREAM_END) {
        scr_err("Failed to compress file %s @ %s:%d",
                file_src, __FILE__, __LINE__
        );
        rc = SCR_FAILURE;
      }

      /* finalize the compression stream */
      deflateEnd(&strm);

      /* add entry for block in header: length, crc cmp, crc orig */
      scr_pack_uint64_t(header, header_size, &header_offset, (uint64_t) block_offset_cmp);
      scr_pack_uint64_t(header, header_size, &header_offset, (uint64_t) block_size_cmp);
      scr_pack_uint32_t(header, header_size, &header_offset, (uint32_t) crc_cmp);
      scr_pack_uint32_t(header, header_size, &header_offset, (uint32_t) crc_orig);
      block_offset_cmp += block_size_cmp;
    }

    /* check whether we've read all of the input file */
    if (nread < block_size) {
      compressing = 0;
    }
  }

  /* compute crc over length of the header and write it to header */
  uLong crc = crc32(0L, Z_NULL, 0);
  crc = crc32(crc, (const Bytef*) header, (uInt) header_offset);
  scr_pack_uint32_t(header, header_size, &header_offset, (uint32_t) crc);

  /* seek to beginning of file */
  if (lseek(fd_dst, (off_t) 0, SEEK_SET) == (off_t) -1) {
    scr_err("Seek to beginning of header failed in %s @ %s:%d",
            file_dst, __FILE__, __LINE__
    );
    rc = SCR_FAILURE;
  }

  /* write header to file */
  ssize_t nwrite_header = scr_write(file_dst, fd_dst, header, header_size);
  if (nwrite_header != header_size) {
    scr_err("Failed to write header to file %s @ %s:%d",
            file_dst, __FILE__, __LINE__
    );
    rc = SCR_FAILURE;
  }

  /* close files */
  scr_close(file_src, fd_src);
  scr_close(file_dst, fd_dst);

  /* TODO: truncate file */

  /* TODO: rename file */

  /* free our buffers */
  if (buf_dst != NULL) {
    scr_align_free(buf_dst);
    buf_dst = NULL;
  }
  if (buf_src != NULL) {
    scr_align_free(buf_src);
    buf_src = NULL;
  }
  if (header != NULL) {
    free(header);
    header = NULL;
  }

  return rc;
}

/* uncompress the specified file and store as file_dst */
int scr_uncompress_in_place(const char* file_src, const char* file_dst)
{
  /* check that we have valid file names */
  if (file_src == NULL || file_dst == NULL) {
    scr_err("NULL filename @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* check that the source file exists and that we can read and write to it */
  if (access(file_src, F_OK | R_OK | W_OK) != 0) {
    scr_err("File %s does not exist or does not have read/write permission @ %s:%d",
            file_src, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* get the page size (used to align buffers) */
  int page_size = getpagesize();
  if (page_size <= 0) {
    scr_err("Call to getpagesize failed when decompressing %s @ %s:%d",
            file_src, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* open original file for reading and writing */
  int fd_src = scr_open(file_src, O_RDWR);
  if (fd_src < 0) {
    scr_err("Opening file for reading: %s errno=%d %m @ %s:%d",
            file_src, errno, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* read first few bytes of header */
  char buf[SCR_FILE_COMPRESSED_HEADER_SIZE];
  size_t nread = scr_read(file_src, fd_src, buf, sizeof(buf));
  if (nread < sizeof(buf)) {
    scr_err("Failed to read header from file %s @ %s:%d",
            file_src, __FILE__, __LINE__
    );
    scr_close(file_src, fd_src);
    return SCR_FAILURE;
  }

  size_t size = 0;

  /* unpack magic number, the type, and the version */
  uint32_t magic;
  uint16_t type, version;
  scr_unpack_uint32_t(buf, sizeof(buf), &size, &magic);
  scr_unpack_uint16_t(buf, sizeof(buf), &size, &type);
  scr_unpack_uint16_t(buf, sizeof(buf), &size, &version);

  /* check the magic number, the type, and the version */
  if (magic   != SCR_FILE_MAGIC ||
      type    != SCR_FILE_TYPE_COMPRESSED ||
      version != SCR_FILE_VERSION_COMPRESSED_1)
  {
    scr_err("File type does not match values for a compressed file %s @ %s:%d",
            file_src, __FILE__, __LINE__
    );
    scr_close(file_src, fd_src);
    return SCR_FAILURE;
  }

  /* read size of header, file size, block size, and number of blocks */
  uint64_t header_size, filesize, block_size, num_blocks;
  scr_unpack_uint64_t(buf, sizeof(buf), &size, &header_size);
  scr_unpack_uint64_t(buf, sizeof(buf), &size, &filesize);
  scr_unpack_uint64_t(buf, sizeof(buf), &size, &block_size);
  scr_unpack_uint64_t(buf, sizeof(buf), &size, &num_blocks);

  /* allocate buffer to hold file header */
  void* header = malloc(header_size);
  if (header == NULL) {
    scr_err("Allocating header buffer when decompressing %s: malloc(%ld) errno=%d %m @ %s:%d",
            file_src, header_size, errno, __FILE__, __LINE__
    );
    scr_close(file_src, fd_src);
    return SCR_FAILURE;
  }

  /* allocate buffer to read data into */
  void* buf_src = scr_align_malloc((size_t) block_size, page_size);
  if (buf_src == NULL) {
    scr_err("Allocating source buffer when decompressing %s: malloc(%ld) errno=%d %m @ %s:%d",
            file_src, block_size, errno, __FILE__, __LINE__
    );
    free(header);
    scr_close(file_src, fd_src);
    return SCR_FAILURE;
  }

  /* allocate buffer to write uncompressed data into */
  void* buf_dst = scr_align_malloc((size_t) block_size, page_size);
  if (buf_dst == NULL) {
    scr_err("Allocating compress buffer when decompressing %s: malloc(%ld) errno=%d %m @ %s:%d",
            file_src, block_size, errno, __FILE__, __LINE__
    );
    free(buf_src);
    free(header);
    scr_close(file_src, fd_src);
    return SCR_FAILURE;
  }

  /* these pointers will track our location within the file,
   * we must make sure that we never overrun the original data when compressing */
  off_t pos_src = 0;
  off_t pos_dst = 0;

  int rc = SCR_SUCCESS;

  /* seek back to start of file to read in full header */
  if (lseek(fd_src, 0, SEEK_SET) == (off_t) -1) {
    scr_err("Failed to seek to start of file %s @ %s:%d",
            file_src, __FILE__, __LINE__
    );
    rc = SCR_FAILURE;
  }

  /* read in full header, this time including block table */
  nread = scr_read(file_src, fd_src, header, header_size);
  if (nread < header_size) {
    scr_err("Failed to read in header from file %s @ %s:%d",
            file_src, __FILE__, __LINE__
    );
    rc = SCR_FAILURE;
  }

  /* get crc for header */
  uint32_t crc_header;
  size_t header_offset = header_size - sizeof(uint32_t);
  scr_unpack_uint32_t(header, header_size, &header_offset, &crc_header);

  /* compute crc over length of the header */
  uLong crc = crc32(0L, Z_NULL, 0);
  crc = crc32(crc, (const Bytef*) header, (uInt) (header_size - sizeof(uint32_t)));

  /* check that crc values match */
  if ((uLong) crc_header != crc) {
    scr_err("CRC32 mismatch detected in header of %s @ %s:%d",
            file_src, __FILE__, __LINE__
    );
    rc = SCR_FAILURE;
  }

  /* set pointer to end of block table */
  header_offset = sizeof(uint32_t) + 2 * sizeof(uint16_t) + 4 * sizeof(uint64_t) +
                  num_blocks * (2 * sizeof(uint64_t) + 2 * sizeof(uint32_t));

  /* read block from source file, compress, write to destination file */
  int block_count = 0;
  while (block_count < num_blocks && rc == SCR_SUCCESS) {
    /* back up one entry in the block table */
    header_offset -= 2 * sizeof(uint64_t) + 2 * sizeof(uint32_t);

    /* read entry for block from header: length, crc cmp, crc orig */
    uint64_t block_offset_cmp, block_size_cmp;
    uint32_t file_crc_cmp, file_crc_orig;
    scr_unpack_uint64_t(header, header_size, &header_offset, &block_offset_cmp);
    scr_unpack_uint64_t(header, header_size, &header_offset, &block_size_cmp);
    scr_unpack_uint32_t(header, header_size, &header_offset, &file_crc_cmp);
    scr_unpack_uint32_t(header, header_size, &header_offset, &file_crc_orig);

    /* set block pointer back to start of the block we just read */
    header_offset -= 2 * sizeof(uint64_t) + 2 * sizeof(uint32_t);

    /* initialize decompression stream */
    z_stream strm;
    strm.zalloc   = Z_NULL;
    strm.zfree    = Z_NULL;
    strm.opaque   = Z_NULL;
    strm.avail_in = 0;
    strm.next_in  = Z_NULL;
    int ret = inflateInit(&strm);
    if (ret != Z_OK) {
      scr_err("Failed to initialize decompression stream when processing %s (ret=%d) @ %s:%d",
              file_src, ret, __FILE__, __LINE__
      );
      rc = SCR_FAILURE;
    }

    /* read data, decompress, and write out */
    uint64_t total_read    = 0;
    uint64_t total_written = 0;
    uLong crc_cmp  = crc32(0L, Z_NULL, 0);
    uLong crc_orig = crc32(0L, Z_NULL, 0);
    while (total_read < block_size_cmp && rc == SCR_SUCCESS) {
      /* limit how much we read in */
      size_t count = block_size_cmp - total_read;
      if (count > block_size) {
        count = block_size;
      }

      /* TODO: these reads will be inefficient for very small compressed blocks
       * would be better to read with a buffered read like fread() here */

      /* seek to current location for reading */
      pos_src = header_size + block_offset_cmp + total_read;
      if (lseek(fd_src, (off_t) pos_src, SEEK_SET) == (off_t) -1) {
        scr_err("Seek to read position failed in %s @ %s:%d",
                file_src, __FILE__, __LINE__
        );
        rc = SCR_FAILURE;
      }

      /* read a block in from the file */
      ssize_t nread = scr_read(file_src, fd_src, buf_src, count);

      /* uncompress data and write it to file */
      if (nread > 0) {
        /* TODO: would be nice to handle this case, but we don't yet when doing
         * in place decompression */
        if (nread < block_size_cmp) {
          scr_err("Failed to read full compressed block from file %s @ %s:%d",
                  file_src, __FILE__, __LINE__
          );
          rc = SCR_FAILURE;
        }

        /* compute crc for compressed block */
        crc_cmp = crc32(crc_cmp, (const Bytef*) buf_src, (uInt) nread);

        /* uncompress data */
        strm.avail_in = nread;
        strm.next_in  = buf_src;
        do {
          /* record the number of uncompressed bytes */
          size_t have = 0;
          strm.avail_out = block_size;
          strm.next_out  = buf_dst;
          do {
            ret = inflate(&strm, Z_NO_FLUSH);
            if (ret == Z_NEED_DICT ||
                ret == Z_DATA_ERROR ||
                ret == Z_MEM_ERROR ||
                ret == Z_STREAM_ERROR)
            {
              /* hit an error of some sort */
              scr_err("Error during decompression in %s (ret=%d) @ %s:%d",
                      file_src, ret, __FILE__, __LINE__
              );
              rc = SCR_FAILURE;
            } else {
              /* compute number of uncompressed bytes written so far */
              have = block_size - strm.avail_out;
            }
          } while (strm.avail_in != 0 && strm.avail_out != 0 && ret != Z_BUF_ERROR && rc == SCR_SUCCESS);

          /* write data */
          if (have > 0 && rc == SCR_SUCCESS) {
            /* compute crc of uncompressed block */
            crc_orig = crc32(crc_orig, (const Bytef*) buf_dst, (uInt) have);

            /* determine byte location to start writing this data */
            pos_dst = (num_blocks - block_count - 1) * block_size + total_written;

            /* check that we don't clobber data we haven't yet read */
//            off_t pos_end = pos_src + block_size_cmp;
            off_t pos_end = pos_src; /* TODO: Here, we assume that we read the entire block starting at pos_src */
            if (pos_dst < pos_end && pos_src != header_size) {
              /* TODO: unwind what decompression we have done if any,
               * for now we just make this a fatal error */
              scr_err("Failed to decompress file in place %s @ %s:%d",
                      file_src, __FILE__, __LINE__
              );
              rc = SCR_FAILURE;
            }

            /* seek to current location for writing */
            if (lseek(fd_src, (off_t) pos_dst, SEEK_SET) == (off_t) -1) {
              scr_err("Seek to write position failed in %s @ %s:%d",
                      file_src, __FILE__, __LINE__
              );
              rc = SCR_FAILURE;
            }

            /* write uncompressed data to file */
            ssize_t nwrite = scr_write(file_src, fd_src, buf_dst, have);
            if (nwrite != have) {
              scr_err("Error writing to %s @ %s:%d",
                      file_src, __FILE__, __LINE__
              );
              rc = SCR_FAILURE;
            }

            /* update the number of uncompressed bytes we've written for this block */
            if (nwrite > 0) {
              total_written += nwrite;
            }
          }
        } while ((ret == Z_OK || ret == Z_BUF_ERROR) && rc == SCR_SUCCESS); /* strm.avail_out == 0 */

        /* check that we uncompressed the entire block */
        if (strm.avail_in != 0 || ret != Z_STREAM_END) {
          scr_err("Failed to decompress file %s @ %s:%d",
                  file_src, __FILE__, __LINE__
          );
          rc = SCR_FAILURE;
        }

        /* add to total that we've read */
        total_read += nread;
      }
    }

    /* done with this block, check crc values */
    if (crc_cmp != file_crc_cmp) {
      scr_err("CRC32 mismatch detected in compressed block #%d in %s @ %s:%d",
              block_count, file_src, __FILE__, __LINE__
      );
      rc = SCR_FAILURE;
    }
    if (crc_orig != file_crc_orig) {
      scr_err("CRC32 mismatch detected in decompressed block #%d in %s @ %s:%d",
              block_count, file_src, __FILE__, __LINE__
      );
      rc = SCR_FAILURE;
    }

    /* finalize the compression stream */
    inflateEnd(&strm);

    /* increment our block count */
    block_count++;
  }

  /* close files */
  scr_close(file_src, fd_src);

  /* truncate file */
  truncate(file_src, filesize);

  /* rename file */
  rename(file_src, file_dst);

  /* free our buffers */
  if (buf_dst != NULL) {
    scr_align_free(buf_dst);
    buf_dst = NULL;
  }
  if (buf_src != NULL) {
    scr_align_free(buf_src);
    buf_src = NULL;
  }
  if (header != NULL) {
    free(header);
    header = NULL;
  }

  return rc;
}

/* uncompress the specified file and store as file_dst */
int scr_uncompress(const char* file_src, const char* file_dst)
{
  /* check that we have valid file names */
  if (file_src == NULL || file_dst == NULL) {
    scr_err("NULL filename @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* check that the source file exists and that we can read it */
  if (access(file_src, F_OK | R_OK) != 0) {
    scr_err("File %s does not exist or does not have read permission @ %s:%d",
            file_src, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* get the page size (used to align buffers) */
  int page_size = getpagesize();
  if (page_size <= 0) {
    scr_err("Call to getpagesize failed when decompressing %s @ %s:%d",
            file_src, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* open original file */
  int fd_src = scr_open(file_src, O_RDONLY);
  if (fd_src < 0) {
    scr_err("Opening file for reading: %s errno=%d %m @ %s:%d",
            file_src, errno, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* open compressed file for writing */
  int fd_dst = scr_open(file_dst, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
  if (fd_dst < 0) {
    scr_err("Opening file for writing: %s errno=%d %m @ %s:%d",
            file_dst, errno, __FILE__, __LINE__
    );
    scr_close(file_src, fd_src);
    return SCR_FAILURE;
  }

  /* read first few bytes of header */
  char buf[SCR_FILE_COMPRESSED_HEADER_SIZE];
  size_t nread = scr_read(file_src, fd_src, buf, sizeof(buf));
  if (nread < sizeof(buf)) {
    scr_err("Failed to read header from file %s @ %s:%d",
            file_src, __FILE__, __LINE__
    );
    scr_close(file_dst, fd_dst);
    scr_close(file_src, fd_src);
    return SCR_FAILURE;
  }

  size_t size = 0;

  /* unpack magic number, the type, and the version */
  uint32_t magic;
  uint16_t type, version;
  scr_unpack_uint32_t(buf, sizeof(buf), &size, &magic);
  scr_unpack_uint16_t(buf, sizeof(buf), &size, &type);
  scr_unpack_uint16_t(buf, sizeof(buf), &size, &version);

  /* check the magic number, the type, and the version */
  if (magic   != SCR_FILE_MAGIC ||
      type    != SCR_FILE_TYPE_COMPRESSED ||
      version != SCR_FILE_VERSION_COMPRESSED_1)
  {
    scr_err("File type does not match values for a compressed file %s @ %s:%d",
            file_src, __FILE__, __LINE__
    );
    scr_close(file_dst, fd_dst);
    scr_close(file_src, fd_src);
    return SCR_FAILURE;
  }

  /* read size of header, file size, block size, and number of blocks */
  uint64_t header_size, filesize, block_size, num_blocks;
  scr_unpack_uint64_t(buf, sizeof(buf), &size, &header_size);
  scr_unpack_uint64_t(buf, sizeof(buf), &size, &filesize);
  scr_unpack_uint64_t(buf, sizeof(buf), &size, &block_size);
  scr_unpack_uint64_t(buf, sizeof(buf), &size, &num_blocks);

  /* allocate buffer to hold file header */
  void* header = malloc(header_size);
  if (header == NULL) {
    scr_err("Allocating header buffer when decompressing %s: malloc(%ld) errno=%d %m @ %s:%d",
            file_src, header_size, errno, __FILE__, __LINE__
    );
    scr_close(file_dst, fd_dst);
    scr_close(file_src, fd_src);
    return SCR_FAILURE;
  }

  /* allocate buffer to read data into */
  void* buf_src = scr_align_malloc((size_t) block_size, page_size);
  if (buf_src == NULL) {
    scr_err("Allocating source buffer when decompressing %s: malloc(%ld) errno=%d %m @ %s:%d",
            file_src, block_size, errno, __FILE__, __LINE__
    );
    free(header);
    scr_close(file_dst, fd_dst);
    scr_close(file_src, fd_src);
    return SCR_FAILURE;
  }

  /* allocate buffer to write uncompressed data into */
  void* buf_dst = scr_align_malloc((size_t) block_size, page_size);
  if (buf_dst == NULL) {
    scr_err("Allocating compress buffer when decompressing %s: malloc(%ld) errno=%d %m @ %s:%d",
            file_src, block_size, errno, __FILE__, __LINE__
    );
    free(buf_src);
    free(header);
    scr_close(file_dst, fd_dst);
    scr_close(file_src, fd_src);
    return SCR_FAILURE;
  }

  int rc = SCR_SUCCESS;

  /* seek back to start of file to read in full header */
  if (lseek(fd_src, 0, SEEK_SET) == (off_t) -1) {
    scr_err("Failed to seek to start of file %s @ %s:%d",
            file_src, __FILE__, __LINE__
    );
    rc = SCR_FAILURE;
  }

  /* read in full header, this time including block table */
  nread = scr_read(file_src, fd_src, header, header_size);
  if (nread < header_size) {
    scr_err("Failed to read in header from file %s @ %s:%d",
            file_src, __FILE__, __LINE__
    );
    rc = SCR_FAILURE;
  }

  /* get crc for header */
  uint32_t crc_header;
  size_t header_offset = header_size - sizeof(uint32_t);
  scr_unpack_uint32_t(header, header_size, &header_offset, &crc_header);

  /* compute crc over length of the header */
  uLong crc = crc32(0L, Z_NULL, 0);
  crc = crc32(crc, (const Bytef*) header, (uInt) (header_size - sizeof(uint32_t)));

  /* check that crc values match */
  if ((uLong) crc_header != crc) {
    scr_err("CRC32 mismatch detected in header of %s @ %s:%d",
            file_src, __FILE__, __LINE__
    );
    rc = SCR_FAILURE;
  }

  /* set header offset to point to entry for first block */
  header_offset = sizeof(uint32_t) + 2 * sizeof(uint16_t) + 4 * sizeof(uint64_t);

  /* read block from source file, compress, write to destination file */
  int block_count = 0;
  while (block_count < num_blocks && rc == SCR_SUCCESS) {
    /* read entry for block from header: length, crc cmp, crc orig */
    uint64_t block_offset_cmp, block_size_cmp;
    uint32_t file_crc_cmp, file_crc_orig;
    scr_unpack_uint64_t(header, header_size, &header_offset, &block_offset_cmp);
    scr_unpack_uint64_t(header, header_size, &header_offset, &block_size_cmp);
    scr_unpack_uint32_t(header, header_size, &header_offset, &file_crc_cmp);
    scr_unpack_uint32_t(header, header_size, &header_offset, &file_crc_orig);

    /* initialize decompression stream */
    z_stream strm;
    strm.zalloc   = Z_NULL;
    strm.zfree    = Z_NULL;
    strm.opaque   = Z_NULL;
    strm.avail_in = 0;
    strm.next_in  = Z_NULL;
    int ret = inflateInit(&strm);
    if (ret != Z_OK) {
      scr_err("Failed to initialize decompression stream when processing %s (ret=%d) @ %s:%d",
              file_src, ret, __FILE__, __LINE__
      );
      rc = SCR_FAILURE;
    }

    /* read data, decompress, and write out */
    uint64_t total_read = 0;
    uLong crc_cmp  = crc32(0L, Z_NULL, 0);
    uLong crc_orig = crc32(0L, Z_NULL, 0);
    while (total_read < block_size_cmp && rc == SCR_SUCCESS) {
      /* limit how much we read in */
      size_t count = block_size_cmp - total_read;
      if (count > block_size) {
        count = block_size;
      }

      /* TODO: these reads will be inefficient for very small compressed blocks
       * would be better to read with a buffered read like fread() here */

      /* read a block in from the file */
      ssize_t nread = scr_read(file_src, fd_src, buf_src, count);

      /* uncompress data and write it to file */
      if (nread > 0) {
        /* compute crc for compressed block */
        crc_cmp = crc32(crc_cmp, (const Bytef*) buf_src, (uInt) nread);

        /* uncompress data */
        strm.avail_in = nread;
        strm.next_in  = buf_src;
        do {
          /* record the number of uncompressed bytes */
          size_t have = 0;
          strm.avail_out = block_size;
          strm.next_out  = buf_dst;
          do {
            ret = inflate(&strm, Z_NO_FLUSH);
            if (ret == Z_NEED_DICT ||
                ret == Z_DATA_ERROR ||
                ret == Z_MEM_ERROR ||
                ret == Z_STREAM_ERROR)
            {
              /* hit an error of some sort */
              scr_err("Error during decompression in %s (ret=%d) @ %s:%d",
                      file_src, ret, __FILE__, __LINE__
              );
              rc = SCR_FAILURE;
            } else {
              /* compute number of uncompressed bytes written so far */
              have = block_size - strm.avail_out;
            }
          } while (strm.avail_in != 0 && strm.avail_out != 0 && ret != Z_BUF_ERROR && rc == SCR_SUCCESS);

          /* write data */
          if (have > 0 && rc == SCR_SUCCESS) {
            /* compute crc of uncompressed block */
            crc_orig = crc32(crc_orig, (const Bytef*) buf_dst, (uInt) have);

            /* write uncompressed data to file */
            ssize_t nwrite = scr_write(file_dst, fd_dst, buf_dst, have);
            if (nwrite != have) {
              scr_err("Error writing to %s @ %s:%d",
                      file_dst, __FILE__, __LINE__
              );
              rc = SCR_FAILURE;
            }
          }
        } while ((ret == Z_OK || ret == Z_BUF_ERROR) && rc == SCR_SUCCESS); /* strm.avail_out == 0 */

        /* check that we uncompressed the entire block */
        if (strm.avail_in != 0 || ret != Z_STREAM_END) {
          scr_err("Failed to decompress file %s @ %s:%d",
                  file_src, __FILE__, __LINE__
          );
          rc = SCR_FAILURE;
        }

        /* add to total that we've read */
        total_read += nread;
      }
    }

    /* done with this block, check crc values */
    if (crc_cmp != file_crc_cmp) {
      scr_err("CRC32 mismatch detected in compressed block #%d in %s @ %s:%d",
              block_count, file_src, __FILE__, __LINE__
      );
      rc = SCR_FAILURE;
    }
    if (crc_orig != file_crc_orig) {
      scr_err("CRC32 mismatch detected in decompressed block #%d in %s @ %s:%d",
              block_count, file_src, __FILE__, __LINE__
      );
      rc = SCR_FAILURE;
    }

    /* finalize the compression stream */
    inflateEnd(&strm);

    /* increment our block count */
    block_count++;
  }

  /* close files */
  scr_close(file_src, fd_src);
  scr_close(file_dst, fd_dst);

  /* TODO: truncate file */

  /* TODO: rename file */

  /* free our buffers */
  if (buf_dst != NULL) {
    scr_align_free(buf_dst);
    buf_dst = NULL;
  }
  if (buf_src != NULL) {
    scr_align_free(buf_src);
    buf_src = NULL;
  }
  if (header != NULL) {
    free(header);
    header = NULL;
  }

  return rc;
}

/*
=========================================
Timing
=========================================
*/

/* returns the current linux timestamp (secs + usecs since epoch) as a double */
double scr_seconds()
{
  struct timeval tv;
  gettimeofday(&tv, NULL);
  double secs = (double) tv.tv_sec + (double) tv.tv_usec / (double) 1000000.0;
  return secs;
}
