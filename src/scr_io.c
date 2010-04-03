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

#include <stdlib.h>
#include <stdarg.h>
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
ssize_t scr_read(int fd, void* buf, size_t size)
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
  return n;
}

/* reliable write to file descriptor (retries, if necessary, until hard error) */
ssize_t scr_write(int fd, const void* buf, size_t size)
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
      scr_err("Error writing: write(%d, %x, %ld) returned 0 @ %s:%d",
	      fd, (char*) buf + n, size - n, __FILE__, __LINE__
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
    ssize_t nread = scr_read(fd, &c, sizeof(c));

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
  ssize_t rc = scr_write(fd, buf, n);

  return rc;
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
    nread = scr_read(fd, buf, buffer_size);
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

/* combine path and filename into a fullpath in file */
int scr_build_path (char* file, const char* path, const char* filename)
{
  /* first build in temp, then copy to file, which lets caller use same variable in input and output parameters */
  char temp[SCR_MAX_FILENAME];

  if ((path == NULL || strcmp(path, "") == 0) && (filename == NULL || strcmp(filename, "") == 0)) {
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

/*
=========================================
File Copy Functions
=========================================
*/

/* TODO: could perhaps use O_DIRECT here as an optimization */
/* TODO: could apply compression/decompression here */
/* copy src_file (full path) to dest_path and return new full path in dest_file */
int scr_copy_to(const char* src, const char* dst_dir, unsigned long buf_size, char* dst, uLong* crc)
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
  scr_build_path(dst, dst_dir, name);

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
