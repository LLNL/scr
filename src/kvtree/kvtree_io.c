/* Implements a reliable open/read/write/close interface via open and close.
 * Implements directory manipulation functions. */

/* Please note todos in the cppr section; an optimization of using CPPR apis is
 * planned for upcoming work */

#include "kvtree.h"
#include "kvtree_err.h"
#include "kvtree_io.h"
#include "kvtree_helpers.h"

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

/* returns user's current mode as determined by their umask */
mode_t kvtree_getmode(int read, int write, int execute)
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
int kvtree_open(const char* file, int flags, ...)
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
    kvtree_dbg(1, "Opening file: open(%s) errno=%d %s @ %s:%d",
      file, errno, strerror(errno), __FILE__, __LINE__
    );

    /* try again */
    int tries = KVTREE_OPEN_TRIES;
    while (tries && fd < 0) {
      usleep(KVTREE_OPEN_USLEEP);
      if (mode_set) {
        fd = open(file, flags, mode);
      } else {
        fd = open(file, flags);
      }
      tries--;
    }

    /* if we still don't have a valid file, consider it an error */
    if (fd < 0) {
      kvtree_err("Opening file: open(%s) errno=%d %s @ %s:%d",
        file, errno, strerror(errno), __FILE__, __LINE__
      );
    }
  }
  return fd;
}

/* fsync and close file */
int kvtree_close(const char* file, int fd)
{
  /* fsync first */
  if (fsync(fd) < 0) {
    /* print warning that fsync failed */
    kvtree_dbg(2, "Failed to fsync file descriptor: %s errno=%d %s @ %s:%d",
      file, errno, strerror(errno), __FILE__, __LINE__
    );
  }

  /* now close the file */
  if (close(fd) != 0) {
    /* hit an error, print message */
    kvtree_err("Closing file descriptor %d for file %s: errno=%d %s @ %s:%d",
      fd, file, errno, strerror(errno), __FILE__, __LINE__
    );
    return KVTREE_FAILURE;
  }

  return KVTREE_SUCCESS;
}

/*
 * Provide process-safe locking to a kvtree file
 */
int kvtree_file_lock_read_write(const char* file, int fd, int write)
{
  #ifdef KVTREE_FILE_LOCK_USE_FLOCK
    int op;
    if (write) {
        op = LOCK_EX;
    } else {
        op = LOCK_SH;
    }
    if (flock(fd, op) != 0) {
      kvtree_err("Failed to acquire file lock on %s: flock(%d, %d) errno=%d %s @ %s:%d",
        file, fd, op, errno, strerror(errno), __FILE__, __LINE__
      );
      return KVTREE_FAILURE;
    }
  #endif

  #ifdef KVTREE_FILE_LOCK_USE_FCNTL
    struct flock lck;
    if (write) {
      lck.l_type = F_WRLCK;
    } else {
      lck.l_type = F_RDLCK;
    }
    lck.l_whence = SEEK_SET;
    lck.l_start = 0L;
    lck.l_len = 0L; //locking the entire file

    if(fcntl(fd, F_SETLKW, &lck) < 0) {
      kvtree_err("Failed to acquire file read lock on %s: fnctl(%d, %d) errno=%d %s @ %s:%d",
        file, fd, F_WRLCK, errno, strerror(errno), __FILE__, __LINE__
      );
      return KVTREE_FAILURE;
    }
  #endif

  return KVTREE_SUCCESS;
}

int kvtree_file_unlock(const char* file, int fd)
{
  #ifdef KVTREE_FILE_LOCK_USE_FLOCK
    if (flock(fd, LOCK_UN) != 0) {
      kvtree_err("Failed to acquire file lock on %s: flock(%d, %d) errno=%d %s @ %s:%d",
        file, fd, LOCK_UN, errno, strerror(errno), __FILE__, __LINE__
      );
      return KVTREE_FAILURE;
    }
  #endif

  #ifdef KVTREE_FILE_LOCK_USE_FCNTL
    struct flock lck;
    lck.l_type = F_UNLCK;
    lck.l_whence = 0;
    lck.l_start = 0L;
    lck.l_len = 0L; //locking the entire file

    if(fcntl(fd, F_SETLK, &lck) < 0) {
      kvtree_err("Failed to acquire file read lock on %s: fnctl(%d, %d) errno=%d %s @ %s:%d",
        file, fd, F_UNLCK, errno, strerror(errno), __FILE__, __LINE__
      );
      return KVTREE_FAILURE;
    }
  #endif

  return KVTREE_SUCCESS;
}

/* opens specified file and waits on a lock before returning the file descriptor */
int kvtree_open_with_lock(const char* file, int flags, mode_t mode, int write)
{
  /* open the file */
  int fd = kvtree_open(file, flags, mode);
  if (fd < 0) {
    kvtree_err("Opening file for write: kvtree_open(%s) errno=%d %s @ %s:%d",
      file, errno, strerror(errno), __FILE__, __LINE__
    );
    return fd;
  }

  /* acquire shared (write=0) or exclusive (write=1) file lock */
  int ret = kvtree_file_lock_read_write(file, fd, write);
  if (ret != KVTREE_SUCCESS) {
    close(fd);
    return ret;
  }

  /* return the opened file descriptor */
  return fd;
}

/* unlocks the specified file descriptor and then closes the file */
int kvtree_close_with_unlock(const char* file, int fd)
{
  /* release the file lock */
  int ret = kvtree_file_unlock(file, fd);
  if (ret != KVTREE_SUCCESS) {
    return ret;
  }

  /* close the file */
  return kvtree_close(file, fd);
}

/* seek file descriptor to specified position */
int kvtree_lseek(const char* file, int fd, off_t pos, int whence)
{
  off_t rc = lseek(fd, pos, whence);
  if (rc == (off_t)-1) {
    kvtree_err("Error seeking %s: errno=%d %s @ %s:%d",
      file, errno, strerror(errno), __FILE__, __LINE__
    );
    return KVTREE_FAILURE;
  }
  return KVTREE_SUCCESS;
}

/* make a good attempt to read from file (retries, if necessary, return error if fail) */
ssize_t kvtree_read_attempt(const char* file, int fd, void* buf, size_t size)
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
        kvtree_err("Error reading file %s errno=%d %s @ %s:%d",
          file, errno, strerror(errno), __FILE__, __LINE__
        );
      } else {
        /* too many failed retries, give up */
        kvtree_err("Giving up read on file %s errno=%d %s @ %s:%d",
	  file, errno, strerror(errno), __FILE__, __LINE__
        );
        return -1;
      }
    }
  }
  return n;
}

/* make a good attempt to write to file (retries, if necessary, return error if fail) */
ssize_t kvtree_write_attempt(const char* file, int fd, const void* buf, size_t size)
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
      kvtree_err("Error writing file %s write returned 0 @ %s:%d",
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
        kvtree_err("Error writing file %s errno=%d %s @ %s:%d",
          file, errno, strerror(errno), __FILE__, __LINE__
        );
      } else {
        /* too many failed retries, give up */
        kvtree_err("Giving up write of file %s errno=%d %s @ %s:%d",
          file, errno, strerror(errno), __FILE__, __LINE__
        );
        return -1;
      }
    }
  }
  return n;
}

/* tests whether the file or directory is readable */
int kvtree_file_is_readable(const char* file)
{
  /* check whether the file can be read */
  if (access(file, R_OK) < 0) {
    /* TODO: would be nice to print a message here, but
     *       functions calling this expect it to be quiet
    kvtree_dbg(2, "File not readable: %s errno=%d %s @ %s:%d",
      file, errno, strerror(errno), __FILE__, __LINE__
    );
    */
    return KVTREE_FAILURE;
  }
  return KVTREE_SUCCESS;
}
