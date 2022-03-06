#define _GNU_SOURCE 1

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include <stdarg.h>
#include "mpi.h"

typedef struct checkpoint_buf_t { char buf[7]; } checkpoint_buf_t;

/* reliable read from file descriptor (retries, if necessary, until hard error) */
ssize_t reliable_read(int fd, void* buf, size_t size)
{
  ssize_t n = 0;
  int retries = 10;
  int rank;
  char host[128];
  while (n < size)
  {
    ssize_t rc = read(fd, (char*) buf + n, size - n);
    if (rc > 0) {
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
        MPI_Comm_rank(MPI_COMM_WORLD, &rank);
        gethostname(host, sizeof(host));
        printf("%d on %s: ERROR: Error reading: read(%d, %p, %ld) errno=%d %s @ %s:%d\n",
                rank, host, fd, (char*) buf + n, size - n, errno, strerror(errno), __FILE__, __LINE__
        );
      } else {
        /* too many failed retries, give up */
        MPI_Comm_rank(MPI_COMM_WORLD, &rank);
        gethostname(host, sizeof(host));
        printf("%d on %s: ERROR: Giving up read: read(%d, %p, %ld) errno=%d %s @ %s:%d\n",
                rank, host, fd, (char*) buf + n, size - n, errno, strerror(errno), __FILE__, __LINE__
        );
        MPI_Abort(MPI_COMM_WORLD, 0);
      }
    }
  }
  return size;
}

/* reliable write to file descriptor (retries, if necessary, until hard error) */
ssize_t reliable_write(int fd, const void* buf, size_t size)
{
  ssize_t n = 0;
  int retries = 10;
  int rank;
  char host[128];
  while (n < size)
  {
    ssize_t rc = write(fd, (char*) buf + n, size - n);
    if (rc > 0) {
      n += rc;
    } else if (rc == 0) {
      /* something bad happened, print an error and abort */
      MPI_Comm_rank(MPI_COMM_WORLD, &rank);
      gethostname(host, sizeof(host));
      printf("%d on %s: ERROR: Error writing: write(%d, %p, %ld) returned 0 @ %s:%d\n",
              rank, host, fd, (char*) buf + n, size - n, __FILE__, __LINE__
      );
      MPI_Abort(MPI_COMM_WORLD, 0);
    } else { /* (rc < 0) */
      /* got an error, check whether it was serious */
      if (errno == EINTR || errno == EAGAIN) {
        continue;
      }

      /* something worth printing an error about */
      retries--;
      if (retries) {
        /* print an error and try again */
        MPI_Comm_rank(MPI_COMM_WORLD, &rank);
        gethostname(host, sizeof(host));
        printf("%d on %s: ERROR: Error writing: write(%d, %p, %ld) errno=%d %s @ %s:%d\n",
                rank, host, fd, (char*) buf + n, size - n, errno, strerror(errno), __FILE__, __LINE__
        );
      } else {
        /* too many failed retries, give up */
        MPI_Comm_rank(MPI_COMM_WORLD, &rank);
        gethostname(host, sizeof(host));
        printf("%d on %s: ERROR: Giving up write: write(%d, %p, %ld) errno=%d %s @ %s:%d\n",
                rank, host, fd, (char*) buf + n, size - n, errno, strerror(errno), __FILE__, __LINE__
        );
        MPI_Abort(MPI_COMM_WORLD, 0);
      }
    }
  }
  return size;
}

/* initialize buffer with some well-known value based on rank */
int init_buffer(char* buf, size_t size, int rank, int ckpt)
{
  size_t i;
  for(i=0; i < size; i++) {
    /*char c = 'a' + (rank+i) % 26;*/
    char c = (char) ((size_t)rank + i) % 256;
    buf[i] = c;
  }
  return 0;
}

/* checks buffer for expected value */
int check_buffer(char* buf, size_t size, int rank, int ckpt)
{
  size_t i;
  for(i=0; i < size; i++) {
    /*char c = 'a' + (rank+i) % 26;*/
    char c = (char) ((size_t)rank + i) % 256;
    if (buf[i] != c)  {
      return 0;
    }
  }
  return 1;
}

/* get size of specified file */
unsigned long get_filesize(const char* file)
{
  /* stat the file to get its size and other metadata */
  unsigned long filesize = 0;
  struct stat stat_buf;
  int stat_rc = stat(file, &stat_buf);
  if (stat_rc == 0) {
    filesize = (unsigned long) stat_buf.st_size;
  }
  return filesize;
}

ssize_t checkpoint_timestep_size()
{
  return sizeof(checkpoint_buf_t);
}

/* write the checkpoint data to fd, and return whether the write was successful */
int write_checkpoint(int fd, int ckpt, char* buf, size_t size)
{
  ssize_t rc;

  /* write the checkpoint id (application timestep) */
  checkpoint_buf_t ckpt_buf;
  sprintf(ckpt_buf.buf, "%06d", ckpt);
  rc = reliable_write(fd, ckpt_buf.buf, sizeof(ckpt_buf));
  if (rc < 0) return 0;

  /* write the checkpoint data */
  rc = reliable_write(fd, buf, size);
  if (rc < 0) return 0;

  return 1;
}

/* read the checkpoint data from file into buf, and return whether the read was successful */
int read_checkpoint(int fd, int* ckpt, char* buf, size_t size)
{
  /* read the checkpoint id */
  checkpoint_buf_t ckpt_buf;
  ssize_t n = reliable_read(fd, ckpt_buf.buf, sizeof(ckpt_buf));

  /* read the checkpoint data, and check the file size */
  n = reliable_read(fd, buf, size);
  if (n != size) {
    printf("Filesize not correct. Expected %lu, got %lu\n", size, n);
    return 0;
  }

  /* if the file looks good, set the timestep and return */
  *ckpt = atoi(ckpt_buf.buf);

  return 1;
}

/* read the checkpoint data from file into buf, and return whether the read was successful */
int read_checkpoint_file(char* file, int* ckpt, char* buf, size_t size)
{
  ssize_t n;

  int fd = open(file, O_RDONLY);
  if (fd >= 0) {
    int timestep;
    int rc = read_checkpoint(fd, &timestep, buf, size);
    if (rc != 1) {
      printf("Failed to read checkpoint data\n");
      close(fd);
      return 0;
    }

    /* read one byte past the expected size to verify we've hit the end of the file */
    char endbuf[1];
    n = reliable_read(fd, endbuf, 1);
    if (n != 0) {
      printf("Filesize not correct. Expected %lu, got %lu\n", size, size+1);
      close(fd);
      return 0;
    }

    /* if the file looks good, set the timestep and return */
    *ckpt = timestep;

    close(fd);

    return 1;
  } else {
    printf("Could not open file %s\n", file);
  }

  return 0;
}

/* This is just snprintf(), but will abort if there's a truncation.  This makes
 * it so you don't have to check the return code for sprintf's that should
 * "never fail". */
int safe_snprintf(char* buf, size_t size, const char* fmt, ...)
{
  va_list args;
  va_start(args, fmt);
  int rc = vsnprintf(buf, size, fmt, args);
  va_end(args);

  if ((size_t)rc >= size) {
    /* We truncated the string */
    printf("%s: truncated string: %s\n", __func__, buf);
    MPI_Abort(MPI_COMM_WORLD, 1);
  }

  return rc;
}
