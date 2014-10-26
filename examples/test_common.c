#define _GNU_SOURCE 1

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>
#include "mpi.h"

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

/* write the checkpoint data to fd, and return whether the write was successful */
int write_checkpoint(int fd, int ckpt, char* buf, size_t size)
{
  ssize_t rc;

  /* write the checkpoint id (application timestep) */
  char ckpt_buf[7];
  sprintf(ckpt_buf, "%06d", ckpt);
  rc = reliable_write(fd, ckpt_buf, sizeof(ckpt_buf));
  if (rc < 0) return 0;

  /* write the checkpoint data */
  rc = reliable_write(fd, buf, size);
  if (rc < 0) return 0;

  return 1;
}

/* read the checkpoint data from file into buf, and return whether the read was successful */
int read_checkpoint(char* file, int* ckpt, char* buf, size_t size)
{
  ssize_t n;
  char ckpt_buf[7];

  int fd = open(file, O_RDONLY);
  if (fd > 0) {
    /* read the checkpoint id */
    n = reliable_read(fd, ckpt_buf, sizeof(ckpt_buf));

    /* read the checkpoint data, and check the file size */
    n = reliable_read(fd, buf, size+1);
    if (n != size) {
      printf("Filesize not correct. Expected %lu, got %lu\n", size, n);
      close(fd);
      return 0;
    }

    /* if the file looks good, set the timestep and return */
    (*ckpt) = atoi(ckpt_buf);

    close(fd);

    return 1;
  }
  else {
  	printf("Could not open file %s\n", file);
  }

  return 0;
}
