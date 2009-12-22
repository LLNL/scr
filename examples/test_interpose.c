#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "mpi.h"

#include <time.h>
#include <sys/time.h>
struct timeval tv0[1];
struct timeval tv1[1];
struct timeval rv[1];

#ifndef timersub
# define timersub(a, b, result)                                               \
  do {                                                                        \
    (result)->tv_sec = (a)->tv_sec - (b)->tv_sec;                             \
    (result)->tv_usec = (a)->tv_usec - (b)->tv_usec;                          \
    if ((result)->tv_usec < 0) {                                              \
      --(result)->tv_sec;                                                     \
      (result)->tv_usec += 1000000;                                           \
    }                                                                         \
  } while (0)
#endif

//size_t filesize = 600*1024*1024;
//size_t filesize = 500*1024*1024;
//size_t filesize = 10*1024*1024;
size_t filesize = 512*1024;
int times = 1;

int main (int argc, char* argv[])
{
  int id = 0;
  MPI_Init(&argc, &argv);

  int rank, size;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &size);

  MPI_Barrier(MPI_COMM_WORLD);

  // checkpoint file
  char file[1024];
  char prefix[1024];
  char dir[1024];
  if (getcwd(prefix, sizeof(prefix)) == NULL) {
    printf("Error reading current working directory\n");
    exit(1);
  }

  // checkpoint data
  filesize = filesize + rank;
  char* buf = (char*) malloc(filesize);
  int i;
  for(i=0; i < filesize; i++) { buf[i] = 'a' + (rank+i) % 26; }

  // write my data to /tmp
  mkdir(prefix, S_IRUSR | S_IWUSR | S_IXUSR);

  // prime the file
  for(i=0; i < 1; i++) {
    id++;
    sprintf(dir,    "%s/checkpoint_set_%d", prefix, id);
    sprintf(file,   "%s/rank_%d.ckpt", dir, rank);
    if (rank == 0) {
      mkdir(dir, S_IRUSR | S_IWUSR | S_IXUSR);
    }
    MPI_Barrier(MPI_COMM_WORLD);
printf("File: %s\n", file);
    int fd_me = open(file, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
    if (fd_me > 0) {
      write(fd_me, buf, filesize);
      close(fd_me);
    }
  }
  MPI_Barrier(MPI_COMM_WORLD);

  int count = 0;
  gettimeofday (tv0, NULL);
  for(i=0; i < times; i++) {
    id++;
    sprintf(dir,    "%s/checkpoint_set_%d", prefix, id);
    sprintf(file,   "%s/rank_%d.ckpt", dir, rank);
    if (rank == 0) {
      mkdir(dir, S_IRUSR | S_IWUSR | S_IXUSR);
    }
    MPI_Barrier(MPI_COMM_WORLD);
printf("File: %s\n", file);
    int fd_me = open(file, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
    if (fd_me > 0) {
      count++;
      write(fd_me, buf, filesize);
      close(fd_me);
    }
  }
  gettimeofday (tv1, NULL);
  timersub (tv1, tv0, rv);
  double usecs = rv->tv_sec * 1000000 + rv->tv_usec;
  double bw = (filesize*count/(1024*1024))*(1000000/usecs);
  //printf("Took %f s\t%7.2f MB/s\n", (float) usecs / (float) (1000000*count), bw);

  MPI_Barrier(MPI_COMM_WORLD);

  double bwmin, bwmax, bwsum;
  MPI_Reduce(&bw, &bwmin, 1, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);
  MPI_Reduce(&bw, &bwmax, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
  MPI_Reduce(&bw, &bwsum, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
  if (rank == 0) { printf("Min %7.2f MB/s\tMax %7.2f MB/s\tAvg %7.2f MB/s\tAgg %7.2f\n", bwmin, bwmax, bwsum/size, bwsum); }

  if (buf != NULL) { free(buf); buf = NULL; }

  MPI_Finalize();

  return 0;
}
