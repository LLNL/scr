#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>
#include "mpi.h"

int main(int argc, char**argv) {
  int rank,commsize,pid,ppid;
  MPI_Init(&argc,&argv);
  MPI_Comm_rank(MPI_COMM_WORLD,&rank);
  MPI_Comm_size(MPI_COMM_WORLD,&commsize);
  pid = getpid();
  ppid = getppid();
  fprintf(stdout,"Rank %d: PID = %d, PPID = %d\n",rank,pid,ppid);
  fprintf(stdout,"This program will write the posix timestamp every 5 seconds\n");
  fflush(stdout);
  fprintf(stderr,"stderr: Rank %d: This is a helpful error message\n",rank);
  fflush(stderr);
  int secs = 20;
  while (secs>0) {
    time_t seconds = time(NULL);
    fprintf(stdout,"%ld\n",seconds);
    fflush(stdout);
    sleep(5);
    secs-=5;
  }
  MPI_Barrier(MPI_COMM_WORLD);
  MPI_Finalize();
  return 0;
}
