#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include "mpi.h"

int main(int argc, char**argv) {
  int rank,commsize,pid,ppid;
  MPI_Init(&argc,&argv);
  MPI_Comm_rank(MPI_COMM_WORLD,&rank);
  MPI_Comm_size(MPI_COMM_WORLD,&commsize);
  FILE *outfile = stdout;
/*
  FILE *outfile = fopen("","a");
  if (!outfile) {
    fprintf(stderr,"ERROR OPENING OUTPUTS\n");
    MPI_Abort(MPI_COMM_WORLD,1);
    return 0;
  }
*/
  pid = getpid();
  ppid = getppid();
  fprintf(outfile,"Rank %d: PID = %d, PPID = %d\n",rank,pid,ppid);
  fprintf(outfile,"%d/%d) Hallo, we are going to take a nap . . .\n",rank+1,commsize);
  int secs = 60;
  while (secs>0) {
    fprintf(outfile,"%d/%d) You have %d seconds to kill me.\n",rank+1,commsize,secs);
    sleep(15);
    secs-=15;
  }
  MPI_Barrier(MPI_COMM_WORLD);
  MPI_Finalize();
  return 0;
}
