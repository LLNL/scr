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
  //FILE *outfile = stdout;
// /*
  char *outfilename = (char*)malloc(sizeof(char)*10);
  sprintf(outfilename,"outrank%d",rank);
  FILE *outfile = fopen(outfilename,"a");
  free(outfilename);
  if (!outfile) {
    fprintf(stderr,"ERROR OPENING OUTPUTS\n");
    MPI_Abort(MPI_COMM_WORLD,1);
    return 0;
  }
// */
  pid = getpid();
  ppid = getppid();
  fprintf(outfile,"Rank %d: PID = %d, PPID = %d\n",rank,pid,ppid);
  fprintf(outfile,"If unsuccessful, this program will run for 10 minutes\n");
  fprintf(outfile,"This program will write the posix timestamp every 5 seconds\n");
  fprintf(outfile,"Rank %d/%d, going to sleep . . .\n",rank+1,commsize);
  fflush(outfile);
  int secs = 600;
  while (secs>0) {
    time_t seconds = time(NULL);
    fprintf(outfile,"%ld\n",seconds);
    fflush(outfile);
    sleep(5);
    secs-=5;
  }
  fclose(outfile);
  MPI_Barrier(MPI_COMM_WORLD);
  MPI_Finalize();
  return 0;
}
