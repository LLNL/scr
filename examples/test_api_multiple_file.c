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
#include "scr.h"
#include "test_common.h"

#include <time.h>
#include <sys/time.h>
struct timeval tv0[1];
struct timeval tv1[1];
struct timeval rv[1];

//size_t filesize = 500*1024*1024;
//size_t filesize = 100*1024*1024;
//size_t filesize =  50*1024*1024;
//size_t filesize =  10*1024*1024;
size_t filesize = 512*1024;
int times = 3;
int seconds = 0;

int  timestep = 0;

int main (int argc, char* argv[])
{
  char *path_to_stdout = NULL;
  int scr_retval;
  /* check that we got an appropriate number of arguments */
  if (argc == 2) {
    path_to_stdout = argv[1];
  }
  else if(argc == 5){
    filesize = (size_t) atol(argv[1]);
    times = atoi(argv[2]);
    seconds = atoi(argv[3]);
    path_to_stdout = argv[4];
  }
  else{
    printf("Usage: test_api_file [filesize times sleep_secs path_to_stdout]\n");
    printf("OR: test_api_file [ path_to_stdout]\n");
    exit(1);
  }
  
  MPI_Init(&argc, &argv);

  int rank = -1, size = 0;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &size);

  /* open file for stdout */
  printf("new stdout filename: \"%s\"\n", path_to_stdout);
  fflush(stdout);
  freopen(path_to_stdout, "a+", stdout);
  MPI_Barrier(MPI_COMM_WORLD);

  /* time how long it takes to get through init */
  MPI_Barrier(MPI_COMM_WORLD);

  double init_start = MPI_Wtime();
  if (SCR_Init() != SCR_SUCCESS){
    printf("FAILED INITIALIZING SCR\n");
    fclose(stdout);
    return -1;
  }
  double init_end = MPI_Wtime();
  double secs = init_end - init_start;

  MPI_Barrier(MPI_COMM_WORLD);

  double secsmin, secsmax, secssum;
  MPI_Reduce(&secs, &secsmin, 1, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);
  MPI_Reduce(&secs, &secsmax, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
  MPI_Reduce(&secs, &secssum, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
  if (rank == 0) { printf("Init: Min %8.6f s\tMax %8.6f s\tAvg %8.6f s\n", secsmin, secsmax, secssum/size); }

  MPI_Barrier(MPI_COMM_WORLD);

  int num_files = rank % 4;
  char** files = NULL;
  char** bufs  = NULL;
  size_t* filesizes = NULL;
  char* buf = NULL;
  if (num_files > 0) {
    files = (char**) malloc(num_files * sizeof(char*));
    bufs  = (char**) malloc(num_files * sizeof(char*));
    filesizes = (size_t*) malloc(num_files * sizeof(size_t));
  }

  int i;
  for (i=0; i < num_files; i++) {
    // route our checkpoint file
    char name[256];
    sprintf(name, "rank_%d.%d.ckpt", rank, i);
    files[i] = strdup(name);
    filesizes[i] = filesize + rank + 2*i;
    bufs[i] = (char*) malloc(filesizes[i]);
  }
  if (num_files > 0) {
    buf = (char*) malloc(filesizes[num_files-1]);
  }

  // check each of our checkpoint files
  int found_checkpoint = 1;
  for (i=0; i < num_files; i++) {
    char file[2094];
    scr_retval = SCR_Route_file(files[i], file);
    if (scr_retval != SCR_SUCCESS) {
      printf("%d: failed calling SCR_Route_file(): %d: @%s:%d\n",
             rank, scr_retval, __FILE__, __LINE__
      );
    }
    if (read_checkpoint(file, &timestep, buf, filesizes[i])) {
      // check that contents are good
      if (!check_buffer(buf, filesizes[i], rank + 2*i, timestep)) {
        printf("!!!!CORRUPTION!!!! Rank %d, File %s: Invalid value in buffer\n", rank, file);
        fflush(stdout);
        fclose(stdout);
        MPI_Abort(MPI_COMM_WORLD, 1);
        return 1;
      }
    } else {
      found_checkpoint = 0;
    }
  }

  // check that everyone found their checkpoint files ok
  int all_found_checkpoint = 0;
  MPI_Allreduce(&found_checkpoint, &all_found_checkpoint, 1, MPI_INT, MPI_LAND, MPI_COMM_WORLD);
  if (!all_found_checkpoint && rank == 0) {
    printf("At least one rank (perhaps all) did not find its checkpoint\n");
    fflush(stdout);
  }

  // check that everyone is at the same timestep
  int timestep_and, timestep_or;
  int timestep_a, timestep_o;
  if (num_files > 0) {
    timestep_a = timestep;
    timestep_o = timestep;
  } else {
    timestep_a = 0xffffffff;
    timestep_o = 0x00000000;
  }
  MPI_Allreduce(&timestep_a, &timestep_and, 1, MPI_INT, MPI_BAND, MPI_COMM_WORLD);
  MPI_Allreduce(&timestep_o, &timestep_or,  1, MPI_INT, MPI_BOR,  MPI_COMM_WORLD);
  if (timestep_and != timestep_or) {
    printf("%d: Timesteps don't agree: timestep %d\n", rank, timestep);
    fflush(stdout);
    fclose(stdout);
    return 1;
  }
  timestep = timestep_and;

  // make up some data for the next checkpoint
  for (i=0; i < num_files; i++) {
    init_buffer(bufs[i], filesizes[i], rank + 2*i, timestep);
  }

  timestep++;

  // prime system once before timing
  int t;
  for(t=0; t < 1; t++) {
    int rc;
    int all_valid = 1;
    scr_retval = SCR_Start_checkpoint();
    if (scr_retval != SCR_SUCCESS) {
      printf("%d: failed calling SCR_Start_checkpoint(): %d: @%s:%d\n",
             rank, scr_retval, __FILE__, __LINE__
      );
    }
  for (i=0; i < num_files; i++) {
    int valid = 0;
    char file[2094];
    scr_retval = SCR_Route_file(files[i], file);
    if (scr_retval != SCR_SUCCESS) {
      printf("%d: failed calling SCR_route_file(): %d: @%s:%d\n",
             rank, scr_retval, __FILE__, __LINE__
      );
    }
    int fd_me = open(file, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
    if (fd_me > 0) {
      valid = 1;

      // write the checkpoint
      rc = write_checkpoint(fd_me, timestep, bufs[i], filesizes[i]);
      if (rc < 0) { valid = 0; }

      rc = fsync(fd_me);
      if (rc < 0) { valid = 0; }

      // make sure the close is without error
      rc = close(fd_me);
      if (rc < 0) { valid = 0; }
    }
    if (!valid) { all_valid = 0; }
  }
  scr_retval = SCR_Complete_checkpoint(all_valid);
  if (scr_retval != SCR_SUCCESS) {
    printf("%d: failed calling SCR_Complete_checkpoint(): %d: @%s:%d\n",
           rank, scr_retval, __FILE__, __LINE__
    );
  }
  if (rank == 0) { printf("Completed checkpoint %d.\n", timestep); fflush(stdout); }

  timestep++;
  }
  MPI_Barrier(MPI_COMM_WORLD);

  if (times > 0) {
    int count = 0;
    double time_start = MPI_Wtime();
    for(t=0; t < times; t++) {
      int rc;
      int all_valid = 1;
      scr_retval = SCR_Start_checkpoint();
      if (scr_retval != SCR_SUCCESS) {
        printf("%d: failed calling SCR_Start_checkpoint(): %d: @%s:%d\n",
               rank, scr_retval, __FILE__, __LINE__
        );
      }
      for (i=0; i < num_files; i++) {
        int valid = 0;
        char file[2094];
        scr_retval = SCR_Route_file(files[i], file);
        if (scr_retval != SCR_SUCCESS) {
          printf("%d: failed calling SCR_Route_file(): %d: @%s:%d\n",
                 rank, scr_retval, __FILE__, __LINE__
          );
        }
        int fd_me = open(file, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
        if (fd_me > 0) {
          count++;
          valid = 1;
          
          // write the checkpoint
          rc = write_checkpoint(fd_me, timestep, bufs[i], filesizes[i]);
          if (rc < 0) { valid = 0; }
          
          rc = fsync(fd_me);
          if (rc < 0) { valid = 0; }
          
          // make sure the close is without error
          rc = close(fd_me);
          if (rc < 0) { valid = 0; }
        }
        if (!valid) { all_valid = 0; }
      }
      scr_retval = SCR_Complete_checkpoint(all_valid);
      if (scr_retval != SCR_SUCCESS) {
        printf("%d: failed calling SCR_Complete_checkpoint(): %d: @%s:%d\n",
               rank, scr_retval, __FILE__, __LINE__
        );
      }
      if (rank == 0) { printf("Completed checkpoint %d.\n", timestep); fflush(stdout); }
      
      timestep++;
      if (seconds > 0) {
        if (rank == 0) { printf("Sleeping for %d seconds... \n", seconds); fflush(stdout); }
        sleep(seconds);
      }
    }
    double time_end = MPI_Wtime();
    double bw = (filesize*count/(1024*1024)) / (time_end - time_start);
    
    MPI_Barrier(MPI_COMM_WORLD);
    
    double bwmin, bwmax, bwsum;
    MPI_Reduce(&bw, &bwmin, 1, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);
    MPI_Reduce(&bw, &bwmax, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(&bw, &bwsum, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
    if (rank == 0) { printf("FileIO: Min %7.2f MB/s\tMax %7.2f MB/s\tAvg %7.2f MB/s\n", bwmin, bwmax, bwsum/size); }
  }

  if (buf != NULL) { free(buf); buf = NULL; }
  for (i=0; i < num_files; i++) {
    if (bufs[i]  != NULL) { free(bufs[i]);  bufs[i]  = NULL; }
    if (files[i] != NULL) { free(files[i]); files[i] = NULL; }
  }
  if (files     != NULL) { free(files);     files     = NULL; }
  if (bufs      != NULL) { free(bufs);      bufs      = NULL; }
  if (filesizes != NULL) { free(filesizes); filesizes = NULL; }

  scr_retval = SCR_Finalize();
  if (scr_retval != SCR_SUCCESS) {
    printf("%d: failed calling SCR_Finalize(): %d: @%s:%d\n",
           rank, scr_retval, __FILE__, __LINE__
    );
  }
  MPI_Finalize();

  fclose(stdout);
  return 0;
}
