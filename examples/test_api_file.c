#define _GNU_SOURCE 1

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
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
int times = 5;
int seconds = 0;
int rank  = -1;
int ranks = 0;

int  timestep = 0;

double getbw(char* name, char* buf, size_t size, int times)
{
  char file[SCR_MAX_FILENAME];
  double bw = 0.0;
  int scr_retval;

  if (times > 0) {
    /* start the timer */
    double time_start = MPI_Wtime();

    /* write the checkpoint file */
    int i, count = 0;
    for(i=0; i < times; i++) {
      int rc;
      int valid = 0;

/*
      int need_checkpoint;
      SCR_Need_checkpoint(&need_checkpoint);
      if (need_checkpoint) {
*/

      /* instruct SCR we are starting the next checkpoint */
      scr_retval = SCR_Start_checkpoint();
      if (scr_retval != SCR_SUCCESS) {
        printf("%d: failed calling SCR_Start_checkpoint(): %d: @%s:%d\n",
               rank, scr_retval, __FILE__, __LINE__
        );
      }

      /* get the file name to write our checkpoint file to */
      char newname[SCR_MAX_FILENAME];
      sprintf(newname, "timestep.%d/%s", timestep, name);
      scr_retval = SCR_Route_file(newname, file);
      if (scr_retval != SCR_SUCCESS) {
        printf("%d: failed calling SCR_Route_file(): %d: @%s:%d\n",
               rank, scr_retval, __FILE__, __LINE__
        );
      }

      /* open the file and write the checkpoint */
      int fd = open(file, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
      if (fd >= 0) {
        count++;
        valid = 1;

        /* write the checkpoint data */
        rc = write_checkpoint(fd, timestep, buf, size);
        if (rc < 0) {
          valid = 0;
          printf("%d: Error writing to %s\n", rank, file);
        }

        /* force the data to storage */
        rc = fsync(fd);
        if (rc < 0) {
          valid = 0;
          printf("%d: Error fsync %s\n", rank, file);
        }

        /* make sure the close is without error */
        rc = close(fd);
        if (rc < 0) {
          valid = 0;
          printf("%d: Error closing %s\n", rank, file);
        }
      }
      else {
      	printf("%d: Could not open file %s\n", rank, file);
      }
      /*
      if( valid )
      	printf("%d: Wrote checkpoint to %s\n", rank, file);
      */

      /* mark this checkpoint as complete */
      scr_retval = SCR_Complete_checkpoint(valid);
      if (scr_retval != SCR_SUCCESS) {
        printf("%d: failed calling SCR_Complete_checkpoint: %d: @%s:%d\n",
               rank, scr_retval, __FILE__, __LINE__
        );
      }
      if (rank == 0) {
        printf("Completed checkpoint %d.\n", timestep);
        fflush(stdout);
      }

/*
      }
*/

      /* increase the timestep counter */
      timestep++;

      /* optionally sleep for some time */
      if (seconds > 0) {
        if (rank == 0) { printf("Sleeping for %d seconds... \n", seconds); fflush(stdout); }
        sleep(seconds);
      }
    }

    /* stop the timer and compute the bandwidth */
    double time_end = MPI_Wtime();
    bw = ((size * count) / (1024*1024)) / (time_end - time_start);
  }

  return bw;
}
int main (int argc, char* argv[])
{

  char *path_to_stdout = NULL;

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

  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &ranks);

  /* open file for stdout */  
  printf("new stdout filename: \"%s\"\n", path_to_stdout);
  fflush(stdout);
  freopen(path_to_stdout, "a+", stdout);
  MPI_Barrier(MPI_COMM_WORLD);
  

  /* time how long it takes to get through init */
  MPI_Barrier(MPI_COMM_WORLD);
  printf("%d: rank startup before scr_init\n", rank);
  double init_start = MPI_Wtime();
  if (SCR_Init() != SCR_SUCCESS){
    printf("FAILED INITIALIZING SCR\n");
    fclose(stdout);
    return -1;
  }
  double init_end = MPI_Wtime();
  double secs = init_end - init_start;
  MPI_Barrier(MPI_COMM_WORLD);
  printf("%d: rank startup after scr_init\n", rank);

  /* compute and print the init stats */
  double secsmin, secsmax, secssum;
  MPI_Reduce(&secs, &secsmin, 1, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);
  MPI_Reduce(&secs, &secsmax, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
  MPI_Reduce(&secs, &secssum, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
  if (rank == 0) {
    printf("Init: Min %8.6f s\tMax %8.6f s\tAvg %8.6f s\n", secsmin, secsmax, secssum/ranks);
  }

  MPI_Barrier(MPI_COMM_WORLD);

  /* allocate space for the checkpoint data (make filesize a function of rank for some variation) */
  filesize = filesize + rank;
  char* buf = (char*) malloc(filesize);
  
  /* get the name of our checkpoint file to open for read on restart */
  char name[256];
  char file[SCR_MAX_FILENAME];
  sprintf(name, "rank_%d.ckpt", rank);
  int found_checkpoint = 0;
  if (SCR_Route_file(name, file) == SCR_SUCCESS) {
    if (read_checkpoint_file(file, &timestep, buf, filesize)) {
      /* read the file ok, now check that contents are good */
      found_checkpoint = 1;
      printf("%d: Successfully read checkpoint from %s\n", rank, file);
      if (!check_buffer(buf, filesize, rank, timestep)) {
        printf("%d: Invalid value in buffer\n", rank);
        MPI_Abort(MPI_COMM_WORLD, 1);
	fclose(stdout);
        return 1;
      }
    } else {
    	printf("%d: Could not read checkpoint %d from %s\n", rank, timestep, file);
    }
  } else
    printf("%d: SCR_Route_file failed during restart attempt\n", rank);

  /* determine whether all tasks successfully read their checkpoint file */
  int all_found_checkpoint = 0;
  MPI_Allreduce(&found_checkpoint, &all_found_checkpoint, 1, MPI_INT, MPI_LAND, MPI_COMM_WORLD);
  if (!all_found_checkpoint && rank == 0) {
    printf("At least one rank (perhaps all) did not find its checkpoint\n");
  }

  /* check that everyone is at the same timestep */
  int timestep_and, timestep_or;
  MPI_Allreduce(&timestep, &timestep_and, 1, MPI_INT, MPI_BAND, MPI_COMM_WORLD);
  MPI_Allreduce(&timestep, &timestep_or,  1, MPI_INT, MPI_BOR,  MPI_COMM_WORLD);
  if (timestep_and != timestep_or) {
    printf("%d: Timesteps don't agree: timestep %d\n", rank, timestep);
    fclose(stdout);
    return 1;
  }

  /* make up some data for the next checkpoint */
  init_buffer(buf, filesize, rank, timestep);

  timestep++;

  /* prime system once before timing */
  getbw(name, buf, filesize, 1);

  /* now compute the bandwidth and print stats */
  if (times > 0) {
    double bw = getbw(name, buf, filesize, times);

    MPI_Barrier(MPI_COMM_WORLD);

    /* compute stats and print them to the screen */
    double bwmin, bwmax, bwsum;
    MPI_Reduce(&bw, &bwmin, 1, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);
    MPI_Reduce(&bw, &bwmax, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(&bw, &bwsum, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
    if (rank == 0) {
      printf("FileIO: Min %7.2f MB/s\tMax %7.2f MB/s\tAvg %7.2f MB/s\tAgg %7.2f MB/s\n",
             bwmin, bwmax, bwsum/ranks, bwsum
      );
    }
  }

  if (buf != NULL) {
    free(buf);
    buf = NULL;
  }

  SCR_Finalize();
  MPI_Finalize();
  fclose(stdout);
  return 0;
}
