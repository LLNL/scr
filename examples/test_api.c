#define _GNU_SOURCE 1

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
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
      int flags = SCR_FLAG_NONE;
      if (timestep % 6 != 0) {
        flags |= SCR_FLAG_CHECKPOINT;
      }
      if (timestep % 3 == 0) {
        flags |= SCR_FLAG_OUTPUT;
      }
      char label[SCR_MAX_FILENAME];
      sprintf(label, "timestep.%d", timestep);
      scr_retval = SCR_Start_output(label, flags);
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
      int fd_me = open(file, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
      if (fd_me > 0) {
        count++;
        valid = 1;

        /* write the checkpoint data */
        rc = write_checkpoint(fd_me, timestep, buf, size);
        if (rc < 0) {
          valid = 0;
          printf("%d: Error writing to %s\n", rank, file);
        }

        /* force the data to storage */
        rc = fsync(fd_me);
        if (rc < 0) {
          valid = 0;
          printf("%d: Error fsync %s\n", rank, file);
        }

        /* make sure the close is without error */
        rc = close(fd_me);
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
      scr_retval = SCR_Complete_output(valid);
      if (scr_retval != SCR_SUCCESS) {
        printf("%d: failed calling SCR_Complete_output: %d: @%s:%d\n",
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
  /* check that we got an appropriate number of arguments */
  if (argc != 1 && argc != 4) {
    printf("Usage: test_correctness [filesize times sleep_secs]\n");
    return 1;
  }

  /* read parameters from command line, if any */
  if (argc > 1) {
    filesize = (size_t) atol(argv[1]);
    times = atoi(argv[2]);
    seconds = atoi(argv[3]);
  }

  MPI_Init(&argc, &argv);

  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &ranks);

  /* time how long it takes to get through init */
  MPI_Barrier(MPI_COMM_WORLD);
  double init_start = MPI_Wtime();
  if (SCR_Init() != SCR_SUCCESS){
    printf("Failed initializing SCR\n");
    return 1;
  }

  double init_end = MPI_Wtime();
  double secs = init_end - init_start;
  MPI_Barrier(MPI_COMM_WORLD);

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
  
  /* define base name for our checkpoint files */
  char name[256];
  sprintf(name, "rank_%d.ckpt", rank);

  /* get the name of our checkpoint file to open for read on restart */
  int found_checkpoint = 0;
  int have_restart;
  char dset[SCR_MAX_FILENAME];
  int scr_retval = SCR_Have_restart(&have_restart, dset);
  if (scr_retval != SCR_SUCCESS) {
    printf("%d: failed calling SCR_Have_restart: %d: @%s:%d\n",
           rank, scr_retval, __FILE__, __LINE__
    );
  }

  if (have_restart) {
    if (rank == 0) {
      printf("Restarting from checkpoint named %s\n", dset);
    }

    /* indicate to library that we're start to read our restart */
    SCR_Start_restart(dset);
    if (scr_retval != SCR_SUCCESS) {
      printf("%d: failed calling SCR_Start_restart: %d: @%s:%d\n",
             rank, scr_retval, __FILE__, __LINE__
      );
    }

    /* get our file name */
    char file[SCR_MAX_FILENAME];
    scr_retval = SCR_Route_file(name, file);
    if (scr_retval != SCR_SUCCESS) {
      printf("%d: failed calling SCR_Route_file: %d: @%s:%d\n",
             rank, scr_retval, __FILE__, __LINE__
      );
    }

    /* read the data */
    if (read_checkpoint(file, &timestep, buf, filesize)) {
      /* read the file ok, now check that contents are good */
      found_checkpoint = 1;
      if (!check_buffer(buf, filesize, rank, timestep)) {
        printf("%d: Invalid value in buffer\n", rank);
        MPI_Abort(MPI_COMM_WORLD, 1);
        return 1;
      }
    } else {
    	printf("%d: Could not read checkpoint %d from %s\n", rank, timestep, file);
    }

    /* indicate to library that we're done with restart, tell it whether we read our data ok */
    scr_retval = SCR_Complete_restart(found_checkpoint);
    if (scr_retval != SCR_SUCCESS) {
      printf("%d: failed calling SCR_Complete_restart: %d: @%s:%d\n",
             rank, scr_retval, __FILE__, __LINE__
      );
    }
  } else {
    if (rank == 0) {
      printf("No checkpoint to restart from\n");
    }
  }

  /* determine whether all tasks successfully read their checkpoint file */
  int all_found_checkpoint = 0;
  MPI_Allreduce(&have_restart, &all_found_checkpoint, 1, MPI_INT, MPI_LAND, MPI_COMM_WORLD);
  if (!all_found_checkpoint && rank == 0) {
    printf("At least one rank (perhaps all) did not find its checkpoint\n");
  }

  /* check that everyone is at the same timestep */
  int timestep_and, timestep_or;
  MPI_Allreduce(&timestep, &timestep_and, 1, MPI_INT, MPI_BAND, MPI_COMM_WORLD);
  MPI_Allreduce(&timestep, &timestep_or,  1, MPI_INT, MPI_BOR,  MPI_COMM_WORLD);
  if (timestep_and != timestep_or) {
    printf("%d: Timesteps don't agree: timestep %d\n", rank, timestep);
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

  return 0;
}
