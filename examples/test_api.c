#define _GNU_SOURCE 1

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <getopt.h>
#include <string.h>

/* pull in things like ULLONG_MAX */
#include <limits.h>
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
int ckptout = 0;
int output = 0;

int rank  = -1;
int ranks = 0;

int  timestep = 0;

static unsigned long long kilo  =                1024ULL;
static unsigned long long mega  =             1048576ULL;
static unsigned long long giga  =          1073741824ULL;
static unsigned long long tera  =       1099511627776ULL;
static unsigned long long peta  =    1125899906842624ULL;
static unsigned long long exa   = 1152921504606846976ULL;

/* abtoull ==> ASCII bytes to unsigned long long
 * Converts string like "10mb" to unsigned long long integer value
 * of 10*1024*1024.  Input string should have leading number followed
 * by optional units.  The leading number can be a floating point
 * value (read by strtod).  The trailing units consist of one or two
 * letters which should be attached to the number with no space
 * in between.  The units may be upper or lower case, and the second
 * letter if it exists, must be 'b' or 'B' (short for bytes).
 *
 * Valid units: k,K,m,M,g,G,t,T,p,P,e,E
 *
 * Examples: 2kb, 1.5m, 200GB, 1.4T.
 *
 * Returns SCR_SUCCESS if conversion is successful,
 * and !SCR_SUCCESS otherwise.
 *
 * Returns converted value in val parameter.  This
 * parameter is only updated if successful. */
#define SCR_FAILURE (!SCR_SUCCESS)
int test_abtoull(char* str, unsigned long long* val)
{
  /* check that we have a string */
  if (str == NULL) {
    printf("test_abtoull: Can't convert NULL string to bytes @ %s:%d",
           __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* check that we have a value to write to */
  if (val == NULL) {
    printf("test_abtoull: NULL address to store value @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* pull the floating point portion of our byte string off */
  errno = 0;
  char* next = NULL;
  double num = strtod(str, &next);
  if (errno != 0) {
    /* conversion failed */
    printf("test_abtoull: Invalid double: %s errno=%d %s @ %s:%d",
           str, errno, strerror(errno), __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }
  if (str == next) {
    /* no conversion performed */
    printf("test_abtoull: Invalid double: %s @ %s:%d",
           str, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* now extract any units, e.g. KB MB GB, etc */
  unsigned long long units = 1;
  if (*next != '\0') {
    switch(*next) {
    case 'k':
    case 'K':
      units = kilo;
      break;
    case 'm':
    case 'M':
      units = mega;
      break;
    case 'g':
    case 'G':
      units = giga;
      break;
    case 't':
    case 'T':
      units = tera;
      break;
    case 'p':
    case 'P':
      units = peta;
      break;
    case 'e':
    case 'E':
      units = exa;
      break;
    default:
      printf("test_abtoull: Unexpected byte string %s @ %s:%d",
             str, __FILE__, __LINE__
      );
      return SCR_FAILURE;
    }

    next++;

    /* handle optional b or B character, e.g. in 10KB */
    if (*next == 'b' || *next == 'B') {
      next++;
    }

    /* check that we've hit the end of the string */
    if (*next != 0) {
      printf("test_abtoull: Unexpected byte string: %s @ %s:%d",
             str, __FILE__, __LINE__
      );
      return SCR_FAILURE;
    }
  }

  /* check that we got a positive value */
  if (num < 0) {
    printf("test_abtoull: Byte string must be positive: %s @ %s:%d",
           str, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* TODO: double check this overflow calculation */
  /* multiply by our units and check for overflow */
  double units_d = (double) units;
  double val_d = num * units_d;
  double max_d = (double) ULLONG_MAX;
  if (val_d > max_d) {
    /* overflow */
    printf("test_abtoull: Byte string overflowed UNSIGNED LONG LONG type: %s @ %s:%d",
           str, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* set return value */
  *val = (unsigned long long) val_d;

  return SCR_SUCCESS;
}

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
      char label[SCR_MAX_FILENAME];
      int flags = SCR_FLAG_NONE;

      if (output > 0 && timestep % output == 0) {
        /* if output is enabled, mark every Nth as pure output */
        flags |= SCR_FLAG_OUTPUT;
        sprintf(label, "output.%d", timestep);
      } else {
        /* otherwise we have a checkpoint */
        flags |= SCR_FLAG_CHECKPOINT;
        sprintf(label, "ckpt.%d", timestep);
      }

      /* if ckptout is enabled, mark every Nth write as output also */
      if (ckptout > 0 && timestep % ckptout == 0) {
        flags |= SCR_FLAG_OUTPUT;
      }

      scr_retval = SCR_Start_output(label, flags);
      if (scr_retval != SCR_SUCCESS) {
        printf("%d: failed calling SCR_Start_checkpoint(): %d: @%s:%d\n",
               rank, scr_retval, __FILE__, __LINE__
        );
      }

      /* get the file name to write our checkpoint file to */
      char newname[SCR_MAX_FILENAME];
      sprintf(newname, "%s/%s", label, name);
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

void print_usage()
{
  printf("\n");
  printf("  Usage: test_api [options]\n");
  printf("\n");
  printf("  Options:\n");
  printf("    -s, --size=<SIZE>    Filesize in bytes, e.g., 1MB (default %lu)\n", (unsigned long) filesize);
  printf("    -t, --times=<COUNT>  Number of iterations (default %d)\n", times);
  printf("    -z, --seconds=<SECS> Sleep for SECS seconds between iterations (default %d)\n", seconds);
  printf("    -f, --flush=<COUNT>  Mark every Nth write as checkpoint+output (default %d)\n", ckptout);
  printf("    -o, --output=<COUNT> Mark every Nth write as pure output (default %d)\n", output);
  printf("    -h, --help           Print usage\n");
  printf("\n");
  return;
}

int main (int argc, char* argv[])
{
  MPI_Init(&argc, &argv);

  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &ranks);

  static const char *opt_string = "s:t:z:f:o:h";
  static struct option long_options[] = {
    {"size",    required_argument, NULL, 's'},
    {"times",   required_argument, NULL, 't'},
    {"seconds", required_argument, NULL, 'z'},
    {"flush",   required_argument, NULL, 'f'},
    {"output",  required_argument, NULL, 'o'},
    {"help",    no_argument,       NULL, 'h'},
    {NULL,      no_argument,       NULL,   0}
  };

  int usage = 0;
  int long_index = 0;
  int opt = getopt_long(argc, argv, opt_string, long_options, &long_index);
  unsigned long long val;
  while (opt != -1) {
    switch(opt) {
      case 's':
        if (test_abtoull(optarg, &val) == SCR_SUCCESS) {
          filesize = (size_t) val;
        } else {
          usage = 1;
        }
        break;
      case 't':
        times = atoi(optarg);
        break;
      case 'z':
        seconds = atoi(optarg);
        break;
      case 'f':
        ckptout = atoi(optarg);
        break;
      case 'o':
        output = atoi(optarg);
        break;
      case 'h':
      default:
        usage = 1;
        break;
    }

    /* get the next option */
    opt = getopt_long(argc, argv, opt_string, long_options, &long_index);
  }

  /* check that we got an appropriate number of arguments */
  if (usage) {
    if (rank == 0) {
      print_usage();
    }
    MPI_Finalize();
    return 1;
  }

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
