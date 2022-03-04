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

size_t my_filesize = 512*1024;
size_t total_filesize = 0;
size_t my_bufsize = 0;
size_t my_file_offset = 0;

int times = 5;
int seconds = 0;
int ckptout = 0;
int output = 0;
int use_config_api = 0;
int use_conf_file = 1;

int use_fsync = 1; /* whether to fsync files after writing */

int use_shared_file = 0;  /* --shared_file */
char* path = NULL;
int use_scr = 1;
int use_scr_restart = 1;

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
#ifndef SCR_FAILURE
#define SCR_FAILURE (!SCR_SUCCESS)
#endif
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

size_t get_my_file_offset()
{
  uint64_t file_offset = 0;

  if (use_shared_file) {
    uint64_t send_buf = my_filesize;
    MPI_Scan(&send_buf, &file_offset, 1, MPI_UINT64_T, MPI_SUM, MPI_COMM_WORLD);
    file_offset -= my_filesize;
  }

  return file_offset;
}

int create_file(char* file)
{
  int fd;

  if (use_shared_file) {
    if (rank == 0) {
      if ( (fd = open(file, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR)) >= 0) {
        if (truncate(file, total_filesize) < 0) {
          printf("%d: Could not truncate file %s : %s\n", rank, file, strerror(errno));
          close(fd);
          fd = -1;
        }
      }
      else {
        printf("%d: Could not create file %s : %s\n", rank, file, strerror(errno));
      }

      if (fd >= 0) {
        close(fd);
      };
    }

    MPI_Bcast(&fd, 1, MPI_INT, 0, MPI_COMM_WORLD); /* Wait for rank 0 to complete creation */

    if (fd >= 0) {
      if ((fd = open(file, O_WRONLY)) < 0) {
        printf("%d: Could not open file %s : %s\n", rank, file, strerror(errno));
      }
    }
  }
  else {
    if ( (fd = open(file, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR)) < 0) {
      printf("%d: Could not create file %s : %s\n", rank, file, strerror(errno));
    }
  }

  return fd;
}

double getbw(char* name, char* buf, int times)
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

      double time_start_output;
      if (rank == 0) {
        time_start_output = MPI_Wtime();
      }

      /* instruct SCR we are starting the next checkpoint */
      int flags = SCR_FLAG_NONE;
      char outname[SCR_MAX_FILENAME];
      if (output > 0 && timestep % output == 0) {
        /* if output is enabled, mark every Nth as pure output */
        flags |= SCR_FLAG_OUTPUT;
        safe_snprintf(outname, sizeof(outname), "output.%d", timestep);
      } else {
        /* otherwise we have a checkpoint */
        flags |= SCR_FLAG_CHECKPOINT;
        safe_snprintf(outname, sizeof(outname), "ckpt.%d", timestep);
      }

      /* if ckptout is enabled, mark every Nth write as output also */
      if (ckptout > 0 && timestep % ckptout == 0) {
        flags |= SCR_FLAG_OUTPUT;
      }

      /* compute directory path to hold output files */
      char outpath[SCR_MAX_FILENAME];
      if (path != NULL) {
        safe_snprintf(outpath, sizeof(outpath), "%s/%s", path, outname);
      } else {
        safe_snprintf(outpath, sizeof(outpath), "%s", outname);
      }

      if (use_scr) {
        /* using scr, start our output */
        scr_retval = SCR_Start_output(outname, flags);
        if (scr_retval != SCR_SUCCESS) {
          printf("%d: failed calling SCR_Start_checkpoint(): %d: @%s:%d\n",
                 rank, scr_retval, __FILE__, __LINE__
          );
        }
      } else {
        /* not using SCR, writing to file system instead,
         * need to create our directory */
        if (rank == 0) {
           rc = mkdir(outpath, S_IRUSR | S_IWUSR | S_IXUSR);
           if (rc != 0 && errno != EEXIST) {
             printf("%d: mkdir failed: %s %d %s @%s:%d\n",
                    rank, outpath, errno, strerror(errno), __FILE__, __LINE__
             );
           }
        }
        MPI_Barrier(MPI_COMM_WORLD);
      }

      /* define name of our file */
      char newname[SCR_MAX_FILENAME];
      safe_snprintf(newname, sizeof(newname), "%s/%s", outpath, name);

      /* get the file name to write our file to */
      if (use_scr) {
        scr_retval = SCR_Route_file(newname, file);
        if (scr_retval != SCR_SUCCESS) {
          printf("%d: failed calling SCR_Route_file(): %d: @%s:%d\n",
                 rank, scr_retval, __FILE__, __LINE__
          );
        }
      } else {
        /* not using scr, keep path as is */
        strcpy(file, newname);
      }

      int my_fd = create_file(file);

      /* write the checkpoint and close */
      if (my_fd >= 0) {
        count++;
        valid = 1;

        if (lseek(my_fd, my_file_offset, SEEK_SET) >= 0) {
          /* write the checkpoint data */
          if (write_checkpoint(my_fd, timestep, buf, my_bufsize)) {
            /* force the data to storage */
            if (use_fsync) {
              if (fsync(my_fd) < 0) {
                valid = 0;
                printf("%d: Error fsync %s\n", rank, file);
              }
            }
          }
          else {
            valid = 0;
            printf("%d: Error writing checkpoint %s\n", rank, file);
          }
        }
        else {
            printf("%d: Failed to seek to 0x%08lx in %s : %s\n", rank, my_file_offset, file, strerror(errno));
            valid = 0;
        }

        /* make sure the close is without error */
        if (close(my_fd) < 0) {
          valid = 0;
          printf("%d: Error closing %s\n", rank, file);
        }
      }

      /* mark this checkpoint as complete */
      if (use_scr) {
        scr_retval = SCR_Complete_output(valid);
        if (scr_retval != SCR_SUCCESS) {
          printf("%d: failed calling SCR_Complete_output: %d: @%s:%d\n",
                 rank, scr_retval, __FILE__, __LINE__
          );
        }
      } else {
        /* wait for all tasks to finish */
        MPI_Barrier(MPI_COMM_WORLD);
      }

      if (rank == 0) {
        double time_end_output = MPI_Wtime();
        double time_secs = time_end_output - time_start_output;
        double bytes = my_filesize * ranks;
        bw = bytes / time_secs;
        printf("Completed checkpoint %d:  %f secs, %e bytes, %e bytes/sec\n", timestep, time_secs, bytes, bw);
        fflush(stdout);
      }

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
    bw = ((my_filesize * count) / (1024*1024)) / (time_end - time_start);
  }

  return bw;
}

/* convert a string to bool
 * acceotable strings for true (1): yes, true, y, 1
 * acceptable strings for false(0): no, false, n, 0
 * all other strings are errors and return -1
 */
int atob(const char *s)
{
  if (strcmp(s, "yes") == 0 || strcmp(s, "true") == 0 || strcmp(s, "y") == 0 ||
      strcmp(s, "1") == 0) {
    return 0;
  } else if (strcmp(s, "no") == 0 || strcmp(s, "false") == 0 || strcmp(s, "n") == 0 ||
             strcmp(s, "0") == 0) {
    return 1;
  } else {
    return -1;
  }
}

/* convert a truth value to "yes" or "no" */
const char *btoa(const int b)
{
  if (b) {
    return "yes";
  } else {
    return "no";
  }
}

int restart_scr(char* name, char* buf)
{
  /* get the name of our checkpoint file to open for read on restart */
  int scr_retval;
  int found_checkpoint = 0;
  char dset[SCR_MAX_FILENAME];

  int have_restart = 0;
  int restarted = 0;
  do {
    scr_retval = SCR_Have_restart(&have_restart, dset);
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
      scr_retval = SCR_Start_restart(dset);
      if (scr_retval != SCR_SUCCESS) {
        printf("%d: failed calling SCR_Start_restart: %d: @%s:%d\n",
               rank, scr_retval, __FILE__, __LINE__
        );
      }

      /* include checkpoint directory path in name */
      char newname[SCR_MAX_FILENAME];
      safe_snprintf(newname, sizeof(newname), "%s/%s", dset, name);

      /* compute directory path to hold output files */
      char outpath[SCR_MAX_FILENAME];
      if (path != NULL) {
        safe_snprintf(outpath, sizeof(outpath), "%s/%s", path, newname);
      } else {
        safe_snprintf(outpath, sizeof(outpath), "%s", newname);
      }

      /* get our file name */
      char file[SCR_MAX_FILENAME];
      scr_retval = SCR_Route_file(outpath, file);
      if (scr_retval != SCR_SUCCESS) {
        printf("%d: failed calling SCR_Route_file: %d: @%s:%d\n",
               rank, scr_retval, __FILE__, __LINE__
        );
      }


      /* read the data */
      int rc;

      rc = use_shared_file ?
          read_shared_checkpoint(file, &timestep, buf, my_bufsize, my_file_offset)
        : read_checkpoint(file, &timestep, buf, my_bufsize);

      if (rc) {
        /* read the file ok, now check that contents are good */
        found_checkpoint = 1;
        if (!check_buffer(buf, my_bufsize, rank, timestep)) {
          printf("%d: Invalid value in buffer\n", rank);
          found_checkpoint = 0;
        }
      } else {
        printf("%d: Could not read checkpoint %d from %s\n", rank, timestep, file);
        found_checkpoint = 0;
      }

      /* indicate to library that we're done with restart, tell it whether we read our data ok */
      scr_retval = SCR_Complete_restart(found_checkpoint);
      if (scr_retval == SCR_SUCCESS) {
        /* all procs succeeded in reading their checkpoint file,
         * we've successfully restarted */
        restarted = 1;
      } else {
        printf("%d: failed calling SCR_Complete_restart: %d: @%s:%d\n",
               rank, scr_retval, __FILE__, __LINE__
        );
      }
    }
  } while (have_restart && !restarted);

  /* determine whether all tasks successfully read their checkpoint file */
  if (!restarted) {
    /* failed to read restart, reset timestep counter */
    timestep = 0;
    if (rank == 0) {
      printf("At least one rank (perhaps all) did not find its checkpoint\n");
    }
    return -1;
  }
  return 0;
}

int restart(char* dset, char* name, char* buf)
{
  if (rank == 0) {
    printf("Restarting from checkpoint named %s\n", dset);
  }

  /* include checkpoint directory path in name */
  char newname[SCR_MAX_FILENAME];
  safe_snprintf(newname, sizeof(newname), "%s/%s", dset, name);

  /* compute directory path to hold output files */
  char outpath[SCR_MAX_FILENAME];
  if (path != NULL) {
    safe_snprintf(outpath, sizeof(outpath), "%s/%s", path, newname);
  } else {
    safe_snprintf(outpath, sizeof(outpath), "%s", newname);
  }

  /* read the data */
  int found_checkpoint = 0;
  int rc;

  rc = use_shared_file
        ? read_shared_checkpoint(outpath, &timestep, buf, my_bufsize, my_file_offset)
        : read_checkpoint(outpath, &timestep, buf, my_bufsize);

  if (rc) {
    /* read the file ok, now check that contents are good */
    found_checkpoint = 1;
    if (!check_buffer(buf, my_bufsize, rank, timestep)) {
      printf("%d: Invalid value in buffer\n", rank);
      found_checkpoint = 0;
    }
  } else {
    printf("%d: Could not read checkpoint %d from %s\n", rank, timestep, outpath);
    found_checkpoint = 0;
  }

  int restarted;
  MPI_Allreduce(&found_checkpoint, &restarted, 1, MPI_INT, MPI_LAND, MPI_COMM_WORLD);

  /* determine whether all tasks successfully read their checkpoint file */
  if (!restarted) {
    /* failed to read restart, reset timestep counter */
    timestep = 0;
    if (rank == 0) {
      printf("At least one rank (perhaps all) did not find its checkpoint\n");
    }
    return -1;
  }
  return 0;
}

void print_usage()
{
  printf("\n");
  printf("  Usage: test_api [options]\n");
  printf("\n");
  printf("  Options:\n");
  printf("    -s, --size=<SIZE>    Rank checkpoint size in bytes, e.g., 1MB (default %lu)\n", (unsigned long) my_filesize);
  printf("    -t, --times=<COUNT>  Number of iterations (default %d)\n", times);
  printf("    -z, --seconds=<SECS> Sleep for SECS seconds between iterations (default %d)\n", seconds);
  printf("    -p, --path=<DIR>     Directory to create and write files to\n");
  printf("    -f, --flush=<COUNT>  Mark every Nth write as checkpoint+output (default %d)\n", ckptout);
  printf("    -o, --output=<COUNT> Mark every Nth write as pure output (default %d)\n", output);
  printf("    -a, --config-api=<BOOL> Use SCR_Config to set values (default %s)\n", btoa(use_config_api));
  printf("    -c, --conf-file=<BOOL>  Use SCR_CONF_FILE file to set values (default %s)\n", btoa(use_conf_file));
  printf("        --current=<CKPT> Specify checkpoint name to load on restart\n");
  printf("        --nofsync        Disable fsync after writing files\n");
  printf("        --noscr          Disable SCR calls\n");
  printf("        --noscrrestart   Disable SCR restart calls\n");
  printf("        --shared-file    Use single shared file instead of file per rank");
  printf("    -h, --help           Print usage\n");
  printf("\n");
  return;
}

int main (int argc, char* argv[])
{
  MPI_Init(&argc, &argv);

  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &ranks);

  static const char *opt_string = "s:t:z:p:f:o:a:c:h";
  static struct option long_options[] = {
    {"size",    required_argument, NULL, 's'},
    {"times",   required_argument, NULL, 't'},
    {"seconds", required_argument, NULL, 'z'},
    {"path",    required_argument, NULL, 'p'},
    {"flush",   required_argument, NULL, 'f'},
    {"output",  required_argument, NULL, 'o'},
    {"config-api", required_argument, NULL, 'a'},
    {"conf-file",  required_argument, NULL, 'c'},
    {"current", required_argument, NULL, 'C'},
    {"nofsync", no_argument,       NULL, 'S'},
    {"noscr",   no_argument,       NULL, 'x'},
    {"noscrrestart", no_argument,  NULL, 'X'},
    {"shared-file", no_argument,   NULL, 'y'},
    {"help",    no_argument,       NULL, 'h'},
    {NULL,      no_argument,       NULL,   0}
  };

  int usage = 0;
  int long_index = 0;
  int opt = getopt_long(argc, argv, opt_string, long_options, &long_index);
  char* current = NULL;
  unsigned long long val;
  while (opt != -1) {
    switch(opt) {
      case 's':
        if (test_abtoull(optarg, &val) == SCR_SUCCESS) {
          my_filesize = (size_t) val;
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
      case 'p':
        path = strdup(optarg);
        break;
      case 'f':
        ckptout = atoi(optarg);
        break;
      case 'o':
        output = atoi(optarg);
        break;
      case 'a':
        use_config_api = atob(optarg);
        break;
      case 'c':
        use_conf_file = atob(optarg);
        break;
      case 'C':
        current = strdup(optarg);
        break;
      case 'S':
        use_fsync = 0;
        break;
      case 'x':
        use_scr = 0;
        break;
      case 'X':
        use_scr_restart = 0;
        break;
      case 'y':
        use_shared_file = 1;
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
  if (use_scr) {
    if (!use_conf_file) {
      unsetenv("SCR_CONF_FILE");
    }

    if (use_config_api) {
      SCR_Config("STORE=/dev/shm GROUP=NODE COUNT=1");
      SCR_Config("SCR_COPY_TYPE=FILE");
      SCR_Config("CKPT=0 INTERVAL=1 GROUP=NODE STORE=/dev/shm TYPE=XOR SET_SIZE=16");
      SCR_Config("SCR_DEBUG=1");
    }

    if (use_shared_file) {
      SCR_Config("SCR_CACHE_BYPASS=1");
    }

    if (SCR_Init() != SCR_SUCCESS){
      printf("Failed initializing SCR\n");
      return 1;
    }

    /* specify name of checkpoint to load if given */
    if (current != NULL) {
      SCR_Current(current);
    }
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

  /* allocate space for the checkpoint data (make my_filesize a function of rank for some variation) */
  my_filesize = my_filesize + rank;
  my_bufsize = my_filesize;

  /* also account for the checkpoint header that is written (no need to adjust bufsize) */
  my_filesize += checkpoint_timestep_size();

  MPI_Reduce(&my_filesize, &total_filesize, 1, MPI_UINT64_T, MPI_SUM, 0, MPI_COMM_WORLD);
  my_file_offset = get_my_file_offset();

  char* buf = (char*) malloc(my_filesize);

  /* define base name for our checkpoint files */
  char name[256];
  if (use_shared_file) {
    safe_snprintf(name, sizeof(name), "rank_shared.ckpt");
  }
  else {
    safe_snprintf(name, sizeof(name), "rank_%d.ckpt", rank);
  }

  /* get the name of our checkpoint file to open for read on restart */
  if (use_scr && use_scr_restart) {
    restart_scr(name, buf);
  } else {
    if (current) {
      restart(current, name, buf);
    }
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
  init_buffer(buf, my_bufsize, rank, timestep);

  timestep++;

  /* prime system once before timing */
  getbw(name, buf, 1);

  /* now compute the bandwidth and print stats */
  if (times > 0) {
    double bw = getbw(name, buf, times);

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

  if (current != NULL) {
    free(current);
    current = NULL;
  }

  if (path != NULL) {
    free(path);
    path = NULL;
  }

  if (buf != NULL) {
    free(buf);
    buf = NULL;
  }

  if (use_scr) {
    SCR_Finalize();
  }
  MPI_Finalize();

  return 0;
}
