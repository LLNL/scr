/*
 * Copyright (c) 2009, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory.
 * Written by Adam Moody <moody20@llnl.gov>.
 * LLNL-CODE-411039.
 * All rights reserved.
 * This file is part of The Scalable Checkpoint / Restart (SCR) library.
 * For details, see https://sourceforge.net/projects/scalablecr/
 * Please also read this file: LICENSE.TXT.
*/

/* This code builds a library which can be interpositioned into an existing binary
 * such that it may utilize SCR without requiring any changes to the application
 * or even a rebuild.  This library intercepts the following calls made by the
 * application:
 *   1) MPI_Init() to call SCR_Init() after returning from MPI_Init()
 *   2) MPI_Finalize() to call SCR_Finalize() before calling MPI_Finalize()
 *   3) open()/fopen() to call SCR_Start_checkpoint() and/or SCR_Route_file()
 *      before opening the file
 *   4) close()/fclose() to call SCR_Complete_checkpoint() after closing file
 *
 * This library determines which files are checkpoint files by comparing them
 * to a regular expression provided by the user via an environment variable.
 *
 * Here are some articles and examples on interposing libraries:
 *   http://www.cs.cmu.edu/afs/cs.cmu.edu/academic/class/15213-s03/src/interposition/mymalloc.c
 *   http://nixforums.org/about36762.html
 *   http://unix.derkeiler.com/Newsgroups/comp.unix.programmer/2004-05/0178.html
*/

/* need this to pick up correct definitions for strndup and RTLD_NEXT */
#define  _GNU_SOURCE

#include <dlfcn.h>

#include <stdio.h>
#include <stdlib.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdarg.h>

#include <string.h>
#include <regex.h>

#include <unistd.h>
#include <libgen.h>

#include <errno.h>

#include "mpi.h"
#include "scr.h"

static int scri_initialized       = 0;
static int scri_interpose_enabled = 0;
static int scri_in_checkpoint     = 0;
static int scri_ranks             = 0;
static int scri_rank              = -1;

static int scri_re_low_high_compiled = 0;
static int scri_re_low_N_compiled    = 0;
static int scri_re_scr_file_compiled = 0;
static regex_t scri_re_low_high;
static regex_t scri_re_low_N;
static regex_t scri_re_scr_file;

/* interpose MPI functions */
int (* scri_real_mpi_init)  (int *, char ***) = NULL;
int (* scri_real_mpi_fini)  ()                = NULL;

/* interpose open/close functions */
/*
int (* scri_real_open)      (const char *, int, mode_t);
*/
int (* scri_real_open)      (const char *, int, ...) = NULL;
int (* scri_real_close)     (int)                    = NULL;

/* interpose fopen/fclose functions */
FILE* (* scri_real_fopen)   (const char *, const char *) = NULL;
int   (* scri_real_fclose)  (FILE*)                      = NULL;

/* interpose mkdir function */
int (* scri_real_mkdir)     (const char *, mode_t) = NULL;

/* TODO: also intercept
 *   creat() -- form of open()
 *   mkdir() -- for checkpoint directories -- turn into a NOP
 */

/* interpose read/write functions */
/*
ssize_t (* real_read)  (int fd, void *buf, size_t count);
ssize_t (* real_write) (int fd, const void *buf, size_t count);
*/

/*
==============================================================================
Checkpoint tracking functions and data structures
==============================================================================
*/

#ifndef MAX_CHECKPOINT_FILES
#define MAX_CHECKPOINT_FILES (8)
#endif

#define SCRI_FNULL   (0)
#define SCRI_FD      (1)
#define SCRI_FSTREAM (2)

struct scri_checkpointfile
{
  int   valid;   /* whether checkpoint file entry is valid */
  int   enabled; /* whether open/close interposing is currently enabled */
  int   need_closed; /* whether checkpoint file is open and needs to be closed to complete a checkpoint */
  char* filename;
  char* tempname;
  regex_t re;
  int   ftype;
  int   fd;
  int   flags;
  FILE* fstream;
  char* mode;
};

/* TODO: change this fixed array to a linked list */
/* keeps track of checkpoint files */
static int    scri_checkpoint_files_valid = 0;
static struct scri_checkpointfile scri_checkpoint_files[MAX_CHECKPOINT_FILES];

/* TODO: support a list of directories like we do for files */
/* keeps track of checkpoint directory */
static int     scri_checkpoint_dir_valid = 0;
static regex_t scri_re_checkpoint_dir;

/* given a filename and regular expression, return whether there is a match */
static int scri_file_matches(const char* filename, regex_t* re)
{
  size_t nmatch = 0;
  regmatch_t pmatch[5];

  /* check for a match on the filename, and check that it's *not* an .scr file */
  memset(pmatch, 0, sizeof(regmatch_t) * nmatch);
  if (regexec(re, filename, nmatch, pmatch, 0) == 0 &&
      regexec(&scri_re_scr_file, filename, nmatch, pmatch, 0) == REG_NOMATCH)
  {
    return 1;
  }
  return 0;
}

/* start a new checkpoint if not already in one, mark each file as need_closed */
static int scri_start_checkpoint()
{
  if (!scri_in_checkpoint) {
    /* mark all files as needing to be completed */
    int i;
    for(i=0; i<MAX_CHECKPOINT_FILES; i++) {
      if (scri_checkpoint_files[i].valid) {
        scri_checkpoint_files[i].need_closed = 1;
      }
    }

    /* start the checkpoint */
    scri_interpose_enabled = 0;
    SCR_Start_checkpoint();
    scri_interpose_enabled = 1;

    /* mark us inside a checkpoint */
    scri_in_checkpoint = 1;
  }

  return SCR_SUCCESS;
}

/* given an index into the checkpoint file array, mark the file as closed
 * if all files are now closed, complete the checkpoint */
static int scri_complete_checkpoint(int index)
{
  if (scri_in_checkpoint) {
    /* mark this checkpoint file as complete */
    if (index < MAX_CHECKPOINT_FILES) {
      scri_checkpoint_files[index].need_closed = 0;
    }

    /* scan through to see if we have any files not yet complete */
    int still_open = 0;
    int i;
    for(i=0; i<MAX_CHECKPOINT_FILES; i++) {
      if (scri_checkpoint_files[i].valid && scri_checkpoint_files[i].need_closed) {
        still_open = 1;
      }
    }

    /* if there are no files yet to be completed, complete the checkpoint */
    if (!still_open) {
      /* disable the interposer since SCR_Complete_checkpoint calls open/close */
      scri_interpose_enabled = 0;
      SCR_Complete_checkpoint(1);
      scri_interpose_enabled = 1;

      /* mark us out of the checkpoint */
      scri_in_checkpoint = 0;
    }
  }

  return SCR_SUCCESS;
}

/* lookup a checkpoint file index given a filename */
static int scri_index_by_filename(const char* filename)
{
  int i;
  for(i=0; i<MAX_CHECKPOINT_FILES; i++) {
    if (scri_checkpoint_files[i].valid &&
        scri_file_matches(filename, &scri_checkpoint_files[i].re))
    {
      return i;
    }
  }
  return MAX_CHECKPOINT_FILES;
}

/* lookup a checkpoint file index given an open file descriptor */
static int scri_index_by_fd(const int fd)
{
  int i;
  for(i=0; i<MAX_CHECKPOINT_FILES; i++) {
    if (scri_checkpoint_files[i].valid &&
        scri_checkpoint_files[i].ftype == SCRI_FD &&
        fd == scri_checkpoint_files[i].fd)
    {
      return i;
    }
  }
  return MAX_CHECKPOINT_FILES;
}

/* lookup a checkpoint file index given an open file stream */
static int scri_index_by_fstream(const FILE* fstream)
{
  int i;
  for(i=0; i<MAX_CHECKPOINT_FILES; i++) {
    if (scri_checkpoint_files[i].valid &&
        scri_checkpoint_files[i].ftype == SCRI_FSTREAM &&
        fstream == scri_checkpoint_files[i].fstream)
    {
      return i;
    }
  }
  return MAX_CHECKPOINT_FILES;
}

/* returns 1 if the given filename is a checkpoint file, and 0 otherwise */
static int scri_is_checkpoint_dirname(const char* name)
{
  if (scri_interpose_enabled &&
      scri_checkpoint_dir_valid &&
      scri_file_matches(name, &scri_re_checkpoint_dir))
  {
    return 1;
  }
  return 0;
}

/* returns 1 if the given filename is a checkpoint file, and 0 otherwise */
static int scri_is_checkpoint_filename(const char* file)
{
  int i = scri_index_by_filename(file);
  if (scri_interpose_enabled &&
      i < MAX_CHECKPOINT_FILES &&
      scri_checkpoint_files[i].enabled)
  {
    return 1;
  }
  return 0;
}

/* returns 1 if the given file descriptor is a checkpoint file, and 0 otherwise */
static int scri_is_checkpoint_fd(const int fd)
{
  int i = scri_index_by_fd(fd);
  if (scri_interpose_enabled &&
      i < MAX_CHECKPOINT_FILES &&
      scri_checkpoint_files[i].enabled)
  {
    return 1;
  }
  return 0;
}

/* returns 1 if the given file stream is a checkpoint file, and 0 otherwise */
static int scri_is_checkpoint_fstream(const FILE* fstream)
{
  int i = scri_index_by_fstream(fstream);
  if (scri_interpose_enabled &&
      i < MAX_CHECKPOINT_FILES &&
      scri_checkpoint_files[i].enabled)
  {
    return 1;
  }
  return 0;
}

/* record file descriptor and flags used in open call for this filename */
static int scri_add_checkpoint_fd(const char* file, const char* temp, const int fd, const int flags)
{
  int i = scri_index_by_filename(file);
  if (i < MAX_CHECKPOINT_FILES) {
    scri_checkpoint_files[i].tempname = strdup(temp);
    scri_checkpoint_files[i].ftype    = SCRI_FD;
    scri_checkpoint_files[i].fd       = fd;
    scri_checkpoint_files[i].flags    = flags;
    return 0;
  }

  /* couldn't find an empty slot for this file */
  fprintf(stderr,"SCRI: ERROR: Too many checkpoint files open when registering %s, maximum supported is %d @ %s:%d\n",
          file, MAX_CHECKPOINT_FILES, __FILE__, __LINE__
  );
  exit(1);

  return 1;
}

/* drop the file descriptor for this filename (file has been closed) */
static int scri_drop_checkpoint_fd(const int fd)
{
  int i = scri_index_by_fd(fd);
  if (i < MAX_CHECKPOINT_FILES) {
    if (scri_checkpoint_files[i].tempname != NULL) {
      free(scri_checkpoint_files[i].tempname);
      scri_checkpoint_files[i].tempname = NULL;
    }
    scri_checkpoint_files[i].ftype = SCRI_FNULL;
    scri_checkpoint_files[i].fd    = -1;
    scri_checkpoint_files[i].flags = 0;
    return 0;
  }
  /* TODO: an error to get here */
  return 1;
}

/* record the fstream value and the mode used in the fopen call for this filename */
static int scri_add_checkpoint_fstream(const char* file, const char* temp, const FILE* fstream, const char* mode)
{
  int i = scri_index_by_filename(file);
  if (i < MAX_CHECKPOINT_FILES) {
    scri_checkpoint_files[i].tempname = strdup(temp);
    scri_checkpoint_files[i].ftype    = SCRI_FSTREAM;
    scri_checkpoint_files[i].fstream  = (FILE*) fstream;
    scri_checkpoint_files[i].mode     = strdup(mode);
    return 0;
  }

  /* couldn't find an empty slot for this file */
  fprintf(stderr,"SCRI: ERROR: Too many checkpoint files open when registering %s, maximum supported is %d @ %s:%d\n",
          file, MAX_CHECKPOINT_FILES, __FILE__, __LINE__
  );
  exit(1);

  return 1;
}

/* drop the fstream for this filename (file has been closed) */
static int scri_drop_checkpoint_fstream(const FILE* fstream)
{
  int i = scri_index_by_fstream(fstream);
  if (i < MAX_CHECKPOINT_FILES) {
    if (scri_checkpoint_files[i].tempname != NULL) {
      free(scri_checkpoint_files[i].tempname);
      scri_checkpoint_files[i].tempname = NULL;
    }
    scri_checkpoint_files[i].ftype   = SCRI_FNULL;
    scri_checkpoint_files[i].fstream = NULL;
    if (scri_checkpoint_files[i].mode != NULL) {
      free(scri_checkpoint_files[i].mode);
      scri_checkpoint_files[i].mode = NULL;
    }
    return 0;
  }
  /* TODO: an error to get here */
  return 1;
}

/* given a regular expression for a checkpoint directory, prepare it for testing */
static int scri_define_checkpoint_dirname_regex(const char* dirname)
{
  /* compile the filename regex pattern */
  int rc = regcomp(&scri_re_checkpoint_dir, dirname, REG_EXTENDED);
  if (rc != 0) {
    fprintf(stderr,"SCRI: ERROR: Checkpoint directory name regex compilation for %s failed (rc=%d) @ %s:%d\n",
            dirname, rc, __FILE__, __LINE__
    );
    exit(1);
  }

  /* note that the checkpoint directory regular expression is valid */
  scri_checkpoint_dir_valid = 1;

  return 0;
}

/* given a regular expression for a checkpoint file, add it to our list and prepare it for testing */
static int scri_define_checkpoint_filename_regex(const char* filename)
{
  int i;
  for(i=0; i<MAX_CHECKPOINT_FILES; i++) {
    if (! scri_checkpoint_files[i].valid) {
      /* mark this entry as valid and copy in the filename */
      scri_checkpoint_files[i].valid = 1;
      scri_checkpoint_files[i].filename = strdup(filename);
      if (scri_checkpoint_files[i].filename == NULL) {
        fprintf(stderr,"SCRI: ERROR: Failed to allocate space to record filename regex for %s @ %s:%d\n",
                filename, __FILE__, __LINE__
        );
        exit(1);
      }

      /* compile the filename regex pattern */
      int rc = regcomp(&scri_checkpoint_files[i].re, filename, REG_EXTENDED);
      if (rc != 0) {
        fprintf(stderr,"SCRI: ERROR: Failed to compile filename regex %s (rc=%d) @ %s:%d\n",
                filename, rc, __FILE__, __LINE__
        );
        exit(1);
      }

      return 0;
    }
  }

  /* couldn't find an empty slot for this file */
  fprintf(stderr,"SCRI: ERROR: Too many filename regex specified, maximum is %d @ %s:%d\n",
          MAX_CHECKPOINT_FILES, __FILE__, __LINE__
  );
  exit(1);

  return 1;
}

/* given a filename and regular expression, return whether there is a match */
static int scri_define_checkpoint_filename_regex_by_rank(const char* filename)
{
  size_t nmatch = 3;
  regmatch_t pmatch[nmatch];

  /* check for a match */
  int low  = -1;
  int high = -1;
  const char* file = NULL;
  char* value = NULL;
  memset(pmatch, 0, sizeof(regmatch_t) * nmatch);
  if (regexec(&scri_re_low_N, filename, nmatch, pmatch, 0) == 0) {
    /* handles 0-N:regex */

    /* pull out the low */
    value = strndup(filename + pmatch[1].rm_so,
           (size_t)(pmatch[1].rm_eo - pmatch[1].rm_so));
    low = atoi(value);
    free(value); value = NULL;

    /* set high to total number of ranks minus one */
    high = scri_ranks - 1;

    /* pull out the file regex */
    file = filename + pmatch[2].rm_eo + 1;
  } else if (regexec(&scri_re_low_high, filename, nmatch, pmatch, 0) == 0) {
    /* e.g., handles 0-3:regex */

    /* pull out the low */
    value = strndup(filename + pmatch[1].rm_so,
           (size_t)(pmatch[1].rm_eo - pmatch[1].rm_so));
    low = atoi(value);
    free(value); value = NULL;

    /* pull out the high */
    value = strndup(filename + pmatch[2].rm_so,
           (size_t)(pmatch[2].rm_eo - pmatch[2].rm_so));
    high = atoi(value);
    free(value); value = NULL;

    /* pull out the file regex */
    file = filename + pmatch[2].rm_eo + 1;
  } else {
    fprintf(stderr,"SCRI: ERROR: Unknown MPI rank range for file: %s, perhaps specify '0-N:%s' @ %s:%d\n",
            filename, filename, __FILE__, __LINE__
    );
    exit(1);
  }

  /* add the file if our rank is in the low-high range */
  if (low <= scri_rank && scri_rank <= high) {
    scri_define_checkpoint_filename_regex(file);
    return 1;
  }

  return 0;
}

/* parse SCR environment variables and add checkpoint file regular expressions */
static int scri_define_checkpoint_files()
{
  /* read in environment variables */
  char* value   = NULL;
  char* pattern = NULL;
  char  token   = ',';
  char* dir_pattern = NULL;

  /* read in the list of checkpoint file regular expressions separated by token */
  if ((value = getenv("SCR_CHECKPOINT_PATTERN")) != NULL) {
    if (strcmp(value, "") != 0) {
      pattern = strdup(value);
    }
  }

  /* override default token of ':' for separating checkpoint file regex in pattern */
  if ((value = getenv("SCR_CHECKPOINT_PATTERN_TOKEN")) != NULL) {
    if (strcmp(value, "") != 0) {
      token = value[0];
    }
  }

  /* read in the pattern for the checkpoint directory names */
  if ((value = getenv("SCR_CHECKPOINT_DIR_PATTERN")) != NULL) {
    if (strcmp(value, "") != 0) {
      dir_pattern = strdup(value);
    }
  }

  /* add the regex patterns to our list */
  if (pattern != NULL) {
    int i = 0;
    char* file = pattern;

    /* loop through breaking string into pieces using token */
    char* stop = strchr(file, (int) token);
    while (stop != NULL) {
      if (i >= MAX_CHECKPOINT_FILES) {
        fprintf(stderr,"SCRI: ERROR: Rank %d: Too many files in SCR_CHECKPOINT_PATTERN '%s' maximum allowed is %d @ %s:%d\n",
                scri_rank, pattern, MAX_CHECKPOINT_FILES, __FILE__, __LINE__
        );
        exit(1);
      }
      *stop = '\0';
      i += scri_define_checkpoint_filename_regex_by_rank(file);
      file = stop + 1;
      stop = strchr(file, (int) token);
    }

    /* now add the last file if we have one */
    if (strcmp(file, "") != 0 && i < MAX_CHECKPOINT_FILES) {
      if (i >= MAX_CHECKPOINT_FILES) {
        fprintf(stderr,"SCRI: ERROR: Rank %d: Too many files in SCR_CHECKPOINT_PATTERN '%s' maximum allowed is %d @ %s:%d\n",
                scri_rank, pattern, MAX_CHECKPOINT_FILES, __FILE__, __LINE__
        );
        exit(1);
      }
      i += scri_define_checkpoint_filename_regex_by_rank(file);
    }

    free(pattern); pattern = NULL;
  }

  /* add the checkpoint directory pattern if specified */
  if (dir_pattern != NULL) {
    scri_define_checkpoint_dirname_regex(dir_pattern);
    free(dir_pattern); dir_pattern = NULL;
  }

  /* check that every rank has at least one file (so we know when to complete each checkpoint) */
  int i;
  int found_one = 0;
  for (i=0; i < MAX_CHECKPOINT_FILES; i++) {
    if (scri_checkpoint_files[i].valid) { found_one = 1; }
  }
  if (!found_one) {
    fprintf(stderr,"SCRI: ERROR: Rank %d: No checkpoint file specified @ %s:%d\n",
            scri_rank, __FILE__, __LINE__
    );
    exit(1);
  }

  return 0;
}

/*
==============================================================================
Interpose functions
==============================================================================
*/

static void* mydlsym(const char *name)
{
  void *p = dlsym(RTLD_NEXT, name);
  if (!p) {
    fprintf(stderr,"dlsym(RTLD_NEXT, %s) failed: %s\n", name, dlerror());
    exit(1);
  }
  return p;
}

/* call dlsym to interpose a set of functions */
static void scr_interpose_init()
{
  if (scri_initialized) { return; }

  /* interpose MPI functions */
  if (scri_real_mpi_init == NULL) {
    scri_real_mpi_init = (int (*)(int *, char ***)) mydlsym("MPI_Init");
  }
  if (scri_real_mpi_fini == NULL) {
    scri_real_mpi_fini = (int (*)()) mydlsym("MPI_Finalize");
  }

  /* interpose open/close functions */
  if (scri_real_open == NULL) {
    scri_real_open  = (int (*)(const char *, int, ...)) mydlsym("open");
  }
  /*
  scri_real_open  = (int (*)(const char *, int, mode_t)) mydlsym("open");
  */
  if (scri_real_close == NULL) {
    scri_real_close = (int (*)(int fd)) mydlsym("close");
  }

  /* interpose fopen/fclose functions */
  if (scri_real_fopen == NULL) {
    scri_real_fopen  = (FILE* (*)(const char *, const char *)) mydlsym("fopen");
  }
  if (scri_real_fclose == NULL) {
    scri_real_fclose = (int (*)(FILE*)) mydlsym("fclose");
  }

  /* interpose mkdir */
  if (scri_real_mkdir == NULL) {
    scri_real_mkdir = (int (*)(const char*, mode_t)) mydlsym("mkdir");
  }

  /* interpose read/write functions */
/*
  real_read  = (ssize_t (*)(int fd, void *buf, size_t count))       mydlsym("read");
  real_write = (ssize_t (*)(int fd, const void *buf, size_t count)) mydlsym("write");
*/

  /* initialize the data structures */
  if (!scri_checkpoint_files_valid) {
    scri_checkpoint_files_valid = 1;
    int i;
    for(i=0; i<MAX_CHECKPOINT_FILES; i++) {
      scri_checkpoint_files[i].valid    = 0;
      scri_checkpoint_files[i].enabled  = 1;
      scri_checkpoint_files[i].filename = NULL;
      scri_checkpoint_files[i].tempname = NULL;
      scri_checkpoint_files[i].ftype    = SCRI_FNULL;
      scri_checkpoint_files[i].fd       = -1;
      scri_checkpoint_files[i].flags    = 0;
      scri_checkpoint_files[i].fstream  = NULL;
      scri_checkpoint_files[i].mode     = NULL;
    }
  }

  /* compile the low-high range regex pattern */
  /* we surround each regcomp with a compiled flag in case the call to regex,
   * leads to a call to open, which in turns calls scri_init() again
   * (e.g., when debugging with TV's memory debugger) */
  int rc;
  char low_high_range[] = "^([0-9]+)-([0-9]+):";
  char low_N_range[]    = "^([0-9]+)-(N):";
  char scr_file_ext[]   = ".scr$";
  if (!scri_re_low_high_compiled) {
    scri_re_low_high_compiled = 1;
    rc = regcomp(&scri_re_low_high, low_high_range, REG_EXTENDED);
    if (rc != 0) {
      fprintf(stderr,"SCRI: ERROR: Failed to compile low-to-high range regex: %s (rc=%d) @ %s:%d\n",
             low_high_range, rc, __FILE__, __LINE__
      );
      exit(1);
    }
  }
  if (!scri_re_low_N_compiled) {
    scri_re_low_N_compiled = 1;
    rc = regcomp(&scri_re_low_N, low_N_range, REG_EXTENDED);
    if (rc != 0) {
      fprintf(stderr,"SCRI: ERROR: Failed to compile low-to-N range regex: %s (rc=%d) @ %s:%d\n",
             low_N_range, rc, __FILE__, __LINE__
      );
      exit(1);
    }
  }
  if (!scri_re_scr_file_compiled) {
    scri_re_scr_file_compiled = 1;
    rc = regcomp(&scri_re_scr_file, scr_file_ext, REG_EXTENDED);
    if (rc != 0) {
      fprintf(stderr,"SCRI: ERROR: Failed to compile scr file extension regex: %s (rc=%d) @ %s:%d\n",
             scr_file_ext, rc, __FILE__, __LINE__
      );
      exit(1);
    }
  }

  scri_interpose_enabled = 1;
  scri_initialized = 1;
}

/*
==============================================================================
Interpose MPI functions
==============================================================================
*/

/* see wrappers_special.c from mpiP */
/* this function has the logic, the C and Fortran versions of MPI_Init route here */
static int _MPI_Init (int *argc, char ***argv)
{
  int rc = 0;

  /* initialize the interposer */
  if (!scri_initialized) { scr_interpose_init(); }
  
  /* call the real MPI_Init */
  rc = (*scri_real_mpi_init)(argc, argv);

  /* initialize the SCR library */
  scri_interpose_enabled = 0;
  SCR_Init();
  scri_interpose_enabled = 1;

  /* get our MPI rank */
  MPI_Comm_rank(MPI_COMM_WORLD, &scri_rank);
  MPI_Comm_size(MPI_COMM_WORLD, &scri_ranks);

  /* parse our checkpoint files (we do this after MPI_Init so we know which rank we are) */
  scri_define_checkpoint_files();

  return rc;
}

#ifdef MPI_Init
#undef MPI_Init
#endif
int MPI_Init (int *argc, char ***argv)
{
  int rc = 0;
  rc = _MPI_Init (argc, argv);
  return rc;
}

#ifdef MPI_Finalize
#undef MPI_Finalize
#endif
int MPI_Finalize ()
{
  int rc = 0;

  /* initialize the interposer */
  if (!scri_initialized) { scr_interpose_init(); }
  
  /* finalize the SCR library */
  scri_interpose_enabled = 0;
  SCR_Finalize();

  /* we called finalize, so we can just leave the interposer disabled */

  /* free off the regular expression structures */
  regfree(&scri_re_low_high);
  regfree(&scri_re_low_N);
  regfree(&scri_re_scr_file);
  if (scri_checkpoint_dir_valid) {
    regfree(&scri_re_checkpoint_dir);
  }
  if (scri_checkpoint_files_valid) {
    int i;
    for(i=0; i<MAX_CHECKPOINT_FILES; i++) {
      if (scri_checkpoint_files[i].valid) {
        scri_checkpoint_files[i].valid   = 0;
        scri_checkpoint_files[i].enabled = 0;
        if (scri_checkpoint_files[i].filename != NULL) {
          free(scri_checkpoint_files[i].filename);
          scri_checkpoint_files[i].filename = NULL;
        }
        if (scri_checkpoint_files[i].tempname != NULL) {
          free(scri_checkpoint_files[i].tempname);
          scri_checkpoint_files[i].tempname = NULL;
        }
        regfree(&scri_checkpoint_files[i].re);
      }
    }
  }

  /* call the real MPI_Finalize */
  rc = (*scri_real_mpi_fini)();

  return rc;
}

/*
==============================================================================
Interpose open/close functions
==============================================================================
*/

#ifdef open 
#undef open
#endif
/*
int open(const char *pathname, int flags, mode_t mode)
*/
int open(const char *pathname, int flags, ...)
{
  const char* name = pathname;

  if (!scri_initialized) { scr_interpose_init(); }

  /* check whether pathname matches pattern for a checkpoint file */
  char temp[SCR_MAX_FILENAME];
  int checkpoint = scri_is_checkpoint_filename(pathname);
  if (checkpoint) {
    /* don't start a new checkpoint if the file is being opened as read-only */
    /* O_RDONLY == 0 so we can't do a straight bit test, instead check whether either RDWR or WRONLY is set */
    /* TODO: must be a better way to do this */
    if ((flags & O_RDWR) || (flags & O_WRONLY)) {
      scri_start_checkpoint();
    }

    /* reroute file to cache */
    scri_interpose_enabled = 0;
    if (SCR_Route_file((char*) pathname, temp) == SCR_SUCCESS) {
      name = temp;
    }
    scri_interpose_enabled = 1;
  }

  /* extract the mode (see man 2 open) */
  mode_t mode = 0;
  if (flags & O_CREAT) {
    va_list ap;
    va_start(ap, flags);
    mode = va_arg(ap, mode_t);
    va_end(ap);
  }

  /* open the file */
  int rc = (*scri_real_open)(name, flags, mode);

  /* mark file descriptor as checkpoint file */
  if (checkpoint) {
    if (rc < 0) {
      /* Don't want to kick out here because user may have expected this open to fail, e.g., read-only */
      fprintf(stderr,"SCRI: ERROR: Failed to open %s for rerouting %s (errno=%d %s) @ %s:%d\n",
              name, pathname, errno, strerror(errno), __FILE__, __LINE__
      );
    } else {
      scri_add_checkpoint_fd(pathname, name, rc, flags);
    }
  }

  /* return what ever the real open call returned */
  return rc;
}

#ifdef close 
#undef close
#endif
int close(int fd)
{
  if (!scri_initialized) { scr_interpose_init(); }

  /* TODO: need to fsync here as well? */

  /* close the file */
  int rc = (*scri_real_close)(fd);

  /* if fd matches a checkpoint file, call SCR_COMPLETE and then remove fd from list */
  if (scri_is_checkpoint_fd(fd)) {
    /* complete the checkpoint */
    int i = scri_index_by_fd(fd);
    scri_complete_checkpoint(i);

    /* drop the file descriptor from our active set */
    scri_drop_checkpoint_fd(fd);
  }

  /* return what ever the real close call gave us */
  return rc;
} 

#ifdef fopen 
#undef fopen
#endif
FILE* fopen(const char * pathname, const char * mode)
{
  const char* name = pathname;

  if (!scri_initialized) { scr_interpose_init(); }

  /* check whether pathname matches pattern for a checkpoint file */
  char temp[SCR_MAX_FILENAME];
  int checkpoint = scri_is_checkpoint_filename(pathname);
  if (checkpoint) {
    /* don't start a new checkpoint if the file is being opened as read-only */
    if (strcmp(mode, "r") != 0 && strcmp(mode, "rb") != 0) {
      scri_start_checkpoint();
    }

    /* reroute file to cache */
    scri_interpose_enabled = 0;
    if (SCR_Route_file((char*) pathname, temp) == SCR_SUCCESS) {
      name = temp;
    }
    scri_interpose_enabled = 1;
  }

  /* open the file */
  FILE* rc = (*scri_real_fopen)(name, mode);

  /* mark file descriptor as checkpoint file */
  if (checkpoint) {
    if (rc == NULL) {
      /* Don't want to kick out here because user may have expected this open to fail, e.g., read-only */
      fprintf(stderr,"SCRI: ERROR: Failed to fopen %s for rerouting %s with mode %s (errno=%d %s) @ %s:%d\n",
             name, pathname, mode, errno, strerror(errno), __FILE__, __LINE__
      );
    } else {
      scri_add_checkpoint_fstream(pathname, name, rc, mode);
    }
  }

  /* return what ever the real open call returned */
  return rc;
}

#ifdef fclose 
#undef fclose
#endif
int fclose(FILE* fstream)
{
  if (!scri_initialized) { scr_interpose_init(); }

  /* TODO: need to fsync here as well? */

  /* close the file */
  int rc = (*scri_real_fclose)(fstream);

  /* if fd matches a checkpoint file, call SCR_COMPLETE and then remove fd from list */
  if (scri_is_checkpoint_fstream(fstream)) {
    /* complete the checkpoint */
    int i = scri_index_by_fstream(fstream);
    scri_complete_checkpoint(i);

    /* drop the file descriptor from our active set */
    scri_drop_checkpoint_fstream(fstream);
  }

  /* return what ever the real close call gave us */
  return rc;
} 

/*
==============================================================================
Interpose mkdir functions
==============================================================================
*/

#ifdef mkdir 
#undef mkdir
#endif
int mkdir(const char *pathname, mode_t mode)
{
  int rc = 0;

  if (!scri_initialized) { scr_interpose_init(); }

  /* if the user is trying to create a checkpoint directory,
   * do nothing and return success, otherwise, just call mkdir */
  int checkpoint = scri_is_checkpoint_dirname(pathname);
  if (!checkpoint) {
    rc = (*scri_real_mkdir)(pathname, mode);
  }

  /* return what ever the real open call returned */
  return rc;
}
