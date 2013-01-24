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

/* This is a utility program that runs on the compute node during a
 * scavenge operation to copy files from cache to the prefix directory.
 * It also creates filemap files so that other commands can identify
 * these files. */

#include "scr_conf.h"
#include "scr.h"
#include "scr_io.h"
#include "scr_err.h"
#include "scr_util.h"
#include "scr_path.h"
#include "scr_meta.h"
#include "scr_hash.h"
#include "scr_filemap.h"
#include "scr_dataset.h"

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/file.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <unistd.h>
#include <getopt.h>

#define PROG ("scr_copy")

static char hostname[256] = "UNKNOWN_HOST";

int print_usage()
{
  /* here we don't print anything since this runs on each node,
   * so it'd be noisy, just exit with a non-zero exit code */
  exit(1);
}

struct arglist {
  char* cntldir;          /* control directory */
  int id;                 /* dataset id */
  char* dstdir;           /* destination directory */
  unsigned long buf_size; /* number of bytes to copy file data to file system */
  int crc_flag;           /* whether to compute crc32 during copy */
  int partner_flag;       /* whether to copy data for partner */
  int container_flag;     /* whether to use containers */
};

int process_args(int argc, char **argv, struct arglist* args)
{
  /* define our options */
  static struct option long_options[] = {
    {"cntldir",    required_argument, NULL, 'c'},
    {"id",         required_argument, NULL, 'i'},
    {"dstdir",     required_argument, NULL, 'd'},
    {"buf",        required_argument, NULL, 'b'},
    {"crc",        no_argument,       NULL, 'r'},
    {"partner",    no_argument,       NULL, 'p'},
    {"containers", no_argument,       NULL, 'n'},
    {0, 0, 0, 0}
  };

  /* set our options to default values */
  args->cntldir        = NULL;
  args->id             = -1;
  args->dstdir         = NULL;
  args->buf_size       = SCR_FILE_BUF_SIZE;
  args->crc_flag       = SCR_CRC_ON_FLUSH;
  args->partner_flag   = 0;
  args->container_flag = SCR_USE_CONTAINERS;

  /* loop through and process all options */
  int c, id;
  unsigned long long bytes;
  do {
    /* read in our next option */
    int option_index = 0;
    c = getopt_long(argc, argv, "c:i:d:b:rpnh", long_options, &option_index);
    switch (c) {
      case 'c':
        /* control directory */
        args->cntldir = optarg;
        break;
      case 'i':
        /* dataset id to flush */
        id = atoi(optarg);
        if (id <= 0) {
          scr_err("%s: Dataset id must be positive '--id %s'",
            PROG, optarg
          );
          return 0;
        }
        args->id = id;
        break;
      case 'd':
        /* destination directory */
        args->dstdir = optarg;
        break;
      case 'b':
        /* buffer size to copy file data to file system */
        if (scr_abtoull(optarg, &bytes) != SCR_SUCCESS) {
          scr_err("%s: Invalid value for buffer size '--buf %s'",
            PROG, optarg
          );
          return 0;
        }
        args->buf_size = (unsigned long) bytes;
        break;
      case 'r':
        /* compute and record crc32 during copy */
        args->crc_flag = 1;
        break;
      case 'p':
        /* copy out partner files */
        args->partner_flag = 1;
        break;
      case 'n':
        /* copy files into containers */
        args->container_flag = 1;
        break;
      case 'h':
        /* print help message and exit */
        print_usage();
        break;
      case '?':
        /* getopt_long printed an error message */
        break;
      default:
        if (c != -1) {
          /* missed an option */
          scr_err("%s: Option '%s' specified but not processed",
            PROG, argv[option_index]
          );
        }
    }
  } while (c != -1);

  /* check that we got a directory name */
  if (args->cntldir == NULL) {
    scr_err("%s: Must specify control directory via '--dir <cntl_dir>'",
      PROG
    );
    return 0;
  }

  /* check that we got a dataset id */
  if (args->id <= 0) {
    scr_err("%s: Must specify dataset id via '--id <id>'",
      PROG
    );
    return 0;
  }

  /* check that we got a destination directory */
  if (args->dstdir == NULL) {
    scr_err("%s: Must specify destination directory via '--dstdir <dst_dir>'",
      PROG
    );
    return 0;
  }

  return 1;
}

/* checks whether specifed file exists, is readable, and is complete */
static int scr_bool_have_file(
  const scr_filemap* map,
  int id,
  int rank,
  const char* file)
{
  /* if no filename is given return false */
  if (file == NULL || strcmp(file,"") == 0) {
    scr_dbg(2, "%s: File name is null or the empty string", PROG);
    return 0;
  }

  /* check that we can read the file */
  if (scr_file_is_readable(file) != SCR_SUCCESS) {
    scr_dbg(2, "%s: Do not have read access to file: %s", PROG, file);
    return 0;
  }

  int valid = 1;

  /* check that we can read meta file for the file */
  scr_meta* meta = scr_meta_new();
  if (valid && scr_filemap_get_meta(map, id, rank, file, meta) != SCR_SUCCESS) {
    scr_dbg(2, "%s: Failed to read meta data for file: %s", PROG, file);
    valid = 0;
  }

  /* check that the file is complete */
  if (valid && scr_meta_is_complete(meta) != SCR_SUCCESS) {
    scr_dbg(2, "%s: File is marked as incomplete: %s", PROG, file);
    valid = 0;
  }

  /* TODODSET: check dataset instead of checkpoint id */

#if 0
  /* check that the file really belongs to the dataset id we think it does */
  if (valid && scr_meta_check_checkpoint(meta, id) != SCR_SUCCESS) {
    scr_dbg(2, "%s: File's dataset ID (%d) does not match id in meta data file for %s",
            PROG, id, file
    );
    valid = 0;
  }
#endif

#if 0
  /* check that the file really belongs to the rank we think it does */
  if (valid && scr_meta_check_rank(meta, rank) != SCR_SUCCESS) {
    scr_dbg(2, "%s: File's rank (%d) does not match rank in meta data file for %s",
            PROG, rank, file
    );
    valid = 0;
  }
#endif

  /* check that the file size matches (use strtol while reading data) */
  unsigned long size = scr_file_size(file);
  if (valid && scr_meta_check_filesize(meta, size) != SCR_SUCCESS) {
    scr_dbg(2, "%s: Filesize is incorrect, currently %lu for %s",
      PROG, size, file
    );
    valid = 0;
  }

  scr_meta_delete(&meta);

  /* TODO: check that crc32 match if set (this would be expensive) */

  /* if we made it here, assume the file is good */
  return valid;
}

static int scr_bool_have_files(scr_filemap* map, int id, int rank)
{
  /* check that the expected number of files matches the real number of files */
  int exp_files = scr_filemap_get_expected_files(map, id, rank);
  int num_files = scr_filemap_num_files(map, id, rank);
  if (exp_files != num_files) {
    return 0;
  }

  int have_files = 1;

  /* now check that we have each file */
  scr_hash_elem* rank_elem;
  for (rank_elem = scr_filemap_first_rank_by_dataset(map, id);
       rank_elem != NULL;
       rank_elem = scr_hash_elem_next(rank_elem))
  {
    int rank = scr_hash_elem_key_int(rank_elem);

    scr_hash_elem* file_elem = NULL;
    for (file_elem = scr_filemap_first_file(map, id, rank);
         file_elem != NULL;
         file_elem = scr_hash_elem_next(file_elem))
    {
      /* get filename and check that we can read it */
      char* file = scr_hash_elem_key(file_elem);

      if (! scr_bool_have_file(map, id, rank, file)) {
        have_files = 0;
      }
    }
  }

  return have_files;
}

int main (int argc, char *argv[])
{
  /* get my hostname */
  if (gethostname(hostname, sizeof(hostname)) != 0) {
    scr_err("scr_copy: Call to gethostname failed @ %s:%d",
      __FILE__, __LINE__
    );
    printf("scr_copy: UNKNOWN_HOST: Return code: 1\n");
    return 1;
  }

  /* process command line arguments, remember index to first argument */
  struct arglist args;
  if (! process_args(argc, argv, &args)) {
    printf("scr_copy: %s: Return code: 1\n", hostname);
    return 1;
  }
  int first_non_option = optind;

  /* build the name of the master filemap */
  scr_path* scr_master_map_file = scr_path_from_str(args.cntldir);
  scr_path_append_str(scr_master_map_file, "filemap.scrinfo");

  /* TODO: get list of partner nodes I have files for */

  /* read in the master map */
  scr_hash* hash = scr_hash_new();
  scr_hash_read_path(scr_master_map_file, hash);

  /* free the name of the master map file */
  scr_path_delete(&scr_master_map_file);

  /* create an empty filemap */
  scr_filemap* map = scr_filemap_new();

  /* for each filemap listed in the master map */
  scr_hash_elem* elem;
  for (elem = scr_hash_elem_first(scr_hash_get(hash, "Filemap"));
       elem != NULL;
       elem = scr_hash_elem_next(elem))
  {
    /* get the filename of this filemap */
    char* file = scr_hash_elem_key(elem);

    /* read in the filemap */
    scr_filemap* tmp_map = scr_filemap_new();
    scr_path* path_file = scr_path_from_str(file);
    scr_filemap_read(path_file, tmp_map);
    scr_path_delete(&path_file);

    /* merge it with local 0 filemap */
    scr_filemap_merge(map, tmp_map);

    /* delete filemap */
    scr_filemap_delete(&tmp_map);
  }

  /* check whether we have the specified dataset id */
  scr_hash_elem* rank_elem = scr_filemap_first_rank_by_dataset(map, args.id);
  if (rank_elem == NULL) {
    printf("scr_copy: %s: Do not have any files for dataset id %d\n",
      hostname, args.id
    );
    printf("scr_copy: %s: Return code: 1\n", hostname);
    scr_filemap_delete(&map);
    scr_hash_delete(&hash);
    return 1;
  }

  /* path to data set directory */
  scr_path* path_dataset = scr_path_from_str(args.dstdir);
  scr_path_reduce(path_dataset);

  /* define the path to the .scr subdirectory */
  scr_path* path_scr = scr_path_dup(path_dataset);
  scr_path_append_str(path_scr, ".scr");
  scr_path_reduce(path_scr);
  char* path_scr_str = scr_path_strdup(path_scr);

  int rc = 0;

  /* iterate over each rank we have for this dataset */
  for (rank_elem = scr_filemap_first_rank_by_dataset(map, args.id);
       rank_elem != NULL;
       rank_elem = scr_hash_elem_next(rank_elem))
  {
    /* get the rank number */
    int rank = scr_hash_elem_key_int(rank_elem);

    /* lookup the scavenge descriptor for this rank */
    scr_hash* flushdesc = scr_hash_new();
    scr_filemap_get_flushdesc(map, args.id, rank, flushdesc);

    /* read hostname of partner */
    char* partner = NULL;
    scr_hash_util_get_str(flushdesc, SCR_SCAVENGE_KEY_PARTNER, &partner);

    /* determine whether we're preserving user directories */
    int preserve_dirs = 0;
    scr_hash_util_get_int(flushdesc, SCR_SCAVENGE_KEY_PRESERVE, &preserve_dirs);

    /* determine whether we're using containers */
    int container = 0;
    scr_hash_util_get_int(flushdesc, SCR_SCAVENGE_KEY_CONTAINER, &container);

    /* free the scavenge descriptor */
    scr_hash_delete(&flushdesc);

    /* if this rank is not a partner, and our partner only flag is set,
     * skip this rank */
    if (partner == NULL && args.partner_flag) {
      continue;
    }

    /* if partner is set, print out the node name */
    if (partner != NULL) {
      /* print partner node name */
      printf("scr_copy: %s: Partners: %s\n", hostname, partner);

      /* if this rank is a partner, only copy files if this node failed */
      int my_partner_failed = 0;
      int i;
      for (i = first_non_option; i < argc; i++) {
        if (strcmp(argv[i], partner) == 0) {
          my_partner_failed = 1;
        }
      }
      if (! my_partner_failed) {
        continue;
      }
    }

    /* only copy data if we actually have the files */
    if (! scr_bool_have_files(map, args.id, rank)) {
      continue;
    }

    /* allocate a rank filemap object and set expected number
     * of files */
    scr_filemap* rank_map = scr_filemap_new();
    int num_files = scr_filemap_get_expected_files(map, args.id, rank);
    scr_filemap_set_expected_files(rank_map, args.id, rank, num_files);

    /* copy the dataset for this rank */
    scr_dataset* dataset = scr_dataset_new();
    scr_filemap_get_dataset(map,      args.id, rank, dataset);
    scr_filemap_set_dataset(rank_map, args.id, rank, dataset);
    scr_dataset_delete(&dataset);

    /* record whether we're preserving user directories or using containers */
    scr_hash* rank_flushdesc = scr_hash_new();
    scr_hash_util_set_int(rank_flushdesc, SCR_SCAVENGE_KEY_PRESERVE,  preserve_dirs);
    scr_hash_util_set_int(rank_flushdesc, SCR_SCAVENGE_KEY_CONTAINER, container);
    scr_filemap_set_flushdesc(rank_map, args.id, rank, rank_flushdesc);
    scr_hash_delete(&rank_flushdesc);

    /* step through each file we have for this rank */
    scr_hash_elem* file_elem = NULL;
    for (file_elem = scr_filemap_first_file(map, args.id, rank);
         file_elem != NULL;
         file_elem = scr_hash_elem_next(file_elem))
    {
      /* get filename */
      char* file = scr_hash_elem_key(file_elem);

      /* check that we can read the file */
      if (scr_bool_have_file(map, args.id, rank, file)) {
        /* read the meta data for this file */
        scr_meta* meta = scr_meta_new();
        scr_filemap_get_meta(map, args.id, rank, file, meta);

        /* check whether file is application file or SCR file */
        int user_file = 0;
        if (scr_meta_check_filetype(meta, SCR_META_FILE_USER) == SCR_SUCCESS) {
          user_file = 1;
        }

        /* get path to copy file */
        char* dst_dir = NULL;
        if (user_file) {
          /* assume that we're not preserving directories and copy
           * all files to top level dir */
          dst_dir = args.dstdir;
          if (preserve_dirs) {
            /* we're preserving user directories, get original path */
            if (scr_meta_get_origpath(meta, &dst_dir) != SCR_SUCCESS) {
              printf("scr_copy: %s: Could not find original path for file %s in dataset id %d\n",
                hostname, file, args.id
              );
              printf("scr_copy: %s: Return code: 1\n", hostname);
              scr_meta_delete(&meta);
              scr_filemap_delete(&map);
              scr_hash_delete(&hash);
              return 1;
            }

            /* TODO: keep a cache of directory names that we've already created */

            /* make directory to file */
            if (scr_mkdir(dst_dir, S_IRWXU) != SCR_SUCCESS) {
              printf("scr_copy: %s: Failed to create original path for file %s in dataset id %d\n",
                hostname, file, args.id
              );
              printf("scr_copy: %s: Return code: 1\n", hostname);
              scr_meta_delete(&meta);
              scr_filemap_delete(&map);
              scr_hash_delete(&hash);
              return 1;
            }
          }
        } else {
          /* scavenge SCR files to SCR directory */
          dst_dir = path_scr_str;
        }

        /* create destination file name */
        scr_path* dst_path = scr_path_from_str(file);
        scr_path_basename(dst_path);
        scr_path_prepend_str(dst_path, dst_dir);
        scr_path_reduce(dst_path);
        char* dst_file = scr_path_strdup(dst_path);

        /* copy the file and optionally compute the crc during the copy */
        int crc_valid = 0;
        uLong crc = crc32(0L, Z_NULL, 0);
        uLong* crc_p = NULL;
        if (args.crc_flag) {
          crc_valid = 1;
          crc_p = &crc;
        }
        if (scr_file_copy(file, dst_file, args.buf_size, crc_p)
          != SCR_SUCCESS)
        {
          crc_valid = 0;
          rc = 1;
        }

        /* compute relative path to destination file from dataset dir */
        scr_path* path_relative = scr_path_relative(path_dataset, dst_path);
        char* file_relative = scr_path_strdup(path_relative);

        /* add this file to the rank_map */
        scr_filemap_add_file(rank_map, args.id, rank, file_relative);

        /* if file has crc32, check it against the one computed during
         * the copy, otherwise if crc_flag is set, record crc32 */
        if (crc_valid) {
          uLong meta_crc;
          if (scr_meta_get_crc32(meta, &meta_crc) == SCR_SUCCESS) {
            if (crc != meta_crc) { 
              /* detected a crc mismatch during the copy */

              /* TODO: unlink the copied file */
              /* scr_file_unlink(dst_file); */

              /* mark the file as invalid */
              scr_meta_set_complete(meta, 0);

              rc = 1;
              scr_err("scr_copy: CRC32 mismatch detected when flushing file %s to %s @ %s:%d",
                file, dst_file, __FILE__, __LINE__
              );

              /* TODO: would be good to log this, but right now only
               * rank 0 can write log entries */
              /*
              if (scr_log_enable) {
                time_t now = scr_log_seconds();
                scr_log_event("CRC32 MISMATCH", my_flushed_file, NULL, &now, NULL);
              }
              */
            }
          } else {
            /* the crc was not already in the metafile, but we just
             * computed it, so set it */
            scr_meta_set_crc32(meta, crc);
          }
        }

        /* record its meta data in the filemap */
        scr_filemap_set_meta(rank_map, args.id, rank, file_relative, meta);

        /* free the string containing the relative file name */
        scr_free(&file_relative);
        scr_path_delete(&path_relative);

        /* free the destination file path and string */
        scr_free(&dst_file);
        scr_path_delete(&dst_path);

        /* free the meta data object */
        scr_meta_delete(&meta);
      } else {
        /* have_file failed, so there was some problem accessing file */
        rc = 1;
        scr_err("scr_copy: File is unreadable or incomplete: CheckpointID %d, Rank %d, File: %s",
          args.id, rank, file
        );
      }
    }

    /* write out the rank filemap for scr_index */
    scr_path* path_rank = scr_path_dup(path_scr);
    scr_path_append_strf(path_rank, "%d.scrfilemap", rank);
    if (scr_filemap_write(path_rank, rank_map) != SCR_SUCCESS) {
      rc = 1;
    }
    scr_path_delete(&path_rank);

    /* delete the rank filemap object */
    scr_filemap_delete(&rank_map);
  }

  /* delete path to scr directory */
  scr_free(&path_scr_str);
  scr_path_delete(&path_scr);

  /* free the dataset directory path */
  scr_path_delete(&path_dataset);

  /* print our return code and exit */
  printf("scr_copy: %s: Return code: %d\n", hostname, rc);
  return rc;
}
