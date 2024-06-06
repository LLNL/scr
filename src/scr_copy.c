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
#include "scr_meta.h"
#include "scr_filemap.h"
#include "scr_dataset.h"
#include "scr_cache_index.h"

#include "spath.h"
#include "kvtree.h"
#include "kvtree_util.h"

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
#include <dirent.h>
#include <regex.h>

#ifdef SCR_GLOBALS_H
#error "globals.h accessed from tools"
#endif

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
  char* prefix;           /* prefix directory */
  unsigned long buf_size; /* number of bytes to copy file data to file system */
  int crc_flag;           /* whether to compute crc32 during copy */
};

int process_args(int argc, char **argv, struct arglist* args)
{
  /* define our options */
  static struct option long_options[] = {
    {"cntldir",    required_argument, NULL, 'c'},
    {"id",         required_argument, NULL, 'i'},
    {"prefix",     required_argument, NULL, 'd'},
    {"buf",        required_argument, NULL, 'b'},
    {"crc",        no_argument,       NULL, 'r'},
    {0, 0, 0, 0}
  };

  /* set our options to default values */
  args->cntldir        = NULL;
  args->id             = -1;
  args->prefix         = NULL;
  args->buf_size       = SCR_FILE_BUF_SIZE;
  args->crc_flag       = SCR_CRC_ON_FLUSH;

  /* loop through and process all options */
  int c, id;
  unsigned long long bytes;
  do {
    /* read in our next option */
    int option_index = 0;
    c = getopt_long(argc, argv, "c:i:d:b:rh", long_options, &option_index);
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
        /* prefix directory */
        args->prefix = optarg;
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
  if (args->prefix == NULL) {
    scr_err("%s: Must specify prefix directory via '--prefix <dir>'",
      PROG
    );
    return 0;
  }

  return 1;
}

/* checks whether specifed file exists, is readable, and is complete */
static int scr_bool_have_file(
  const scr_filemap* map,
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
  if (valid && scr_filemap_get_meta(map, file, meta) != SCR_SUCCESS) {
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

#if 0
static int scr_bool_have_files(scr_filemap* map)
{
  int have_files = 1;

  kvtree_elem* file_elem = NULL;
  for (file_elem = scr_filemap_first_file(map);
       file_elem != NULL;
       file_elem = kvtree_elem_next(file_elem))
  {
    /* get filename and check that we can read it */
    char* file = kvtree_elem_key(file_elem);

    if (! scr_bool_have_file(map, file)) {
      have_files = 0;
    }
  }

  return have_files;
}
#endif

static int copy_files_for_filemap(
  const spath* path_prefix,
  const spath* path_scr,
  const spath* cache_path,
  const char* entryname,
  int rank,
  const struct arglist* args,
  const char* hostname)
{
  int rc = 0;

  /* define full path to the filemap */
  spath* path_filemap = spath_dup(cache_path);
  spath_append_str(path_filemap, entryname);
  spath_reduce(path_filemap);

  /* read in file map */
  scr_filemap* map = scr_filemap_new();
  scr_filemap_read(path_filemap, map);
  const char* src_filemap = spath_strdup(path_filemap);
  spath_delete(&path_filemap);

  /* allocate a rank filemap object */
  scr_filemap* rank_map = scr_filemap_new();

  /* step through each file we have for this rank */
  kvtree_elem* file_elem = NULL;
  for (file_elem = scr_filemap_first_file(map);
       file_elem != NULL;
       file_elem = kvtree_elem_next(file_elem))
  {
    /* get filename */
    char* file = kvtree_elem_key(file_elem);
  
    /* check that we can read the file */
    if (scr_bool_have_file(map, file)) {
      /* read the meta data for this file */
      scr_meta* meta = scr_meta_new();
      scr_filemap_get_meta(map, file, meta);
  
      /* TODO: filemap no longer lists redundancy files,
       * so need another way to grab those */
  
      /* get path to copy file */
      char* dst_dir = NULL;
      if (scr_meta_get_origpath(meta, &dst_dir) != SCR_SUCCESS) {
        printf("scr_copy: %s: Could not find original path for file %s in dataset id %d\n",
          hostname, file, args->id
        );
        printf("scr_copy: %s: Return code: 1\n", hostname);
        scr_meta_delete(&meta);
        scr_filemap_delete(&map);
        scr_filemap_delete(&rank_map);
        return 1;
      }
  
      /* TODO: keep a cache of directory names that we've already created */
  
      /* make directory to file */
      if (scr_mkdir(dst_dir, S_IRWXU) != SCR_SUCCESS) {
        printf("scr_copy: %s: Failed to create path for file %s in dataset id %d\n",
          hostname, file, args->id
        );
        printf("scr_copy: %s: Return code: 1\n", hostname);
        scr_meta_delete(&meta);
        scr_filemap_delete(&map);
        scr_filemap_delete(&rank_map);
        return 1;
      }
  
      /* create destination file name */
      spath* dst_path = spath_from_str(file);
      spath_basename(dst_path);
      spath_prepend_str(dst_path, dst_dir);
      spath_reduce(dst_path);
      char* dst_file = spath_strdup(dst_path);
  
      /* copy the file and optionally compute the crc during the copy */
      int crc_valid = 0;
      uLong crc = crc32(0L, Z_NULL, 0);
      uLong* crc_p = NULL;
      if (args->crc_flag) {
        crc_valid = 1;
        crc_p = &crc;
      }
      if (strcmp(file, dst_file) != 0) {
        /* in case of bypass, only copy file if source and dest paths are different */
        if (scr_file_copy(file, dst_file, args->buf_size, crc_p) != SCR_SUCCESS) {
          crc_valid = 0;
          rc = 1;
        }
      } else {
        /* TODO: should we stat file and check its size? */
        /* didn't attempt a copy, so we don't have a valid crc */
        crc_valid = 0;
      }

      /* apply metadata to file */
      if (scr_meta_apply_stat(meta, dst_file) != SCR_SUCCESS) {
        rc = 1;
        scr_err("scr_copy: Failed to copy file metadata properties from %s to %s @ %s:%d",
          file, dst_file, __FILE__, __LINE__
        );
      }
  
      /* add this file to the rank_map */
      scr_filemap_add_file(rank_map, file);
  
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
              scr_log_event("CRC32_MISMATCH", my_flushed_file, NULL, NULL, NULL);
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
      scr_filemap_set_meta(rank_map, file, meta);
  
      /* free the destination file path and string */
      scr_free(&dst_file);
      spath_delete(&dst_path);
  
      /* free the meta data object */
      scr_meta_delete(&meta);
    } else {
      /* have_file failed, so there was some problem accessing file */
      rc = 1;
      scr_err("scr_copy: File is unreadable or incomplete: CheckpointID %d, Rank %d, File: %s",
        args->id, rank, file
      );
    }
  }
  
  /* TODO: would be nice to use the updated filemap, since it has the CRC on the file,
   * but we have to keep the same file that we applied the encoding to in case we need
   * to rebuild it */
  /* write out the rank filemap for scr_index */
  spath* path_rank = spath_dup(path_scr);
  spath_append_strf(path_rank, "filemap_%d", rank);
#if 0
  if (scr_filemap_write(path_rank, rank_map) != SCR_SUCCESS) {
    rc = 1;
  }
#endif
  char* dst_filemap = spath_strdup(path_rank);
  if (scr_file_copy(src_filemap, dst_filemap, args->buf_size, NULL) != SCR_SUCCESS) {
    rc = 1;
  }
  scr_free(&dst_filemap);
  scr_free(&src_filemap);
  spath_delete(&path_rank);

  /* delete the rank filemap object */
  scr_filemap_delete(&rank_map);
  scr_filemap_delete(&map);

  return rc;
}

static int copy_files_redset(
  const spath* path_prefix,
  const spath* path_scr,
  const spath* cache_path,
  const char* entryname,
  const struct arglist* args,
  const char* hostname)
{
  int rc = 0;

  /* define full path to the source redset file */
  spath* path = spath_dup(cache_path);
  spath_append_str(path, entryname);
  spath_reduce(path);
  char* file = spath_strdup(path);

  /* define full path to the destination redset file */
  spath* dst_path = spath_dup(path_scr);
  spath_append_str(dst_path, entryname);
  spath_reduce(dst_path);
  char* dst_file = spath_strdup(dst_path);

  /* copy redset file to prefix directory */
  if (scr_file_copy(file, dst_file, args->buf_size, NULL) != SCR_SUCCESS) {
    rc = 1;
  }

  /* free our paths */
  scr_free(&dst_file);
  spath_delete(&dst_path);
  scr_free(&file);
  spath_delete(&path);

  return rc;
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

  /* read cindex file to get metadata for dataset */
  scr_cache_index* scr_cindex = scr_cache_index_new();
  spath* scr_cindex_file = spath_from_str(args.cntldir);
  spath_append_str(scr_cindex_file, "cindex.scrinfo");
  scr_cache_index_read(scr_cindex_file, scr_cindex);
  spath_delete(&scr_cindex_file);

  /* lookup path to given dataset id from cache index */
  char* cachedir = NULL;
  if (scr_cache_index_get_dir(scr_cindex, args.id, &cachedir) != SCR_SUCCESS) {
    /* failed to lookup cache path for this dataset, so we can bail early */
    printf("scr_copy: %s: Failed to find cache directory for dataset id %d\n",
      hostname, args.id
    );
    scr_cache_index_delete(&scr_cindex);
    return 1;
  }

#if 0
  kvtree_elem* rank_elem = scr_filemap_first_rank_by_dataset(map, args.id);
  if (rank_elem == NULL) {
    printf("scr_copy: %s: Do not have any files for dataset id %d\n",
      hostname, args.id
    );
    printf("scr_copy: %s: Return code: 1\n", hostname);
    scr_filemap_delete(&map);
    kvtree_delete(&hash);
    return 1;
  }
#endif

  /* path to prefix directory */
  spath* path_prefix = spath_from_str(args.prefix);
  spath_reduce(path_prefix);

  /* define the path to the dataset metadata subdirectory in prefix */
  spath* path_scr = spath_dup(path_prefix);
  spath_append_str(path_scr, ".scr");
  spath_append_strf(path_scr, "scr.dataset.%d", args.id);
  spath_reduce(path_scr);

  /* define the path to the dataset directory in cache */
  spath* cache_path = spath_from_str(cachedir);
  spath_append_str(cache_path, ".scr");
  spath_reduce(cache_path);
  char* cache_str = spath_strdup(cache_path);

  regex_t re_filemap_file;
  regcomp(&re_filemap_file, "filemap_([0-9]+)", REG_EXTENDED);

  regex_t re_redsetmap_file;
  regcomp(&re_redsetmap_file, "reddescmap.er.([0-9]+).redset", REG_EXTENDED);

  regex_t re_redsetmap_type_file;
  regcomp(&re_redsetmap_type_file, "reddescmap.er.([0-9]+).[a-z]+.grp_([0-9]+)_of_([0-9]+).mem_([0-9]+)_of_([0-9]+).redset", REG_EXTENDED);

  regex_t re_redset_file;
  regcomp(&re_redset_file, "reddesc.er.([0-9]+).redset", REG_EXTENDED);

  regex_t re_redset_type_file;
  regcomp(&re_redset_type_file, "reddesc.er.([0-9]+).[a-z]+.grp_([0-9]+)_of_([0-9]+).mem_([0-9]+)_of_([0-9]+).redset", REG_EXTENDED);

  int rc = 0;

  /* iterate over each rank we have for this dataset */
  errno = 0;
  DIR* d = opendir(cache_str);
  if (d != NULL) {
    errno = 0;
    struct dirent* de;
    while ((de = readdir(d))) {
      /* get pointer to name of entry */
      const char* entryname = de->d_name;

      int rank = -1;
      char* value = NULL;
      size_t nmatch = 5;
      regmatch_t pmatch[5];

      /* look for file names like: "filemap_0" */
      if (regexec(&re_filemap_file, entryname, nmatch, pmatch, 0) == 0) {
        /* get the MPI rank of the file */
        value = strndup(entryname + pmatch[1].rm_so, (size_t)(pmatch[1].rm_eo - pmatch[1].rm_so));
        if (value != NULL) {
          rank = atoi(value);
          scr_free(&value);
        }

        /* found a filemap, copy its files */
        int tmp_rc = copy_files_for_filemap(path_prefix, path_scr, cache_path, entryname, rank, &args, hostname);
        if (tmp_rc != 0) {
          rc = tmp_rc;
        }
        continue;
      }

      /* look for file names like: "reddescmap.er.0.redset" */
      if (regexec(&re_redsetmap_file, entryname, nmatch, pmatch, 0) == 0) {
        /* found a filemap, copy its files */
        int tmp_rc = copy_files_redset(path_prefix, path_scr, cache_path, entryname, &args, hostname);
        if (tmp_rc != 0) {
          rc = tmp_rc;
        }
        continue;
      }

      /* look for file names like: "reddescmap.er.0.partner.0_1.redset" */
      if (regexec(&re_redsetmap_type_file, entryname, nmatch, pmatch, 0) == 0) {
        /* found a filemap, copy its files */
        int tmp_rc = copy_files_redset(path_prefix, path_scr, cache_path, entryname, &args, hostname);
        if (tmp_rc != 0) {
          rc = tmp_rc;
        }
        continue;
      }

      /* look for file names like: "reddesc.er.0.redset" */
      if (regexec(&re_redset_file, entryname, nmatch, pmatch, 0) == 0) {
        /* found a filemap, copy its files */
        int tmp_rc = copy_files_redset(path_prefix, path_scr, cache_path, entryname, &args, hostname);
        if (tmp_rc != 0) {
          rc = tmp_rc;
        }
        continue;
      }

      /* look for file names like: "reddesc.er.0.partner.0_1.redset" */
      if (regexec(&re_redset_type_file, entryname, nmatch, pmatch, 0) == 0) {
        /* found a filemap, copy its files */
        int tmp_rc = copy_files_redset(path_prefix, path_scr, cache_path, entryname, &args, hostname);
        if (tmp_rc != 0) {
          rc = tmp_rc;
        }
        continue;
      }
    }

    /* close directory */
    errno = 0;
    if (closedir(d) == -1) {
      /* failed to close directory */
    }
  } else {
    /* failed to open directory */
    printf("scr_copy: %s: Failed to open directory %s in dataset id %d\n",
      hostname, cache_str, args.id
    );
    rc = 1;
  }

  /* free our regular expressions */
  regfree(&re_filemap_file);
  regfree(&re_redsetmap_file);
  regfree(&re_redsetmap_type_file);
  regfree(&re_redset_file);
  regfree(&re_redset_type_file);

  /* free string pointing to cache directory for this dataset */
  scr_free(&cache_str);
  spath_delete(&cache_path);

  /* delete path to dataset metadata directory */
  spath_delete(&path_scr);

  /* free the prefix directory path */
  spath_delete(&path_prefix);

  scr_cache_index_delete(&scr_cindex);

  /* print our return code and exit */
  printf("scr_copy: %s: Return code: %d\n", hostname, rc);
  return rc;
}
