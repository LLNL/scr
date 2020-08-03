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

/* Utility to rebuild a missing file given the file names of the
 * remaining N-1 data files and the N-1 XOR segments. */

#include "scr.h"
#include "scr_io.h"
#include "scr_meta.h"
#include "scr_err.h"
#include "scr_util.h"
#include "scr_filemap.h"

#include "spath.h"
#include "kvtree.h"
#include "kvtree_util.h"

#include "redset.h"

#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>

/* variable length args */
#include <stdarg.h>
#include <errno.h>

/* basename/dirname */
#include <unistd.h>
#include <libgen.h>

#ifdef SCR_GLOBALS_H
#error "globals.h accessed from tools"
#endif

/* given a file map, and a path to a file in cache, allocate and return
 * corresponding path to file in prefix directory */
static char* lookup_path(const scr_filemap* map, const char* file)
{
  /* lookup metadata for file in filemap */
  scr_meta* meta = scr_meta_new();
  scr_filemap_get_meta(map, file, meta);
  
  /* get original filename */
  char* origname;
  if (scr_meta_get_origname(meta, &origname) != SCR_SUCCESS) {
    scr_err("Failed to read original name for file %s @ %s:%d",
      file, __FILE__, __LINE__
    );
    return NULL;
  }

  /* get original path of file */
  char* origpath;
  if (scr_meta_get_origpath(meta, &origpath) != SCR_SUCCESS) {
    scr_err("Failed to read original path for file %s @ %s:%d",
      file, __FILE__, __LINE__
    );
    return NULL;
  }

  /* construct full path to file */
  spath* path_user_full = spath_from_str(origname);
  spath_prepend_str(path_user_full, origpath);
  spath_reduce(path_user_full);

  /* make a copy of the full path */
  char* path = spath_strdup(path_user_full);

  /* free path and meta */
  spath_delete(&path_user_full);
  scr_meta_delete(&meta);

  return path;
}

/* this defines an output map that translates the path of the filemap
 * as it was stored in cache to the map now stored in the prefix directory
 * after a scavenge, this map will be needed to tell redset where those
 * files are now located */
int build_map_filemap(
  const spath* path_prefix,
  int set_size,
  const char** files,
  int* ranks,
  int missing,
  kvtree* map)
{
  int rc = 0;

  /* we're missing one rank, so the number of files
   * is one less than the set size */
  int numfiles = set_size - 1;

  /* get a full listing of data files that are encoded in redundancy files */
  redset_filelist list = redset_filelist_get_data_xor(numfiles, files);

  if (list == NULL) {
    /* failed to get a list */
    return 1;
  }

  /* get number of data files */
  int num = redset_filelist_count(list);

  /* iterate over list of files and define its new path for each one */
  int j;
  for (j = 0; j < num; j++) {
    /* get name for this file */
    const char* file = redset_filelist_file(list, j);

    /* for filemap files, we're running this command in the same directory
     * so we can just use the basename to open each of those */
    spath* path_name = spath_from_str(file);
    spath_basename(path_name);
    char* new_file = spath_strdup(path_name);
    spath_delete(&path_name);

    /* map from filemap as it was in cache to its new location
     * in the current working directory */
    kvtree_util_set_str(map, file, new_file);

    scr_free(&new_file);
  }

  /* done with the list of files */
  redset_filelist_release(&list);

  return rc;
}

/* this defines an output map that translates the path of each user data file
 * as it was stored in cache to the location where it is now stored within
 * the prefix directory after a scavenge, this map is needed to tell redset
 * where those files are now located */
int build_map_data(
  const spath* path_prefix, /* path to the filemap (could probably drop this) */
  int set_size, /* size of redundancy set */
  int* ranks,   /* global mpi rank of each member in the redundancy set */
  int missing,  /* index within the redundancy set for the missing member */
  kvtree* map)  /* output map that maps data file in cache to its location within prefix directory */
{
  int rc = 0;

  /* get file name, file size, and open each of the user files that we have */
  int i;
  for (i = 0; i < set_size; i++) {
    /* lookup global mpi rank for this group rank */
    int rank = ranks[i];

    /* define name of filemap file for this rank */
    spath* filemap_path = spath_dup(path_prefix);
    spath_append_strf(filemap_path, "filemap_%d", rank);

    /* read in filemap for this member */
    scr_filemap* filemap = scr_filemap_new();
    scr_filemap_read(filemap_path, filemap);

    /* free the name of the filemap file */
    spath_delete(&filemap_path);

    /* get list of files from the filemap */
    int num;
    char** files;
    scr_filemap_list_files(filemap, &num, &files);

    /* iterate over each file to define its new
     * path and record in the output map */
    int j;
    for (j = 0; j < num; j++) {
      /* get original file name */
      char* file = files[j];

      /* get path of file, we have to remap based on filemap info */
      char* new_file = lookup_path(filemap, file);

      /* map original file name to new location */
      kvtree_util_set_str(map, file, new_file);

      /* if this is the root, also create pre-create
       * directory for this file */
      if (i == missing) {
        /* get parent directory for file */
        spath* user_dir_path = spath_from_str(new_file);
        spath_reduce(user_dir_path);
        spath_dirname(user_dir_path);

        /* create directory */
        if (! spath_is_null(user_dir_path)) {
          char* user_dir = spath_strdup(user_dir_path);
          mode_t mode_dir = scr_getmode(1, 1, 1);
          if (scr_mkdir(user_dir, mode_dir) != SCR_SUCCESS) {
            scr_err("Failed to create directory for user file %s @ %s:%d",
              user_dir, __FILE__, __LINE__
            );
            rc = 1;
          }
          scr_free(&user_dir);
        }

        /* free directory */
        spath_delete(&user_dir_path);
      }

      scr_free(&new_file);
    }

    scr_free(&files);
    scr_filemap_delete(&filemap);
  }

  return rc;
}

int rebuild(const spath* path_prefix, int build_data, int index, const char* argv[])
{
  int rc = 0;

  /* read in the size of the redundancy set */
  int set_size = (int) strtol(argv[index++], (char **)NULL, 10);
  if (set_size <= 0) {
    scr_err("Invalid set size argument %s @ %s:%d",
      argv[index-1], __FILE__, __LINE__
    );
    return 1;
  }

  /* read in the rank of the missing process (the root) */
  int root = (int) strtol(argv[index++], (char **)NULL, 10);
  if (root < 0 || root >= set_size) {
    scr_err("Invalid root argument %s @ %s:%d",
      argv[index-1], __FILE__, __LINE__
    );
    return 1;
  }

  /* allocate memory for data structures based on the set size */
  int* ranks = malloc(set_size * sizeof(int*));
  if (ranks == NULL) {
    scr_err("Failed to allocate array for rank list @ %s:%d",
      __FILE__, __LINE__
    );
    return 1;
  }

  /* set rank of missing rank (within its group) */
  int missing = root;

  /* get list of global rank ids in set */
  redset_lookup_ranks_xor(set_size, &argv[index], ranks, &missing);

  /* define name for missing XOR file */
  spath* file_prefix = spath_dup(path_prefix);
  kvtree* map = kvtree_new();
  if (build_data) {
    spath_append_str(file_prefix, "reddesc.er.");
    build_map_data(path_prefix, set_size, ranks, missing, map);
  } else {
    spath_append_str(file_prefix, "reddescmap.er.");
    build_map_filemap(path_prefix, set_size, &argv[index], ranks, missing, map);
  }
  char* prefix = spath_strdup(file_prefix);
  spath_delete(&file_prefix);

  redset_rebuild_xor(set_size, root, &argv[index], prefix, map);

  scr_free(&prefix);
  kvtree_delete(&map);

  scr_free(&ranks);

  return rc;
}

int main(int argc, char* argv[])
{
  /* print usage if not enough arguments were given */
  if (argc < 2) {
    printf("Usage: scr_rebuild_xor <xor|map> <size> <root> <ordered_remaining_xor_filenames>\n");
    return 1;
  }

  int index = 1;

  /* TODO: want to pass this on command line? */
  /* get current working directory */
  char dsetdir[SCR_MAX_FILENAME];
  scr_getcwd(dsetdir, sizeof(dsetdir));

  /* create and reduce path for dataset */
  spath* path_prefix = spath_from_str(dsetdir);
  spath_reduce(path_prefix);

  /* rebuild filemaps if given map command */
  int rc = 1;
  if (strcmp(argv[index++], "map") == 0) {
    rc = rebuild(path_prefix, 0, index, (const char**)argv);
  } else {
    rc = rebuild(path_prefix, 1, index, (const char**)argv);
  }

  spath_delete(&path_prefix);

  return rc;
}
