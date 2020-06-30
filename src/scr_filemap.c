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

/* Defines a data structure that keeps track of the number
 * and the names of the files a process writes out in a given
 * dataset. */

/*
GOALS:
  - support different number of processes per node on
    a restart
  - support multiple files per rank per dataset
  - support multiple datasets at different cache levels

READ:
  leader process on each node reads filemap
  and distributes pieces to others

WRITE:
  all processes send their file info to leader
  and leader writes it out

  leader filemap file
    list of ranks this node has files for
      for each rank, list of dataset ids
        for each dataset id, list of locations (RAM, SSD, PFS, etc)
            for each location, list of files for this rank for this dataset
*/

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <unistd.h>

#include "scr.h"
#include "scr_io.h"
#include "scr_err.h"
#include "scr_util.h"
#include "scr_filemap.h"

#include "spath.h"
#include "kvtree.h"
#include "kvtree_util.h"

#define SCR_FILEMAP_KEY_FILES   ("FILES")
#define SCR_FILEMAP_KEY_FILE    ("FILE")
#define SCR_FILEMAP_KEY_DATA    ("DSETDESC")
#define SCR_FILEMAP_KEY_META    ("META")

/* returns the FILE hash associated with filemap */
static kvtree* scr_filemap_get_fh(const kvtree* hash)
{
  kvtree* fh = kvtree_get(hash, SCR_FILEMAP_KEY_FILE);
  return fh;
}

/* returns the hash associated with a particular file */
static kvtree* scr_filemap_get_f(const kvtree* hash, const char* file)
{
  kvtree* fh  = scr_filemap_get_fh(hash);
  kvtree* f = kvtree_get(fh, file);
  return f;
}

/* adds a new filename to the filemap */
int scr_filemap_add_file(scr_filemap* map, const char* file)
{
  /* add file to FILE hash */
  kvtree_set_kv(map, SCR_FILEMAP_KEY_FILE, file);

  return SCR_SUCCESS;
}

/* removes a filename from the filemap */
int scr_filemap_remove_file(scr_filemap* map, const char* file)
{
  /* remove file from FILE hash */
  kvtree_unset_kv(map, SCR_FILEMAP_KEY_FILE, file);

  return SCR_SUCCESS;
}

/* sets the dataset for the files */
int scr_filemap_set_dataset(scr_filemap* map, kvtree* hash)
{
  /* set the DATA value under the DSET hash */
  kvtree_unset(map, SCR_FILEMAP_KEY_DATA);
  kvtree* desc = kvtree_new();
  kvtree_merge(desc, hash);
  kvtree_set(map, SCR_FILEMAP_KEY_DATA, desc);

  return SCR_SUCCESS;
}

/* copies the dataset for the files */
int scr_filemap_get_dataset(const scr_filemap* map, kvtree* hash)
{
  /* get the REDDESC value under the DSET hash */
  kvtree* desc = kvtree_get(map, SCR_FILEMAP_KEY_DATA);
  if (desc != NULL) {
    kvtree_merge(hash, desc);
    return SCR_SUCCESS;
  }

  return SCR_FAILURE; 
}

/* unset the dataset */
int scr_filemap_unset_dataset(scr_filemap* map)
{
  /* unset DATA value */
  kvtree_unset(map, SCR_FILEMAP_KEY_DATA);

  return SCR_SUCCESS;
}

/* sets metadata for file */
int scr_filemap_set_meta(scr_filemap* map, const char* file, const scr_meta* meta)
{
  /* get FILE hash */
  kvtree* f = scr_filemap_get_f(map, file);

  /* add metadata */
  if (f != NULL) {
    kvtree_unset(f, SCR_FILEMAP_KEY_META);
    scr_meta* meta_copy = scr_meta_new();
    scr_meta_copy(meta_copy, meta);
    kvtree_set(f, SCR_FILEMAP_KEY_META, meta_copy);
    return SCR_SUCCESS;
  }

  return SCR_FAILURE;
}

/* gets metadata for file */
int scr_filemap_get_meta(const scr_filemap* map, const char* file, scr_meta* meta)
{
  /* get FILE hash */
  kvtree* f = scr_filemap_get_f(map, file);

  /* copy metadata if it is set */
  scr_meta* meta_copy = kvtree_get(f, SCR_FILEMAP_KEY_META);
  if (meta_copy != NULL) {
    scr_meta_copy(meta, meta_copy);
    return SCR_SUCCESS;
  }

  return SCR_FAILURE;
}

/* unsets metadata for file */
int scr_filemap_unset_meta(scr_filemap* map, const char* file)
{
  /* set indicies and get hash reference */
  kvtree* f = scr_filemap_get_f(map, file);

  /* unset metadata */
  kvtree_unset(f, SCR_FILEMAP_KEY_META);

  return SCR_SUCCESS;
}

/* clear the filemap completely */
int scr_filemap_clear(scr_filemap* map)
{
  return kvtree_unset_all(map);
}

/* given a filemap, a dataset id, and a rank, return the number of files and a list of the filenames */
int scr_filemap_list_files(const scr_filemap* map, int* n, char*** v)
{
  /* assume there aren't any matching files */
  *n = 0;
  *v = NULL;

  /* get rank element */
  kvtree* fh = scr_filemap_get_fh(map);
  int count = kvtree_size(fh);
  if (count == 0) {
    return SCR_SUCCESS;
  }

  /* now allocate array of pointers to the filenames */
  char** list = (char**) SCR_MALLOC(count * sizeof(char*));

  /* record pointer values in array */
  count = 0;
  kvtree_elem* file;
  for (file = kvtree_elem_first(fh);
       file != NULL;
       file = kvtree_elem_next(file))
  {
    list[count] = kvtree_elem_key(file);
    count++;
  }

  *n = count;
  *v = list;

  return SCR_SUCCESS;
}

/* given a filemap, return a hash elem pointer to the first file */
kvtree_elem* scr_filemap_first_file(const scr_filemap* map)
{
  kvtree* fh = scr_filemap_get_fh(map);
  kvtree_elem* elem = kvtree_elem_first(fh);
  return elem;
}

/* return the number of files in the filemap */
int scr_filemap_num_files(const scr_filemap* map)
{
  kvtree* fh = scr_filemap_get_fh(map);
  int size = kvtree_size(fh);
  return size;
}

/* allocate a new filemap structure and return it */
scr_filemap* scr_filemap_new()
{
  scr_filemap* map = kvtree_new();
  if (map == NULL) {
    scr_err("Failed to allocate filemap @ %s:%d", __FILE__, __LINE__);
  }
  return map;
}

/* free memory resources assocaited with filemap */
int scr_filemap_delete(scr_filemap** ptr_map)
{
  kvtree_delete(ptr_map);
  return SCR_SUCCESS;
}

/* adds all files from map2 to map1 */
int scr_filemap_merge(scr_filemap* map1, scr_filemap* map2)
{
  kvtree_merge(map1, map2);
  return SCR_SUCCESS;
}

/* reads specified file and fills in filemap structure */
int scr_filemap_read(const spath* path_file, scr_filemap* map)
{
  /* check that we have a map pointer and a hash within the map */
  if (map == NULL) {
    return SCR_FAILURE;
  }

  /* assume we'll fail */
  int rc = SCR_FAILURE;

  /* get file name */
  char* file = spath_strdup(path_file);

  /* can't read file, return error (special case so as not to print error message below) */
  if (scr_file_is_readable(file) != SCR_SUCCESS) {
    goto cleanup;
  }

  /* ok, now try to read the file */
  if (kvtree_read_file(file, map) != KVTREE_SUCCESS) {
    scr_err("Reading filemap %s @ %s:%d",
      file, __FILE__, __LINE__
    );
    goto cleanup;
  }

  /* TODO: check that file count for each rank matches expected count */

  /* success if we make it this far */
  rc = SCR_SUCCESS;

cleanup:
  /* free file name string */
  scr_free(&file);

  return rc;
}

/* writes given filemap to specified file */
int scr_filemap_write(const spath* file, const scr_filemap* map)
{
  /* check that we have a map pointer */
  if (map == NULL) {
    return SCR_FAILURE;
  }

  /* write out the hash */
  if (kvtree_write_path(file, map) != KVTREE_SUCCESS) {
    char path_err[SCR_MAX_FILENAME];
    spath_strcpy(path_err, sizeof(path_err), file);
    scr_err("Writing filemap %s @ %s:%d",
      path_err, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  return SCR_SUCCESS;
}
