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

/* Reads filemap and reports info on datasets which need flushed */

#include "scr.h"
#include "scr_err.h"
#include "scr_io.h"
#include "scr_path.h"
#include "scr_util.h"
#include "scr_meta.h"
#include "scr_hash.h"
#include "scr_filemap.h"

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

int buffer_size = 128*1024;

static char scr_my_hostname[SCR_MAX_FILENAME];

/* checks whether specifed file exists, is readable, and is complete */
static int scr_bool_have_file(const scr_filemap* map, int dset, int rank, const char* file)
{
  /* if no filename is given return false */
  if (file == NULL || strcmp(file,"") == 0) {
    scr_dbg(2, "File name is null or the empty string @ %s:%d",
      __FILE__, __LINE__
    );
    return 0;
  }

  /* check that we can read the file */
  if (scr_file_is_readable(file) != SCR_SUCCESS) {
    scr_dbg(2, "Do not have read access to file: %s @ %s:%d",
      file, __FILE__, __LINE__
    );
    return 0;
  }

  /* check that we can read meta file for the file */
  scr_meta* meta = scr_meta_new();
  if (scr_filemap_get_meta(map, dset, rank, file, meta) != SCR_SUCCESS) {
    scr_dbg(2, "Failed to read meta data for file: %s @ %s:%d",
      file, __FILE__, __LINE__
    );
    scr_meta_delete(&meta);
    return 0;
  }

  /* check that the file is complete */
  if (scr_meta_is_complete(meta) != SCR_SUCCESS) {
    scr_dbg(2, "File is marked as incomplete: %s @ %s:%d",
      file, __FILE__, __LINE__
    );
    scr_meta_delete(&meta);
    return 0;
  }

  /* TODODSET: check that dataset id matches */
#if 0
  /* check that the file really belongs to the checkpoint id we think it does */
  int meta_dset = -1;
  if (scr_meta_get_checkpoint(meta, &meta_dset) != SCR_SUCCESS) {
    scr_dbg(2, "Failed to read checkpoint field in meta data: %s @ %s:%d",
      file, __FILE__, __LINE__
    );
    scr_meta_delete(&meta);
    return 0;
  }
  if (dset != meta_dset) {
    scr_dbg(2, "File's checkpoint ID (%d) does not match id in meta data file (%d) for %s @ %s:%d",
      dset, meta_dset, file, __FILE__, __LINE__
    );
    scr_meta_delete(&meta);
    return 0;
  }
#endif

#if 0
  /* check that the file really belongs to the rank we think it does */
  int meta_rank = -1;
  if (scr_meta_get_rank(meta, &meta_rank) != SCR_SUCCESS) {
    scr_dbg(2, "Failed to read rank field in meta data: %s @ %s:%d",
      file, __FILE__, __LINE__
    );
    scr_meta_delete(&meta);
    return 0;
  }
  if (rank != meta_rank) {
    scr_dbg(2, "File's rank (%d) does not match rank in meta data file (%d) for %s @ %s:%d",
      rank, meta_rank, file, __FILE__, __LINE__
    );
    scr_meta_delete(&meta);
    return 0;
  }
#endif

#if 0
  /* check that the file was written with same number of ranks we think it was */
  int meta_ranks = -1;
  if (scr_meta_get_ranks(meta, &meta_ranks) != SCR_SUCCESS) {
    scr_dbg(2, "Failed to read ranks field in meta data: %s @ %s:%d",
      file, __FILE__, __LINE__
    );
    scr_meta_delete(&meta);
    return 0;
  }
  if (ranks != meta_ranks) {
    scr_dbg(2, "File's ranks (%d) does not match ranks in meta data file (%d) for %s @ %s:%d",
      ranks, meta_ranks, file, __FILE__, __LINE__
    );
    scr_meta_delete(&meta);
    return 0;
  }
#endif

  /* check that the file size matches (use strtol while reading data) */
  unsigned long size = scr_file_size(file);
  unsigned long meta_size = 0;
  if (scr_meta_get_filesize(meta, &meta_size) != SCR_SUCCESS) {
    scr_dbg(2, "Failed to read filesize field in meta data: %s @ %s:%d",
      file, __FILE__, __LINE__
    );
    scr_meta_delete(&meta);
    return 0;
  }
  if (size != meta_size) {
    scr_dbg(2, "Filesize is incorrect, currently %lu, expected %lu for %s @ %s:%d",
      size, meta_size, file, __FILE__, __LINE__
    );
    scr_meta_delete(&meta);
    return 0;
  }

  /* TODO: check that crc32 match if set (this would be expensive) */

  scr_meta_delete(&meta);

  /* if we made it here, assume the file is good */
  return 1;
}

int main(int argc, char* argv[])
{
  /* print usage if not enough arguments were given */
  if (argc < 2) {
    printf("Usage: scr_inspect_cache <cntldir>\n");
    return 1;
  }

  scr_path* scr_master_map_file = scr_path_from_str(strdup(argv[1]));

  /* get my hostname */
  if (gethostname(scr_my_hostname, sizeof(scr_my_hostname)) != 0) {
    scr_err("scr_inspect_cache: Call to gethostname failed @ %s:%d",
      __FILE__, __LINE__
    );
    return 1;
  }

  /* read in the master map */
  scr_hash* hash = scr_hash_new();
  scr_hash_read_path(scr_master_map_file, hash);

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

  /* scan each file for each rank of each dataset */
  scr_hash_elem* dset_elem;
  for (dset_elem = scr_filemap_first_dataset(map);
       dset_elem != NULL;
       dset_elem = scr_hash_elem_next(dset_elem))
  {
    /* get dataset id */
    int dset = scr_hash_elem_key_int(dset_elem);

    scr_hash_elem* rank_elem;
    for (rank_elem = scr_filemap_first_rank_by_dataset(map, dset);
         rank_elem != NULL;
         rank_elem = scr_hash_elem_next(rank_elem))
    {
      /* get rank id */
      int rank = scr_hash_elem_key_int(rank_elem);

      int missing_file = 0;
      int expected = scr_filemap_get_expected_files(map, dset, rank);
      int num      = scr_filemap_num_files(map, dset, rank);
      if (expected == num) {
        /* first time through the file list, check that we have each file */
        scr_hash_elem* file_elem = NULL;
        for (file_elem = scr_filemap_first_file(map, dset, rank);
             file_elem != NULL;
             file_elem = scr_hash_elem_next(file_elem))
        {
          /* get filename */
          char* file = scr_hash_elem_key(file_elem);

          /* check that we can read the file */
          if (! scr_bool_have_file(map, dset, rank, file)) {
              missing_file = 1;
              scr_dbg(1, "File is unreadable or incomplete: Dataset %d, Rank %d, File: %s",
                dset, rank, file
              );
          }
        }
      } else {
        missing_file = 1;
      }

      /* TODO: print partner names */
      /* if we're not missing a file for rank, print this info out */
      if (! missing_file) {
        scr_hash* desc = scr_hash_new();
        scr_filemap_get_desc(map, dset, rank, desc);
        char* type           = scr_hash_elem_get_first_val(desc, SCR_CONFIG_KEY_TYPE);
        char* groups_str     = scr_hash_elem_get_first_val(desc, SCR_CONFIG_KEY_GROUPS);
        char* group_id_str   = scr_hash_elem_get_first_val(desc, SCR_CONFIG_KEY_GROUP_ID);
        char* group_size_str = scr_hash_elem_get_first_val(desc, SCR_CONFIG_KEY_GROUP_SIZE);
        char* group_rank_str = scr_hash_elem_get_first_val(desc, SCR_CONFIG_KEY_GROUP_RANK);
        if (type != NULL && groups_str != NULL && group_id_str != NULL && group_size_str != NULL && group_rank_str != NULL) {
          /* we already have a group id and rank, use that to rebuild the communicator */
          int groups     = atoi(groups_str);
          int group_id   = atoi(group_id_str);
          int group_size = atoi(group_size_str);
          int group_rank = atoi(group_rank_str);
          printf("DSET=%d RANK=%d TYPE=%s GROUPS=%d GROUP_ID=%d GROUP_SIZE=%d GROUP_RANK=%d FILES=1\n",
            dset, rank, type, groups, group_id, group_size, group_rank
          );
        }
      }
    }
  }

  scr_path_delete(&scr_master_map_file);

  return 0;
}
