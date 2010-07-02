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

/* Reads filemap and reports info on checkpoints which need flushed */

#include "scr.h"
#include "scr_err.h"
#include "scr_io.h"
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
static char* scr_master_map_file = NULL;

/* checks whether specifed file exists, is readable, and is complete */
static int scr_bool_have_file(const char* file, int ckpt, int rank)
{
  /* if no filename is given return false */
  if (file == NULL || strcmp(file,"") == 0) {
    scr_dbg(2, "scr_bool_have_file: File name is null or the empty string");
    return 0;
  }

  /* check that we can read the file */
  if (access(file, R_OK) < 0) {
    scr_dbg(2, "scr_bool_have_file: Do not have read access to file: %s", file);
    return 0;
  }

  /* check that we can read meta file for the file */
  struct scr_meta meta;
  if (scr_meta_read(file, &meta) != SCR_SUCCESS) {
    scr_dbg(2, "scr_bool_have_file: Failed to read meta data file for file: %s", file);
    return 0;
  }

  /* check that the file is complete */
  if (!meta.complete) {
    scr_dbg(2, "scr_bool_have_file: File is marked as incomplete: %s", file);
    return 0;
  }

  /* check that the file really belongs to the checkpoint id we think it does */
  if (meta.checkpoint_id != ckpt) {
    scr_dbg(2, "scr_bool_have_file: File's checkpoint ID (%d) does not match id in meta data file (%d) for %s",
            ckpt, meta.checkpoint_id, file
    );
    return 0;
  }

  /* check that the file really belongs to the rank we think it does */
  if (meta.rank != rank) {
    scr_dbg(2, "scr_bool_have_file: File's rank (%d) does not match rank in meta data file (%d) for %s",
            rank, meta.rank, file
    );
    return 0;
  }

  /* check that the file size matches (use strtol while reading data) */
  unsigned long size = scr_filesize(file);
  if (meta.filesize != size) {
    scr_dbg(2, "scr_bool_have_file: Filesize is incorrect, currently %lu, expected %lu for %s",
            size, meta.filesize, file
    );
    return 0;
  }

  /* TODO: check that crc32 match if set (this would be expensive) */

  /* if we made it here, assume the file is good */
  return 1;
}

int main(int argc, char* argv[])
{
  int i, j;
  int index = 1;

  /* print usage if not enough arguments were given */
  if (argc < 2) {
    printf("Usage: scr_inspect_cache <cntldir>\n");
    return 1;
  }

  scr_master_map_file = strdup(argv[1]);

  /* get my hostname */
  if (gethostname(scr_my_hostname, sizeof(scr_my_hostname)) != 0) {
    scr_err("scr_inspect_cache: Call to gethostname failed @ %s:%d",
            __FILE__, __LINE__
    );
    return 1;
  }

  /* read in the master map */
  struct scr_hash* hash = scr_hash_new();
  scr_hash_read(scr_master_map_file, hash);

  /* create an empty filemap */
  scr_filemap* map = scr_filemap_new();

  /* for each filemap listed in the master map */
  struct scr_hash_elem* elem;
  for (elem = scr_hash_elem_first(scr_hash_get(hash, "Filemap"));
       elem != NULL;
       elem = scr_hash_elem_next(elem))
  {
    /* get the filename of this filemap */
    char* file = scr_hash_elem_key(elem);

    /* read in the filemap */
    scr_filemap* tmp_map = scr_filemap_new();
    scr_filemap_read(file, tmp_map);

    /* merge it with local 0 filemap */
    scr_filemap_merge(map, tmp_map);

    /* delete filemap */
    scr_filemap_delete(tmp_map);
  }

  /* scan each file for each rank of each checkpoint */
  struct scr_hash_elem* ckpt_elem;
  for (ckpt_elem = scr_filemap_first_checkpoint(map);
       ckpt_elem != NULL;
       ckpt_elem = scr_hash_elem_next(ckpt_elem))
  {
    int ckpt = scr_hash_elem_key_int(ckpt_elem);

    struct scr_hash_elem* rank_elem;
    for (rank_elem = scr_filemap_first_rank_by_checkpoint(map, ckpt);
         rank_elem != NULL;
         rank_elem = scr_hash_elem_next(rank_elem))
    {
      int rank = scr_hash_elem_key_int(rank_elem);

      int missing_file = 0;
      int expected = scr_filemap_num_expected_files(map, ckpt, rank);
      int num      = scr_filemap_num_files(map, ckpt, rank);
      if (expected == num) {
        /* first time through the file list, check that we have each file */
        struct scr_hash_elem* file_elem = NULL;
        for (file_elem = scr_filemap_first_file(map, ckpt, rank);
             file_elem != NULL;
             file_elem = scr_hash_elem_next(file_elem))
        {
          /* get filename and check that we can read it */
          char* file = scr_hash_elem_key(file_elem);
          if (!scr_bool_have_file(file, ckpt, rank)) {
              missing_file = 1;
              scr_dbg(1, "File is unreadable or incomplete: CheckpointID %d, Rank %d, File: %s",
                      ckpt, rank, file
              );
          }
        }
      } else {
        missing_file = 1;
      }

      /* TODO: print partner names */
      /* if we're not missing a file for rank, print this info out */
      if (!missing_file) {
        struct scr_hash* desc = scr_hash_new();
        scr_filemap_get_desc(map, ckpt, rank, desc);
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
          printf("CKPT=%d RANK=%d TYPE=%s GROUPS=%d GROUP_ID=%d GROUP_SIZE=%d GROUP_RANK=%d FILES=1\n",
                 ckpt, rank, type, groups, group_id, group_size, group_rank
          );
        }
      }
    }
  }

  if (scr_master_map_file != NULL) {
    free(scr_master_map_file);
    scr_master_map_file = NULL;
  }

  return 0;
}
