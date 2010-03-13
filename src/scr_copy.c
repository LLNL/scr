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

/* This is a utility program that lets one list, set, and unset values
 * in the halt file.  It's a small C program which must run on the
 * same node where rank 0 runs -- it's coordinates access to the halt
 * file with rank 0 via flock(), which does not work across NFS.
 *
 * One will typically call some other script, which in turn identifies
 * the rank 0 node and issues a remote shell command to run this utility. */

#include "scr_conf.h"
#include "scr.h"
#include "scr_io.h"
#include "scr_err.h"
#include "scr_util.h"
#include "scr_meta.h"
#include "scr_hash.h"
#include "scr_filemap.h"

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

static char hostname[256] = "UNKNOWN_HOST";

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

static int scr_bool_have_files(scr_filemap* map, int ckpt, int rank)
{
  /* check that the expected number of files matches the real number of files */
  int exp_files = scr_filemap_num_expected_files(map, ckpt, rank);
  int num_files = scr_filemap_num_files(map, ckpt, rank);
  if (exp_files != num_files) {
    return 0;
  }

  int have_files = 1;

  /* now check that we have each file */
  struct scr_hash_elem* rank_elem;
  for (rank_elem = scr_filemap_first_rank_by_checkpoint(map, ckpt);
       rank_elem != NULL;
       rank_elem = scr_hash_elem_next(rank_elem))
  {
    int rank = scr_hash_elem_key_int(rank_elem);

    struct scr_hash_elem* file_elem = NULL;
    for (file_elem = scr_filemap_first_file(map, ckpt, rank);
         file_elem != NULL;
         file_elem = scr_hash_elem_next(file_elem))
    {
      /* get filename and check that we can read it */
      char* file = scr_hash_elem_key(file_elem);

      if (! scr_bool_have_file(file, ckpt, rank)) {
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

  /* scr_copy <cntldir> <id> <dstdir> <buf_size> <crc_flag> <partner_flag> spaced down nodes*/

  /* check that we were given at least one argument (the transfer file name) */
  int req_args = 7;
  if (argc < req_args) {
    printf("Usage: scr_copy <cntldir> <id> <dstdir> <buf_size> <crc_flag> <partner_flag> spaced down nodes\n");
    printf("scr_copy: %s: Return code: 1\n", hostname);
    return 1;
  }

  /* record the name of the control directory */
  char* cntldir = strdup(argv[1]);
  if (cntldir == NULL) {
    scr_err("scr_copy: Failed to dup name of master filemap @ %s:%d",
            __FILE__, __LINE__
    );
    printf("scr_copy: %s: Return code: 1\n", hostname);
    return 1;
  }

  /* build the name of the master filemap and the flush file */
  char scr_master_map_file[SCR_MAX_FILENAME];
  char scr_flush_file[SCR_MAX_FILENAME];
  sprintf(scr_master_map_file, "%s/filemap.scrinfo", cntldir);
  sprintf(scr_flush_file,      "%s/flush.scrinfo",   cntldir);

  /* get the checkpoint id we should be copying */
  int ckpt = atoi(argv[2]);

  /* get the destination directory name */
  char* dir = strdup(argv[3]);
  if (dir == NULL) {
    scr_err("scr_copy: Failed to dup destination directory name @ %s:%d",
            __FILE__, __LINE__
    );
    free(cntldir);
    printf("scr_copy: %s: Return code: 1\n", hostname);
    return 1;
  }

  /* get the buffer size */
  unsigned long buf_size = atoi(argv[4]);

  /* get the crc flag */
  int crc_flag = atoi(argv[5]);

  /* get the partner flag */
  int partner_flag = atoi(argv[6]);

  /* read the flush file */
  struct scr_hash* flush = scr_hash_new();
  scr_hash_read(scr_flush_file, flush);

  /* check whether we have the specified checkpoint id */
  struct scr_hash* ckpt_hash = scr_hash_get_kv_int(flush, SCR_FLUSH_KEY_CKPT, ckpt);
  if (ckpt_hash == NULL) {
    scr_hash_delete(flush);
    free(dir);
    free(cntldir);
    printf("scr_copy: %s: Do not have any files for checkpoint id %d\n", hostname, ckpt);
    printf("scr_copy: %s: Return code: 1\n", hostname);
    return 1;
  }

  /* if this checkpoint has already been flushed, return success immediately */
  struct scr_hash* flush_hash = scr_hash_get_kv(ckpt_hash, SCR_FLUSH_KEY_LOCATION, SCR_FLUSH_KEY_LOCATION_PFS);
  if (flush_hash != NULL) {
    scr_hash_delete(flush);
    free(dir);
    free(cntldir);
    printf("scr_copy: %s: Return code: 0\n", hostname);
    return 0;
  }

  /* free the flush file hash object */
  scr_hash_delete(flush);

  /* TODO: get list of partner nodes I have files for */

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

  int rc = 0;

  /* iterate over each file for each rank we have for this checkpoint */
  struct scr_hash_elem* rank_elem;
  for (rank_elem = scr_filemap_first_rank_by_checkpoint(map, ckpt);
       rank_elem != NULL;
       rank_elem = scr_hash_elem_next(rank_elem))
  {
    int rank = scr_hash_elem_key_int(rank_elem);

    char* partner = scr_filemap_get_tag(map, ckpt, rank, SCR_FILEMAP_KEY_PARTNER);

    /* if this rank is not a partner, and out partner only flag is set, skip this rank */
    if (partner == NULL && partner_flag) {
      continue;
    }

    /* if partner is set, print out the node name */
    if (partner != NULL) {
      /* print partner node name */
      printf("scr_copy: %s: Partners: %s\n", hostname, partner);

      /* if this rank is a partner, only copy files if this node failed */
      int my_partner_failed = 0;
      int i;
      for (i=req_args; i < argc; i++) {
        if (strcmp(argv[i], partner) == 0) {
          my_partner_failed = 1;
        }
      }
      if (!my_partner_failed) {
        continue;
      }
    }

    /* only copy data if we actually have the files */
    if (! scr_bool_have_files(map, ckpt, rank)) {
      continue;
    }

    /* allocate a rank filemap object and set the expected number of files */
    scr_filemap* rank_map = scr_filemap_new();
    int num_files = scr_filemap_num_expected_files(map, ckpt, rank);
    scr_filemap_set_expected_files(rank_map, ckpt, rank, num_files);

    struct scr_hash_elem* file_elem = NULL;
    for (file_elem = scr_filemap_first_file(map, ckpt, rank);
         file_elem != NULL;
         file_elem = scr_hash_elem_next(file_elem))
    {
      /* get filename and check that we can read it */
      char* file = scr_hash_elem_key(file_elem);
      if (scr_bool_have_file(file, ckpt, rank)) {
        /* copy the file and optionally compute the crc during the copy */
        int crc_valid = 0;
        uLong crc = crc32(0L, Z_NULL, 0);
        uLong* crc_p = NULL;
        if (crc_flag) {
          crc_valid = 1;
          crc_p = &crc;
        }
        char dst[SCR_MAX_FILENAME];
        if (scr_copy_to(file, dir, buf_size, dst, crc_p) != SCR_SUCCESS) {
          crc_valid = 0;
          rc = 1;
        }

        /* read the .scr for this file */
        struct scr_meta meta;
        scr_meta_read(file, &meta);

        /* add this file to the rank_map */
        scr_filemap_add_file(rank_map, ckpt, rank, meta.filename);

        /* if file has crc32, check it against the one computed during the copy,
         * otherwise if crc_flag is set, record crc32 */
        if (crc_valid) {
          if (meta.crc32_computed) {
            if (crc != meta.crc32) { 
              /* detected a crc mismatch during the copy */

              /* TODO: unlink the copied file */
              /* unlink(dst); */

              /* mark the file as invalid */
              meta.complete = 0;
              scr_meta_write(file, &meta);

              rc = 1;
              scr_err("scr_copy: CRC32 mismatch detected when flushing file %s to %s @ %s:%d",
                      file, dst, __FILE__, __LINE__
              );

              /* TODO: would be good to log this, but right now only rank 0 can write log entries */
              /*
              if (scr_log_enable) {
                time_t now = scr_log_seconds();
                scr_log_event("CRC32 MISMATCH", my_flushed_file, NULL, &now, NULL);
              }
              */
            }
          } else {
            /* the crc was not already in the metafile, but we just computed it, so set it */
            meta.crc32_computed = 1;
            meta.crc32          = crc;
            scr_meta_write(file, &meta);
          }
        }

        /* now copy the metafile */
        char metafile[SCR_MAX_FILENAME];
        char dst_metafile[SCR_MAX_FILENAME];
        scr_meta_name(metafile, file);
        if (scr_copy_to(metafile, dir, buf_size, dst_metafile, NULL) != SCR_SUCCESS) {
          rc = 1;
        }
      } else {
        /* have_file failed, so there was some problem accessing the file */
        rc = 1;
        scr_err("scr_copy: File is unreadable or incomplete: CheckpointID %d, Rank %d, File: %s",
                ckpt, rank, file
        );
      }
    }

    /* write out the rank filemap for scr_check_complete */
    char rank_filemap_name[SCR_MAX_FILENAME];
    sprintf(rank_filemap_name, "%s/%d.scrfilemap", dir, rank);
    if (scr_filemap_write(rank_filemap_name, rank_map) != SCR_SUCCESS) {
      rc = 1;
    }

    /* delete the rank filemap object */
    scr_filemap_delete(rank_map);
  }

  /* free the strdup'd destination directory */
  if (dir != NULL) {
    free(dir);
    dir = NULL;
  }

  /* free the strdup'd control directory */
  if (cntldir != NULL) {
    free(cntldir);
    cntldir = NULL;
  }

  printf("scr_copy: %s: Return code: %d\n", hostname, rc);
  return rc;
}
