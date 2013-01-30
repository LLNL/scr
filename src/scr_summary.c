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

#include "scr_globals.h"

/*
=========================================
Summary file functions
=========================================
*/

/* read in the summary file from dir assuming file is using version 4 format or earlier,
 * convert to version 5 hash */
static int scr_summary_read_v4_to_v5(const scr_path* dir, scr_hash* summary_hash)
{
  /* check that we have a pointer to a hash */
  if (summary_hash == NULL) {
    return SCR_FAILURE;
  }

  /* build name of summary file */
  scr_path* summary_path = scr_path_dup(dir);
  scr_path_append_str(summary_path, "scr_summary.txt");
  char* summary_file = scr_path_strdup(summary_path);
  scr_path_delete(&summary_path);

  /* check whether we can read the file before we actually try,
   * we take this step to avoid printing an error in scr_hash_read */
  if (scr_file_is_readable(summary_file) != SCR_SUCCESS) {
    scr_free(&summary_file);
    return SCR_FAILURE;
  }

  /* open the summary file */
  FILE* fs = fopen(summary_file, "r");
  if (fs == NULL) {
    scr_err("Opening summary file for read: fopen(%s, \"r\") errno=%d %s @ %s:%d",
      summary_file, errno, strerror(errno), __FILE__, __LINE__
    );
    scr_free(&summary_file);
    return SCR_FAILURE;
  }

  /* assume we have one file per rank */
  int num_records = scr_ranks_world;

  /* read the first line (all versions have at least one header line) */
  int linenum = 0;
  char line[2048];
  char field[2048];
  fgets(line, sizeof(line), fs);
  linenum++;

  /* get the summary file version number, if no number, assume version=1 */
  int version = 1;
  sscanf(line, "%s", field);
  if (strcmp(field, "Version:") == 0) {
    sscanf(line, "%s %d", field, &version);
  }

  /* all versions greater than 1, have two header lines, read and throw away the second */
  if (version > 1) {
    /* version 3 and higher writes the number of rows in the file (ranks may write 0 or more files) */
    if (version >= 3) {
      fgets(line, sizeof(line), fs);
      linenum++;
      sscanf(line, "%s %d", field, &num_records);
    }
    fgets(line, sizeof(line), fs);
    linenum++;
  }

  /* now we know how many records we'll be reading, so allocate space for them */
  if (num_records <= 0) {
    scr_err("No file records found in summary file %s, perhaps it is corrupt or incomplete @ %s:%d",
      summary_file, __FILE__, __LINE__
    );
    fclose(fs);
    scr_free(&summary_file);
    return SCR_FAILURE;
  }

  /* set the version number in the summary hash, initialize a pointer to the checkpoint hash */
  scr_hash_set_kv_int(summary_hash, SCR_SUMMARY_KEY_VERSION, SCR_SUMMARY_FILE_VERSION_5);
  scr_hash* ckpt_hash = NULL;

  /* read the record for each rank */
  int i;
  int bad_values        =  0;
  int all_complete      =  1;
  int all_ranks         = -1;
  int all_checkpoint_id = -1;
  for(i=0; i < num_records; i++) {
    int expected_n, n;
    int rank, scr, ranks, pattern, complete, match_filesize, checkpoint_id;
    char filename[SCR_MAX_FILENAME];
    unsigned long exp_filesize, filesize;
    int crc_computed = 0;
    uLong crc = 0UL;

    /* read a line from the file, parse depending on version */
    if (version == 1) {
      expected_n = 10;
      n = fscanf(fs, "%d\t%d\t%d\t%d\t%d\t%d\t%lu\t%d\t%lu\t%s\n",
                 &rank, &scr, &ranks, &pattern, &checkpoint_id, &complete,
                 &exp_filesize, &match_filesize, &filesize, filename
      );
      linenum++;
    } else {
      expected_n = 11;
      n = fscanf(fs, "%d\t%d\t%d\t%d\t%d\t%lu\t%d\t%lu\t%s\t%d\t0x%lx\n",
                 &rank, &scr, &ranks, &checkpoint_id, &complete,
                 &exp_filesize, &match_filesize, &filesize, filename,
                 &crc_computed, &crc
      );
      linenum++;
    }

    /* check the return code returned from the read */
    if (n == EOF) {
      scr_err("Early EOF in summary file %s at line %d.  Only read %d of %d expected records @ %s:%d",
        summary_file, linenum, i, num_records, __FILE__, __LINE__
      );
      fclose(fs);
      scr_hash_unset_all(summary_hash);
      scr_free(&summary_file);
      return SCR_FAILURE;
    } else if (n != expected_n) {
      scr_err("Invalid read of record %d in %s at line %d @ %s:%d",
        i, summary_file, linenum, __FILE__, __LINE__
      );
      fclose(fs);
      scr_hash_unset_all(summary_hash);
      scr_free(&summary_file);
      return SCR_FAILURE;
    }

    /* TODO: check whether all files are complete, match expected size, number of ranks, checkpoint_id, etc */
    if (rank < 0 || rank >= scr_ranks_world) {
      bad_values = 1;
      scr_err("Invalid rank detected (%d) in a job with %d tasks in %s at line %d @ %s:%d",
        rank, scr_my_rank_world, summary_file, linenum, __FILE__, __LINE__
      );
    }

    /* chop to basename of filename */
    char* base = basename(filename);

    /* set the pointer to the checkpoint hash, if we haven't already */
    if (ckpt_hash == NULL) {
      /* get a pointer to the checkpoint hash */
      ckpt_hash = scr_hash_set_kv_int(summary_hash, SCR_SUMMARY_5_KEY_CKPT, checkpoint_id);
    }

    /* get a pointer to the hash for this rank, and then to the file for this rank */
    scr_hash* rank_hash = scr_hash_set_kv_int(ckpt_hash, SCR_SUMMARY_5_KEY_RANK, rank);
    scr_hash* file_hash = scr_hash_set_kv(    rank_hash, SCR_SUMMARY_5_KEY_FILE, base);

    /* set the file size, and the crc32 value if it was computed */
    scr_hash_util_set_bytecount(file_hash, SCR_SUMMARY_5_KEY_SIZE, exp_filesize);
    if (crc_computed) {
      scr_hash_util_set_crc32(file_hash, SCR_SUMMARY_5_KEY_CRC, crc);
    }

    /* if the file is incomplete, set the incomplete field for this file */
    if (! complete) {
      all_complete = 0;
      scr_hash_set_kv_int(file_hash, SCR_SUMMARY_5_KEY_COMPLETE, 0);
    }

    /* check that the checkpoint id matches all other checkpoint ids in the file */
    if (checkpoint_id != all_checkpoint_id) {
      if (all_checkpoint_id == -1) {
        all_checkpoint_id = checkpoint_id;
      } else {
        bad_values = 1;
        scr_err("Checkpoint id %d on record %d does not match expected checkpoint id %d in %s at line %d @ %s:%d",
          checkpoint_id, i, all_checkpoint_id, summary_file, linenum, __FILE__, __LINE__
        );
      }
    }

    /* check that the number of ranks matches all the number of ranks specified by all other records in the file */
    if (ranks != all_ranks) {
      if (all_ranks == -1) {
        all_ranks = ranks;
      } else {
        bad_values = 1;
        scr_err("Number of ranks %d on record %d does not match expected number of ranks %d in %s at line %d @ %s:%d",
          ranks, i, all_ranks, summary_file, linenum, __FILE__, __LINE__
        );
      }
    }
  }

  /* we've read in all of the records, now set the values for the complete field
   * and the number of ranks field */
  if (ckpt_hash != NULL) {
    scr_hash_set_kv_int(ckpt_hash, SCR_SUMMARY_5_KEY_COMPLETE, all_complete);
    scr_hash_set_kv_int(ckpt_hash, SCR_SUMMARY_5_KEY_RANKS, all_ranks);
  }

  /* close the file */
  fclose(fs);

  /* if we found any problems while reading the file, clear the hash and return with an error */
  if (bad_values) {
    /* clear the hash, since we may have set bad values */
    scr_hash_unset_all(summary_hash);
    scr_free(&summary_file);
    return SCR_FAILURE;
  }

  /* free summary file string */
  scr_free(&summary_file);

  /* otherwise, return success */
  return SCR_SUCCESS;
}

/* verify the hash is a valid hash for a version 5 summary file */
static int scr_summary_check_v5(scr_hash* hash)
{
  /* check that the summary file version is something we support */
  int version;
  if (scr_hash_util_get_int(hash, SCR_SUMMARY_KEY_VERSION, &version) != SCR_SUCCESS) {
    /* couldn't find version number */
    scr_err("Failed to read version number in summary file @ %s:%d",
      __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  if (version != SCR_SUMMARY_FILE_VERSION_5) {
    /* invalid version number */
    scr_err("Found version number %d when %d was expected in summary file @ %s:%d",
      __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* check that we have exactly one checkpoint */
  scr_hash* ckpt_hash = scr_hash_get(hash, SCR_SUMMARY_5_KEY_CKPT);
  if (scr_hash_size(ckpt_hash) != 1) {
    scr_err("More than one checkpoint found in summary file @ %s:%d",
      __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* get the first (and only) checkpoint id */
  char* ckpt_str = scr_hash_elem_get_first_val(hash, SCR_SUMMARY_5_KEY_CKPT);
  scr_hash* ckpt = scr_hash_get(ckpt_hash, ckpt_str);

  /* check that the complete string is set and is set to 1 */
  int complete;
  if (scr_hash_util_get_int(ckpt, SCR_SUMMARY_5_KEY_COMPLETE, &complete) != SCR_SUCCESS) {
    /* could not find complete value (assume it's incomplete) */
    return SCR_FAILURE;
  }
  if (complete != 1) {
    /* checkpoint is marked as incomplete */
    return SCR_FAILURE;
  }

  /* read in the the number of ranks for this checkpoint */
  int ranks;
  if (scr_hash_util_get_int(ckpt, SCR_SUMMARY_5_KEY_RANKS, &ranks) != SCR_SUCCESS) {
    scr_err("Failed to read number of ranks in summary file @ %s:%d",
      __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* check that the number of ranks matches the number we're currently running with */
  if (ranks != scr_ranks_world) {
    scr_err("Number of ranks %d that wrote checkpoint does not match current number of ranks %d @ %s:%d",
      ranks, scr_ranks_world, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  return SCR_SUCCESS;
}

/* read in the summary file from dir */
static int scr_summary_read_v5(const scr_path* dir, scr_hash* summary_hash)
{
  /* check that we got a pointer to a hash */
  if (summary_hash == NULL) {
    return SCR_FAILURE;
  }

  /* assume that we'll fail */
  int rc = SCR_FAILURE;

  /* build the summary filename */
  scr_path* summary_path = scr_path_dup(dir);
  scr_path_append_str(summary_path, "summary.scr");
  char* summary_file = scr_path_strdup(summary_path);

  /* check whether we can read the file before we actually try,
   * we take this step to avoid printing an error in scr_hash_read */
  if (scr_file_is_readable(summary_file) != SCR_SUCCESS) {
    goto cleanup;
  }

  /* read in the summary hash file */
  if (scr_hash_read_path(summary_path, summary_hash) != SCR_SUCCESS) {
    scr_err("Reading summary file %s @ %s:%d",
      summary_file, __FILE__, __LINE__
    );
    goto cleanup;
  }

  /* if we made it here, we successfully read the summary file as a hash */
  rc = SCR_SUCCESS;

cleanup:
  /* free the summary path */
  scr_free(&summary_file);
  scr_path_delete(&summary_path);

  return rc;
}

/* read in the summary file from dir */
static int scr_summary_read_v6(const scr_path* dir, scr_hash* summary_hash)
{
  /* check that we got a pointer to a hash */
  if (summary_hash == NULL) {
    return SCR_FAILURE;
  }

  /* assume that we'll fail */
  int rc = SCR_FAILURE;

  /* build the summary filename */
  scr_path* summary_path = scr_path_dup(dir);
  scr_path_append_str(summary_path, ".scr");
  scr_path_append_str(summary_path, "summary.scr");
  char* summary_file = scr_path_strdup(summary_path);

  /* check whether we can read the file before we actually try,
   * we take this step to avoid printing an error in scr_hash_read */
  if (scr_file_is_readable(summary_file) != SCR_SUCCESS) {
    goto cleanup;
  }

  /* read in the summary hash file */
  if (scr_hash_read(summary_file, summary_hash) != SCR_SUCCESS) {
    scr_err("Reading summary file %s @ %s:%d",
      summary_file, __FILE__, __LINE__
    );
    goto cleanup;
  }

  /* read the version from the summary hash */
  int version;
  if (scr_hash_util_get_int(summary_hash, SCR_SUMMARY_KEY_VERSION, &version) != SCR_SUCCESS) {
    scr_err("Failed to read version from summary file %s @ %s:%d",
      summary_file, __FILE__, __LINE__
    );
    goto cleanup;
  }

  /* check that the version number matches */
  if (version != SCR_SUMMARY_FILE_VERSION_6) {
    scr_err("Summary file %s is version %d instead of version %d @ %s:%d",
      summary_file, version, SCR_SUMMARY_FILE_VERSION_6, __FILE__, __LINE__
    );
    goto cleanup;
  }

  /* if we made it here, we successfully read the summary file as a hash */
  rc = SCR_SUCCESS;

cleanup:
  /* free the summary file string */
  scr_free(&summary_file);
  scr_path_delete(&summary_path);

  return rc;
}

static int scr_summary_convert_v5_to_v6(scr_hash* old, scr_hash* new)
{
  /* TODO: convert into a version 6 hash */

  return SCR_SUCCESS;
}

/* read in the summary file from dir */
int scr_summary_read(const scr_path* dir, scr_hash* summary_hash)
{
  /* check that we have pointers to a hash */
  if (summary_hash == NULL) {
    return SCR_FAILURE;
  }

  /* clear the hash */
  scr_hash_unset_all(summary_hash);

  /* attempt to read the summary file, assuming it is in version 5 format */
  if (scr_summary_read_v6(dir, summary_hash) != SCR_SUCCESS) {
    /* failed to read file as version 6 format, try to get a version 5 hash */
    scr_hash* summary_hash_v5 = scr_hash_new();
    if (scr_summary_read_v5(dir, summary_hash_v5) != SCR_SUCCESS) {
      /* failed to read the summary file, try again, but now assume an older format */
      if (scr_summary_read_v4_to_v5(dir, summary_hash_v5) != SCR_SUCCESS) {
        /* we still failed, don't report an error here, just return failure,
         * the read functions will report errors as needed */
        scr_err("Reading summary file in %s @ %s:%d",
          dir, __FILE__, __LINE__
        );
        scr_hash_delete(&summary_hash_v5);
        return SCR_FAILURE;
      }
    }

    /* check that the hash looks like version 5 summary file */
    if (scr_summary_check_v5(summary_hash_v5) != SCR_SUCCESS) {
      /* hash failed version 5 check */
      scr_err("Invalid version 5 summary file in %s @ %s:%d",
        dir, __FILE__, __LINE__
      );
      scr_hash_delete(&summary_hash_v5);
      return SCR_FAILURE;
    }

    /* convert version 5 summary file hash into version 6 hash */
    if (scr_summary_convert_v5_to_v6(summary_hash_v5, summary_hash) != SCR_SUCCESS) {
      /* failed to convert version 5 hash into version 6 format */
      scr_err("Invalid version 5 summary file in %s @ %s:%d",
        dir, __FILE__, __LINE__
      );
      scr_hash_delete(&summary_hash_v5);
      return SCR_FAILURE;
    }
    scr_hash_delete(&summary_hash_v5);
  }

  /* TODO: check that hash looks like a version 6 hash */

  return SCR_SUCCESS;
}

/* write out the summary file to dir */
int scr_summary_write(const scr_path* dir, const scr_dataset* dataset, int all_complete, scr_hash* data)
{
  /* build the summary filename */
  scr_path* summary_path = scr_path_dup(dir);
  scr_path_append_str(summary_path, ".scr");
  scr_path_append_str(summary_path, "summary.scr");

  /* create an empty hash to build our summary info */
  scr_hash* summary_hash = scr_hash_new();

  /* write the summary file version number */
  scr_hash_util_set_int(summary_hash, SCR_SUMMARY_KEY_VERSION, SCR_SUMMARY_FILE_VERSION_6);

  /* mark whether the flush is complete in the summary file */
  scr_hash_util_set_int(summary_hash, SCR_SUMMARY_6_KEY_COMPLETE, all_complete);

  /* write the dataset descriptor */
  scr_hash* dataset_hash = scr_hash_new();
  scr_hash_merge(dataset_hash, dataset);
  scr_hash_set(summary_hash, SCR_SUMMARY_6_KEY_DATASET, dataset_hash);

  /* for each file, insert hash listing filename, then file size, crc,
   * and incomplete flag under that */
  scr_hash_merge(summary_hash, data);

  /* write the number of ranks used to write this dataset */
  scr_hash* rank2file_hash = scr_hash_get(summary_hash, SCR_SUMMARY_6_KEY_RANK2FILE);
  scr_hash_util_set_int(rank2file_hash, SCR_SUMMARY_6_KEY_RANKS, scr_ranks_world);

  /* write the hash to a file */
  scr_hash_write_path(summary_path, summary_hash);

  /* free the hash object */
  scr_hash_delete(&summary_hash);

  /* free the file name string */
  scr_path_delete(&summary_path);

  return SCR_SUCCESS;
}
