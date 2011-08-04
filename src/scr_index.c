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

/*
 * The scr_index command updates the index.scr file to account for new
 * output set directories.
*/

/*
setenv TV /usr/global/tools/totalview/v/totalview.8X.8.0-4/bin/totalview
mpigcc -g -O0 -o scr_index_cmd scr_index_cmd.c scr_hash.o scr_halt_cntl-scr_err_serial.o scr_io.o scr_index.o scr_meta.o scr_filemap.o tv_data_display.o -lz
mpigcc -g -O0 -o scr_index_cmd scr_index_cmd.c scr_hash.o scr_halt_cntl-scr_err_serial.o scr_io.o scr_index.o scr_meta.o scr_filemap.o tv_data_display.o -lz /usr/global/tools/totalview/v/totalview.8X.8.0-4/linux-x86-64/lib/libdbfork_64.a
cd /g/g0/moody20/packages/scr/index_test
$TV ../src/scr_index_cmd -a `pwd` scr.2010-06-29_17:22:08.1018033.10 &
*/

#define _GNU_SOURCE

/* read in the config.h to pick up any parameters set from configure 
 * these values will override any settings below */
#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "scr_conf.h"
#include "scr.h"
#include "scr_io.h"
#include "scr_err.h"
#include "scr_util.h"
#include "scr_hash.h"
#include "scr_meta.h"
#include "scr_filemap.h"
#include "scr_param.h"
#include "scr_index_api.h"

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
#include <sys/wait.h>
#include <getopt.h>

#include <dirent.h>

#include <regex.h>

#define SCR_IO_KEY_DIR     ("DIR")
#define SCR_IO_KEY_FILE    ("FILE")
#define SCR_IO_KEY_UNKNOWN ("UNKNOWN")

#define SCR_SUMMARY_FILENAME "summary.scr"
#define BUILD_CMD (X_BINDIR"/scr_rebuild_xor")

/* Hash format returned from scr_read_dir
 * 
 * DIR
 *   <dir1>
 *   <dir2>
 * FILE
 *   <file1>
 *   <file2>
*/

/* Hash format returned from scr_scan_files
 *  - files are only added under RANK if they
 *    pass all checks
 *
 * DLIST
 *   <dataset_id>
 *     RANK2FILE
 *       RANKS
 *         <num_ranks>
 *       RANK
 *         <rank>
 *           FILE
 *             <filename>
 *               SIZE
 *                 <filesize>
 *               CRC
 *                 <crc>
 *     XOR
 *       <xor_setid>
 *         MEMBERS
 *           <num_members_in_set>
 *         MEMBER
 *           <member_number>
 *             FILE
 *               <xor_file_name>
 *             RANK
 *               <rank>
*/

/* Hash format returned from scr_inspect_files
 *  - files are only added under RANK if they
 *    pass all checks
 *
 * CKPT
 *   <checkpoint_id>
 *     INVALID
 *     MISSING
 *       <rank1>
 *     RANKS
 *       <num_ranks>
 *     RANK
 *       <rank>
 *         FILES
 *           <num_files_to_expect>
 *         FILE
 *           <file_name>
 *             SIZE
 *               <size_in_bytes>
 *             CRC
 *               <crc32_string_in_0x_form>
 *     XOR
 *       <xor_setid>
 *         MEMBERS
 *           <num_members_in_set>
 *         MEMBER
 *           <member_number>
 *             FILE
 *               <xor_file_name>
 *             RANK
 *               <rank>
*/

#define SCR_SCAN_KEY_XOR      ("XOR")
#define SCR_SCAN_KEY_MEMBER   ("MEMBER")
#define SCR_SCAN_KEY_MEMBERS  ("MEMBERS")

#define SCR_SCAN_KEY_DLIST    ("DLIST")
#define SCR_SCAN_KEY_MISSING  ("MISSING")
#define SCR_SCAN_KEY_INVALID  ("INVALID")
#define SCR_SCAN_KEY_UNRECOVERABLE ("UNRECOVERABLE")
#define SCR_SCAN_KEY_BUILD    ("BUILD")

/* read the file and directory names from dir and return in hash */
int scr_read_dir (const char* dir, scr_hash* hash)
{
  int rc = SCR_SUCCESS;

  DIR* dirp = NULL;
  struct dirent* dp = NULL;

  /* open the directory */
  dirp = opendir(dir);
  if (dirp == NULL) {
    scr_err("Failed to open directory %s (errno=%d %m) @ %s:%d",
            dir, errno, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  } 

  /* read each file from the directory */
  do {
    errno = 0;
    dp = readdir(dirp);
    if (dp != NULL) {
      #ifdef _DIRENT_HAVE_D_TYPE
        /* distinguish between directories and files if we can */
        if (dp->d_type == DT_DIR) {
          scr_hash_set_kv(hash, SCR_IO_KEY_DIR, dp->d_name);
        } else {
          scr_hash_set_kv(hash, SCR_IO_KEY_FILE, dp->d_name);
        }
      #else
        /* TODO: throw a compile error here instead? */
        scr_hash_set_kv(hash, SCR_IO_KEY_FILE, dp->d_name);
      #endif
    } else {
      if (errno != 0) {
        scr_err("Failed to read directory %s (errno=%d %m) @ %s:%d",
                dir, errno, __FILE__, __LINE__
        );
        rc = SCR_FAILURE;
      }
    }
  } while (dp != NULL);

  /* close the directory */
  if (closedir(dirp) < 0) {
    scr_err("Failed to close directory %s (errno=%d %m) @ %s:%d",
            dir, errno, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  return rc;
}

int scr_summary_read(const char* dir, scr_hash* hash)
{
  /* build the filename for the summary file */
  char summary_file[SCR_MAX_FILENAME];
  if (scr_build_path(summary_file, sizeof(summary_file), dir, SCR_SUMMARY_FILENAME) != SCR_SUCCESS) {
    scr_err("Failed to build full filename for summary file for directory %s @ %s:%d",
            dir, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* check whether the file exists before we attempt to read it
   * (do this error to avoid printing an error in scr_hash_read) */
  if (scr_file_exists(summary_file) != SCR_SUCCESS) {
    return SCR_FAILURE;
  }

  /* now attempt to read the file contents into the hash */
  if (scr_hash_read(summary_file, hash) != SCR_SUCCESS) {
    return SCR_FAILURE;
  }

  return SCR_SUCCESS;
}

int scr_summary_write(const char* dir, scr_hash* hash)
{
  /* build the filename for the summary file */
  char summary_file[SCR_MAX_FILENAME];
  if (scr_build_path(summary_file, sizeof(summary_file), dir, SCR_SUMMARY_FILENAME) != SCR_SUCCESS) {
    scr_err("Failed to build full filename for summary file for directory %s @ %s:%d",
            dir, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* now write the hash to the file */
  if (scr_hash_write(summary_file, hash) != SCR_SUCCESS) {
    return SCR_FAILURE;
  }

  return SCR_SUCCESS;
}

/* forks and execs processes to rebuild missing files and waits for them to complete,
 * returns SCR_FAILURE if any dataset failed to rebuild, SCR_SUCCESS otherwise */
int scr_fork_rebuilds(const char* dir, scr_hash* cmds)
{
  int rc = SCR_SUCCESS;

  /* count the number of build commands */
  int builds = scr_hash_size(cmds);

  /* allocate space to hold the pid for each child */
  pid_t* pids = NULL;
  if (builds > 0) {
    pids = (pid_t*) malloc(builds * sizeof(pid_t));
    if (pids == NULL) {
      scr_err("Failed to allocate space to record pids @ %s:%d",
              __FILE__, __LINE__
      );
      return SCR_FAILURE;
    }
  }

  /* TODO: flow control the number of builds ongoing at a time */

  /* step through and fork off each of our build commands */
  int pid_count = 0;
  scr_hash_elem* elem = NULL;
  for (elem = scr_hash_elem_first(cmds);
       elem != NULL;
       elem = scr_hash_elem_next(elem))
  {
    /* get the hash of argv values for this command */
    scr_hash* cmd_hash = scr_hash_elem_hash(elem);

    /* sort the arguments by their index */
    scr_hash_sort_int(cmd_hash, SCR_HASH_SORT_ASCENDING);

    /* print the command to screen, so the user knows what's happening */
    int offset = 0;
    char full_cmd[SCR_MAX_FILENAME];
    scr_hash_elem* arg_elem = NULL;
    for (arg_elem = scr_hash_elem_first(cmd_hash);
         arg_elem != NULL;
         arg_elem = scr_hash_elem_next(arg_elem))
    {
      char* key = scr_hash_elem_key(arg_elem);
      char* arg_str = scr_hash_elem_get_first_val(cmd_hash, key);
      int remaining = sizeof(full_cmd) - offset;
      if (remaining > 0) {
        offset += snprintf(full_cmd + offset, remaining, "%s ", arg_str);
      }
    }
    scr_dbg(0, "Rebuild command: %s\n", full_cmd);

    /* count the number of command line arguments */
    int argc = scr_hash_size(cmd_hash);

    /* issue build command */
    pids[pid_count] = fork();
    if (pids[pid_count] == 0) {
      /* this is the child, which will do the exec, build the argv array */

      /* allocate space for the argv array */
      char** argv = (char**) malloc((argc + 1) * sizeof(char*));
      if (argv == NULL) {
        scr_err("Failed to allocate memory for build execv @ %s:%d",
                __FILE__, __LINE__
        );
        exit(1);
      }

      /* fill in our argv values and null-terminate the array */
      int index = 0;
      scr_hash_elem* arg_elem = NULL;
      for (arg_elem = scr_hash_elem_first(cmd_hash);
           arg_elem != NULL;
           arg_elem = scr_hash_elem_next(arg_elem))
      {
        char* key = scr_hash_elem_key(arg_elem);
        argv[index] = scr_hash_elem_get_first_val(cmd_hash, key);
        index++;
      }
      argv[index] = NULL;

      /* cd to current working directory */
      if (chdir(dir) != 0) {
        scr_err("Failed to change to directory %s @ %s:%d",
                dir, __FILE__, __LINE__
        );
        exit(1);
      }

      /* execv the build command */
      execv(BUILD_CMD, argv);
    }
    pid_count++;
  }

  /* wait for for each child to finish */
  while (pid_count > 0) {
    int stat = 0;
    int ret = wait(&stat);
    if (ret == -1 || ret == (pid_t)-1) {
      scr_err("Got a -1 from wait @ %s:%d",
              __FILE__, __LINE__
      );
      rc = SCR_FAILURE;
    } else if (stat != 0) {
      scr_err("Child returned with non-zero @ %s:%d",
              __FILE__, __LINE__
      );
      rc = SCR_FAILURE;
    }
    pid_count--;
  }

  /* free the pid array */
  if (pids != NULL) {
    free(pids);
    pids = NULL;
  }

  return rc;
}

/* returns SCR_FAILURE if any dataset failed to rebuild, SCR_SUCCESS otherwise */
int scr_rebuild_scan(const char* dir, scr_hash* scan)
{
  /* assume we'll be successful */
  int rc = SCR_SUCCESS;

  /* step through and check each of our datasets */
  scr_hash_elem* dset_elem = NULL;
  scr_hash* dsets_hash = scr_hash_get(scan, SCR_SCAN_KEY_DLIST);
  for (dset_elem = scr_hash_elem_first(dsets_hash);
       dset_elem != NULL;
       dset_elem = scr_hash_elem_next(dset_elem))
  {
    /* get id and the hash for this dataset */
    int dset_id = scr_hash_elem_key_int(dset_elem);
    scr_hash* dset_hash = scr_hash_elem_hash(dset_elem);

    /* if the dataset is marked as inconsistent -- consider it to be beyond repair */
    scr_hash* invalid = scr_hash_get(dset_hash, SCR_SCAN_KEY_INVALID);
    if (invalid != NULL) {
      rc = SCR_FAILURE;
      continue;
    }

    /* check whether there are any missing files in this dataset */
    scr_hash* missing_hash = scr_hash_get(dset_hash, SCR_SCAN_KEY_MISSING);
    if (missing_hash != NULL) {
      /* at least one rank is missing files, attempt to rebuild them */
      int build_command_count = 0;

      /* step through each of our xor sets */
      scr_hash_elem* xor_elem = NULL;
      scr_hash* xors_hash = scr_hash_get(dset_hash, SCR_SCAN_KEY_XOR);
      for (xor_elem = scr_hash_elem_first(xors_hash);
           xor_elem != NULL;
           xor_elem = scr_hash_elem_next(xor_elem))
      {
        /* get the set id and the hash for this xor set */
        int xor_setid = scr_hash_elem_key_int(xor_elem);
        scr_hash* xor_hash = scr_hash_elem_hash(xor_elem);

        /* TODO: Check that there is only one members value */

        /* get the number of members in this set */
        char* members_str = scr_hash_elem_get_first_val(xor_hash, SCR_SCAN_KEY_MEMBERS);
        if (members_str == NULL) {
          /* unknown number of members in this set, skip this set */
          scr_err("Unknown number of members in XOR set %d in dataset %d @ %s:%d",
                  xor_setid, dset_id, __FILE__, __LINE__
          );
          rc = SCR_FAILURE;
          continue;
        }
        int members = atoi(members_str);

        /* if we don't have all members, add rebuild command if we can */
        scr_hash* members_hash = scr_hash_get(xor_hash, SCR_SCAN_KEY_MEMBER);
        int members_have = scr_hash_size(members_hash);
        if (members_have < members - 1) {
          /* not enough members to attempt rebuild of this set, skip it */
          /* TODO: most likely this means that a rank can't be recovered */
          rc = SCR_FAILURE;
          continue;
        }

        /* attempt a rebuild if either:
         *   a member is missing (likely lost all files for that rank)
         *   or if we have all members but one of the corresponding ranks
         *     is missing files (got the XOR file, but missing the full file) */
        int missing_count = 0;
        int missing_member = -1;
        int member;
        for (member = 1; member <= members; member++) {
          scr_hash* member_hash = scr_hash_get_kv_int(xor_hash, SCR_SCAN_KEY_MEMBER, member);
          if (member_hash == NULL) {
            /* we're missing the XOR file for this member */
            missing_member = member;
            missing_count++;
          } else {
            /* get the rank this member corresponds to */
            char* rank_str = scr_hash_elem_get_first_val(member_hash, SCR_SUMMARY_6_KEY_RANK);
            if (rank_str != NULL) {
              /* check whether we're missing any files for this rank */
              scr_hash* missing_rank_hash = scr_hash_get(missing_hash, rank_str);
              if (missing_rank_hash != NULL) {
                /* we have the XOR file for this member, but we're missing one or more regular files */
                missing_member = member;
                missing_count++;
              }
            } else {
              /* couldn't identify rank for this member, print an error */
              scr_err("Could not identify rank corresponding to member %d of XOR set %d in dataset %d @ %s:%d",
                      member, xor_setid, dset_id, __FILE__, __LINE__
              );
              rc = SCR_FAILURE;
            }
          }
        }

        if (missing_count > 1) {
          /* TODO: unrecoverable */
          scr_hash_set_kv_int(dset_hash, SCR_SCAN_KEY_UNRECOVERABLE, xor_setid);
        } else if (missing_count > 0) {
          scr_hash* buildcmd_hash = scr_hash_set_kv_int(dset_hash, SCR_SCAN_KEY_BUILD, build_command_count);
          build_command_count++;

          int argc = 0;

          /* write the command name */
          scr_hash_setf(buildcmd_hash, NULL, "%d %s", argc, BUILD_CMD);
          argc++;

          /* write the number of members in the xor set */
          scr_hash_setf(buildcmd_hash, NULL, "%d %s", argc, members_str);
          argc++;

          /* write the index of the missing xor file (convert from 1-based index to 0-based index) */
          scr_hash_setf(buildcmd_hash, NULL, "%d %d", argc, missing_member - 1);
          argc++;

          /* build the name of the missing xor file */
          char missing_filename[SCR_MAX_FILENAME];
          snprintf(missing_filename, sizeof(missing_filename), "%d_of_%d_in_%d.xor", missing_member, members, xor_setid);
          scr_hash_setf(buildcmd_hash, NULL, "%d %s", argc, missing_filename);
          argc++;

          /* write each of the existing xor file names, skipping the missing member */
          for (member = 1; member <= members; member++) {
            if (member == missing_member) {
              continue;
            }
            scr_hash* member_hash = scr_hash_get_kv_int(xor_hash, SCR_SCAN_KEY_MEMBER, member);
            char* filename = scr_hash_elem_get_first_val(member_hash, SCR_SUMMARY_6_KEY_FILE);
            scr_hash_setf(buildcmd_hash, NULL, "%d %s", argc, filename);
            argc++;
          }
        }
      }

      /* rebuild if we can */
      scr_hash* unrecoverable = scr_hash_get(dset_hash, SCR_SCAN_KEY_UNRECOVERABLE);
      if (unrecoverable != NULL) {
        /* at least some files cannot be recovered */
        scr_err("Insufficient files to attempt rebuild of dataset %d in %s @ %s:%d",
                dset_id, dir, __FILE__, __LINE__
        );
        rc = SCR_FAILURE;
      } else {
        /* we have a shot to rebuild everything, let's give it a go */
        scr_hash* builds_hash = scr_hash_get(dset_hash, SCR_SCAN_KEY_BUILD);
        if (scr_fork_rebuilds(dir, builds_hash) != SCR_SUCCESS) {
          scr_err("At least one rebuild failed for dataset %d in %s @ %s:%d",
                  dset_id, dir, __FILE__, __LINE__
          );
          rc = SCR_FAILURE;
        }
      }
    }
  }

  return rc;
}

/* cheks scan hash for any missing files,
 * returns SCR_FAILURE if any dataset is missing any files
 * or if any dataset is marked as inconsistent,
 * SCR_SUCCESS otherwise */
int scr_inspect_scan(scr_hash* scan)
{
  /* assume nothing is missing, we'll set this to 1 if we find anything that is */
  int any_missing = 0;

  /* look for missing files for each dataset */
  scr_hash_elem* dset_elem = NULL;
  scr_hash* dsets = scr_hash_get(scan, SCR_SCAN_KEY_DLIST);
  for (dset_elem = scr_hash_elem_first(dsets);
       dset_elem != NULL;
       dset_elem = scr_hash_elem_next(dset_elem))
  {
    /* get the dataset id */
    int dset_id = scr_hash_elem_key_int(dset_elem);

    /* get the dataset hash */
    scr_hash* dset_hash = scr_hash_elem_hash(dset_elem);

    /* get the hash for the RANKS key */
    scr_hash* rank2file_hash   = scr_hash_get(dset_hash, SCR_SUMMARY_6_KEY_RANK2FILE);
    scr_hash* ranks_count_hash = scr_hash_get(rank2file_hash, SCR_SUMMARY_6_KEY_RANKS);

    /* check that this dataset has only one value under the RANKS key */
    int ranks_size = scr_hash_size(ranks_count_hash);
    if (ranks_size != 1) {
      /* found more than one RANKS value, mark it as inconsistent */
      any_missing = 1;
      scr_hash_set_kv_int(dset_hash, SCR_SCAN_KEY_INVALID, 1);
      scr_err("Dataset %d has more than one value for the number of ranks @ %s:%d",
              dset_id, __FILE__, __LINE__
      );
      continue;
    }

    /* lookup the number of ranks */
    char* ranks_str = scr_hash_elem_get_first_val(rank2file_hash, SCR_SUMMARY_6_KEY_RANKS);
    int ranks = atoi(ranks_str);

    /* assume this dataset is valid */
    int dataset_valid = 1;

    /* get the ranks hash and sort it by rank id */
    scr_hash* ranks_hash = scr_hash_get(rank2file_hash, SCR_SUMMARY_6_KEY_RANK);
    scr_hash_sort_int(ranks_hash, SCR_HASH_SORT_ASCENDING);

    /* for each rank, check that we have each of its files */
    int expected_rank = 0;
    scr_hash_elem* rank_elem = NULL;
    for (rank_elem = scr_hash_elem_first(ranks_hash);
         rank_elem != NULL;
         rank_elem = scr_hash_elem_next(rank_elem))
    {
      /* get the rank */
      int rank_id = scr_hash_elem_key_int(rank_elem);

      /* get the hash for this rank */
      scr_hash* rank_hash = scr_hash_elem_hash(rank_elem);

      /* check that the rank is in order */
      if (rank_id < expected_rank) {
        /* found a rank out of order, mark the dataset as incomplete */
        dataset_valid = 0;
        scr_err("Internal error: Rank out of order %d expected %d in dataset %d @ %s:%d",
                rank_id, expected_rank, dset_id, __FILE__, __LINE__
        );
      }

      /* check that rank is in range */
      if (rank_id >= ranks) {
        /* found a rank out of range, mark the dataset as incomplete */
        dataset_valid = 0;
        scr_err("Rank %d out of range, expected at most %d ranks in dataset %d @ %s:%d",
                rank_id, ranks, dset_id, __FILE__, __LINE__
        );
      }

      /* if rank_id is higher than expected rank, mark the expected rank as missing */
      while (expected_rank < rank_id) {
        scr_hash_set_kv_int(dset_hash, SCR_SCAN_KEY_MISSING, expected_rank);
        expected_rank++;
      }

      /* get the hash for the FILES key */
      scr_hash* files_count_hash = scr_hash_get(rank_hash, SCR_SUMMARY_6_KEY_FILES);

      /* check that this dataset has only one value for the FILES key */
      int files_size = scr_hash_size(files_count_hash);
      if (files_size != 1) {
        /* found more than one FILES value for this rank, mark it as incomplete */
        dataset_valid = 0;
        scr_err("Rank %d of dataset %d has more than one value for the number of files @ %s:%d",
                rank_id, dset_id, __FILE__, __LINE__
        );

        /* advance our expected rank id and skip to the next rank */
        expected_rank++;
        continue;
      }

      /* lookup the number of files */
      char* files_str = scr_hash_elem_get_first_val(rank_hash, SCR_SUMMARY_6_KEY_FILES);
      int files = atoi(files_str);

      /* get the files hash for this rank */
      scr_hash* files_hash = scr_hash_get(rank_hash, SCR_SUMMARY_6_KEY_FILE);

      /* check that each file is marked as complete */
      int file_count = 0;
      scr_hash_elem* file_elem = NULL;
      for (file_elem = scr_hash_elem_first(files_hash);
           file_elem != NULL;
           file_elem = scr_hash_elem_next(file_elem))
      {
        /* get the file hash */
        scr_hash* file_hash = scr_hash_elem_hash(file_elem);

        /* check that the file is not marked as incomplete */
        scr_hash* complete_hash = scr_hash_get(file_hash, SCR_SUMMARY_6_KEY_COMPLETE);
        if (complete_hash != NULL) {
          /* the complete key is set, check its value */
          char* complete_str = scr_hash_elem_get_first_val(file_hash, SCR_SUMMARY_6_KEY_COMPLETE);
          if (complete_str != NULL) {
            int complete = atoi(complete_str);
            if (complete == 0) {
              /* file is explicitly marked as incomplete, add the rank to the missing list */
              scr_hash_set_kv_int(dset_hash, SCR_SCAN_KEY_MISSING, rank_id);
            }
          }
        }

        file_count++;
      }

      /* if we're missing any files, mark this rank as missing */
      if (file_count < files) {
        scr_hash_set_kv_int(dset_hash, SCR_SCAN_KEY_MISSING, rank_id);
      }

      /* if we found more files than expected, mark the dataset as incomplete */
      if (file_count > files) {
        dataset_valid = 0;
        scr_err("Rank %d in dataset %d has more files than expected @ %s:%d",
                rank_id, dset_id, __FILE__, __LINE__
        );
      }

      /* advance our expected rank id */
      expected_rank++;
    }

    /* check that we found all of the ranks */
    while (expected_rank < ranks) {
      /* mark the expected rank as missing */
      scr_hash_set_kv_int(dset_hash, SCR_SCAN_KEY_MISSING, expected_rank);
      expected_rank++;
    }

    /* check that the total number of ranks matches what we expect */
    if (expected_rank > ranks) {
      /* more ranks than expected, mark the dataset as incomplete */
      dataset_valid = 0;
      scr_err("Dataset %d has more ranks than expected @ %s:%d",
              dset_id, __FILE__, __LINE__
      );
    }

    /* mark the dataset as invalid if needed */
    if (! dataset_valid) {
      any_missing = 1;
      scr_hash_setf(dset_hash, NULL, "%s", SCR_SCAN_KEY_INVALID);
    }

    /* check whether we have any missing files for this dataset */
    scr_hash* missing_hash = scr_hash_get(dset_hash, SCR_SCAN_KEY_MISSING);
    if (missing_hash != NULL) {
      any_missing = 1;
    }

    /* if dataset is not marked invalid, and if there are no missing files, then mark it as complete */
    if (dataset_valid && missing_hash == NULL) {
      scr_hash_set_kv_int(dset_hash, SCR_SUMMARY_6_KEY_COMPLETE, 1);
    }
  }

  if (any_missing) {
    return SCR_FAILURE;
  }
  return SCR_SUCCESS;
}

/* reads files from given checkpoint directory and adds them to scan hash.
 * Returns SCR_SUCCESS if the files could be scanned */
int scr_scan_files(const char* dir, scr_hash* scan)
{
  /* create an empty hash to hold the file names */
  scr_hash* contents = scr_hash_new();

  /* read the contents of the directory */
  if (scr_read_dir(dir, contents) != SCR_SUCCESS) {
    scr_err("Failed to read directory %s @ %s:%d",
            dir, __FILE__, __LINE__
    );
    scr_hash_delete(contents);
    return SCR_FAILURE;
  }

  /* set up a regular expression so we can extract the xor set information from a file */
  /* TODO: move this info to the meta data */
  regex_t re_xor_file;
  regcomp(&re_xor_file, "([0-9]+)_of_([0-9]+)_in_([0-9]+).xor", REG_EXTENDED);

  int ranks = -1;
  scr_hash_elem* elem = NULL;
  scr_hash* files = scr_hash_get(contents, SCR_IO_KEY_FILE);

  /* read scrfilemap files first to know how many files to expect for each rank */
  for (elem = scr_hash_elem_first(files);
       elem != NULL;
       elem = scr_hash_elem_next(elem))
  {
    /* get the file name */
    char* name = scr_hash_elem_key(elem);

    /* skip this file unless it has the .scrfilemap extension */
    int len = strlen(name);
    char* ext = name + len - 11;
    if (strcmp(ext, ".scrfilemap") != 0) {
      continue;
    }

    /* build the full file name */
    char name_tmp[SCR_MAX_FILENAME];
    if (scr_build_path(name_tmp, sizeof(name_tmp), dir, name) != SCR_SUCCESS) {
      scr_err("Filename too long to copy into internal buffer: %s/%s @ %s:%d",
              dir, name, __FILE__, __LINE__
      );
      continue;
    }

    /* create an empty filemap to store contents */
    scr_filemap* rank_map = scr_filemap_new();

    /* read in the filemap */
    if (scr_filemap_read(name_tmp, rank_map) != SCR_SUCCESS) {
      scr_err("Error reading filemap: %s @ %s:%d",
              name_tmp, __FILE__, __LINE__
      );
      scr_filemap_delete(rank_map);
      continue;
    }

    /* lookup the number of expected files for each rank */
    scr_hash_elem* dset_elem = NULL;
    for (dset_elem = scr_filemap_first_dataset(rank_map);
         dset_elem != NULL;
         dset_elem = scr_hash_elem_next(dset_elem))
    {
      /* get the dataset id */
      int dset_id = scr_hash_elem_key_int(dset_elem);

      /* lookup scan hash for this dataset id */
      scr_hash* list_hash = scr_hash_set_kv_int(scan, SCR_SCAN_KEY_DLIST, dset_id);

      /* lookup rank2file hash for this dataset, allocate a new one if it's not found */
      scr_hash* rank2file_hash = scr_hash_get(list_hash, SCR_SUMMARY_6_KEY_RANK2FILE);
      if (rank2file_hash == NULL) {
        /* there is no existing rank2file hash, create a new one and add it */
        rank2file_hash = scr_hash_new();
        scr_hash_set(list_hash, SCR_SUMMARY_6_KEY_RANK2FILE, rank2file_hash);
      }

      /* for each rank that we have for this dataset,
       * set dataset descriptor and the expected number of files */
      scr_hash_elem* rank_elem = NULL;
      for (rank_elem = scr_filemap_first_rank_by_dataset(rank_map, dset_id);
           rank_elem != NULL;
           rank_elem = scr_hash_elem_next(rank_elem))
      {
        /* get the rank number */
        int rank_id = scr_hash_elem_key_int(rank_elem);

        /* read dataset hash from filemap and record in summary */
        scr_dataset* rank_dset = scr_dataset_new();
        scr_filemap_get_dataset(rank_map, dset_id, rank_id, rank_dset);
        scr_dataset* current_dset = scr_hash_get(list_hash, SCR_SUMMARY_6_KEY_DATASET);
        if (current_dset == NULL ) {
          /* there is no dataset hash currently assigned, so use the one for the current rank */
          scr_hash_set(list_hash, SCR_SUMMARY_6_KEY_DATASET, rank_dset);
        } else {
          /* TODODSET */
          /* check that the dataset for this rank matches the one we already have */
          /* if rank_dset != current_dset, then problem */
          scr_dataset_delete(rank_dset);
        }

        /* lookup rank hash for this rank */
        scr_hash* rank_hash = scr_hash_set_kv_int(rank2file_hash, SCR_SUMMARY_6_KEY_RANK, rank_id);

        /* set number of expected files for this rank */
        int num_expect = scr_filemap_get_expected_files(rank_map, dset_id, rank_id);
        scr_hash_set_kv_int(rank_hash, SCR_SUMMARY_6_KEY_FILES, num_expect);

        /* TODO: check that we have each named file for this rank */
        scr_hash_elem* file_elem = NULL;
        for (file_elem = scr_filemap_first_file(rank_map, dset_id, rank_id);
             file_elem != NULL;
             file_elem = scr_hash_elem_next(file_elem))
        {
          /* get the file name */
          char* file_name = scr_hash_elem_key(file_elem);

          /* build the full file name */
          char name_tmp[SCR_MAX_FILENAME];
          if (scr_build_path(name_tmp, sizeof(name_tmp), dir, file_name) != SCR_SUCCESS) {
            scr_err("Filename too long to copy into internal buffer: %s/%s @ %s:%d",
                    dir, file_name, __FILE__, __LINE__
            );
            continue;
          }

          /* get meta data for this file */
          scr_meta* meta = scr_meta_new();
          if (scr_filemap_get_meta(rank_map, dset_id, rank_id, file_name, meta) != SCR_SUCCESS) {
            scr_err("Failed to read meta data for %s from dataset %d @ %s:%d",
                    file_name, dset_id, __FILE__, __LINE__
            );
            continue;
          }

          /* only check files ending with .scr and skip the summary.scr file
           *   check that file is complete
           *   check that file exists
           *   check that file size matches
           *   check that ranks agree
           *   check that checkpoint id agrees */

#if 0
          /* read the rank from the meta data */
          int meta_rank = -1;
          if (scr_meta_get_rank(meta, &meta_rank) != SCR_SUCCESS) {
            scr_err("Reading rank from meta data from %s @ %s:%d",
                    name_tmp, __FILE__, __LINE__
            );
            scr_meta_delete(meta);
            continue;
          }
#endif

          /* read the ranks from the meta data */
          int meta_ranks = -1;
          if (scr_meta_get_ranks(meta, &meta_ranks) != SCR_SUCCESS) {
            scr_err("Reading ranks from meta data from %s @ %s:%d",
                    name_tmp, __FILE__, __LINE__
            );
            scr_meta_delete(meta);
            continue;
          }

          /* read filename from meta data */
          char* meta_filename = NULL;
          if (scr_meta_get_filename(meta, &meta_filename) != SCR_SUCCESS) {
            scr_err("Reading filename from meta data from %s @ %s:%d",
                    name_tmp, __FILE__, __LINE__
            );
            scr_meta_delete(meta);
            continue;
          }

          /* read filesize from meta data */
          unsigned long meta_filesize = 0;
          if (scr_meta_get_filesize(meta, &meta_filesize) != SCR_SUCCESS) {
            scr_err("Reading filesize from meta data from %s @ %s:%d",
                    name_tmp, __FILE__, __LINE__
            );
            scr_meta_delete(meta);
            continue;
          }

#if 0
#endif

          /* set our ranks if it's not been set */
          if (ranks == -1) {
            ranks = meta_ranks;
          }

/* TODO: need to check directories on all of these file names */

          /* build the full path to the file named in the meta file */
          char full_filename[SCR_MAX_FILENAME];
          if (scr_build_path(full_filename, sizeof(full_filename), dir, file_name) != SCR_SUCCESS) {
            scr_err("Filename too long to copy into internal buffer: %s/%s @ %s:%d",
                    dir, file_name, __FILE__, __LINE__
            );
            scr_meta_delete(meta);
            continue;
          }

          /* check that the file name matches */
          if (strcmp(name_tmp, full_filename) != 0) {
            scr_err("File name of %s does not match internal file name %s @ %s:%d",
                    name_tmp, full_filename, __FILE__, __LINE__
            );
            scr_meta_delete(meta);
            continue;
          }

          /* check that the file is complete */
          if (scr_meta_is_complete(meta) != SCR_SUCCESS) {
            scr_err("File is not complete: %s @ %s:%d",
                    full_filename, __FILE__, __LINE__
            );
            scr_meta_delete(meta);
            continue;
          }

          /* check that the file exists */
          if (scr_file_exists(full_filename) != SCR_SUCCESS) {
            scr_err("File does not exist: %s @ %s:%d",
                    full_filename, __FILE__, __LINE__
            );
            scr_meta_delete(meta);
            continue;
          }

          /* check that the file size matches */
          unsigned long size = scr_filesize(full_filename);
          if (meta_filesize != size) {
            scr_err("File is %lu bytes but expected to be %lu bytes: %s @ %s:%d",
                    size, meta_filesize, full_filename, __FILE__, __LINE__
            );
            scr_meta_delete(meta);
            continue;
          }

#if 0
#endif

          /* check that the ranks match */
          if (meta_ranks != ranks) {
            scr_err("File was created with %d ranks, but expected %d ranks: %s @ %s:%d",
                    meta_ranks, ranks, full_filename, __FILE__, __LINE__
            );
            scr_meta_delete(meta);
            continue;
          }

          /* DLIST
           *   <dataset_id>
           *     RANK2FILE
           *       RANKS
           *         <num_ranks>
           *       RANK
           *         <rank>
           *           FILE
           *             <filename>
           *               SIZE
           *                 <filesize>
           *               CRC
           *                 <crc> */
          /* TODODSET: rank2file_hash may not exist yet */
          scr_hash* list_hash = scr_hash_set_kv_int(scan, SCR_SCAN_KEY_DLIST, dset_id);
          scr_hash* rank2file_hash = scr_hash_get(list_hash, SCR_SUMMARY_6_KEY_RANK2FILE);
          scr_hash_set_kv_int(rank2file_hash, SCR_SUMMARY_6_KEY_RANKS, meta_ranks);
          scr_hash* rank_hash = scr_hash_set_kv_int(rank2file_hash, SCR_SUMMARY_6_KEY_RANK, rank_id);
          scr_hash* file_hash = scr_hash_set_kv(rank_hash, SCR_SUMMARY_6_KEY_FILE, meta_filename);
          scr_hash_util_set_bytecount(file_hash, SCR_SUMMARY_6_KEY_SIZE, meta_filesize);

          uLong meta_crc;
          if (scr_meta_get_crc32(meta, &meta_crc) == SCR_SUCCESS) {
            scr_hash_util_set_crc32(file_hash, SCR_SUMMARY_6_KEY_CRC, meta_crc);
          }

          /* if the file is an XOR file, read in the XOR set parameters */
          if (scr_meta_check_filetype(meta, SCR_META_FILE_XOR) == SCR_SUCCESS) {
            /* mark this file as being an XOR file */
            scr_hash_set(file_hash, SCR_SUMMARY_6_KEY_NOFETCH, NULL);

            /* extract the xor set id, the size of the xor set, and our position within the set */
            size_t nmatch = 4;
            regmatch_t pmatch[nmatch];
            char* value = NULL;
            int xor_rank, xor_ranks, xor_setid;
            if (regexec(&re_xor_file, name_tmp, nmatch, pmatch, 0) == 0) {
              xor_rank  = -1;
              xor_ranks = -1;
              xor_setid = -1;

              /* get the rank in the xor set */
              value = strndup(name_tmp + pmatch[1].rm_so, (size_t)(pmatch[1].rm_eo - pmatch[1].rm_so));
              if (value != NULL) {
                xor_rank = atoi(value);
                free(value);
                value = NULL;
              }

              /* get the size of the xor set */
              value = strndup(name_tmp + pmatch[2].rm_so, (size_t)(pmatch[2].rm_eo - pmatch[2].rm_so));
              if (value != NULL) {
                xor_ranks = atoi(value);
                free(value);
                value = NULL;
              }

              /* get the id of the xor set */
              value = strndup(name_tmp + pmatch[3].rm_so, (size_t)(pmatch[3].rm_eo - pmatch[3].rm_so));
              if (value != NULL) {
                xor_setid = atoi(value);
                free(value);
                value = NULL;
              }

              /* add the XOR file entries into our scan hash */
              if (xor_rank != -1 && xor_ranks != -1 && xor_setid != -1) {
                /* DLIST
                 *   <dataset_id>
                 *     XOR
                 *       <xor_setid>
                 *         MEMBERS
                 *           <num_members_in_xor_set>
                 *         MEMBER
                 *           <xor_set_member_id>
                 *             FILE
                 *               <filename>
                 *             RANK
                 *               <rank_id> */
                scr_hash* xor_hash = scr_hash_set_kv_int(list_hash, SCR_SCAN_KEY_XOR, xor_setid);
                scr_hash_set_kv_int(xor_hash, SCR_SCAN_KEY_MEMBERS, xor_ranks);
                scr_hash* xor_rank_hash = scr_hash_set_kv_int(xor_hash, SCR_SCAN_KEY_MEMBER, xor_rank);
                scr_hash_set_kv(xor_rank_hash, SCR_SUMMARY_6_KEY_FILE, meta_filename);
                scr_hash_set_kv_int(xor_rank_hash, SCR_SUMMARY_6_KEY_RANK, rank_id);
              } else {
                scr_err("Failed to extract XOR rank, set size, or set id from %s @ %s:%d",
                        full_filename, __FILE__, __LINE__
                );
              }
            } else {
              scr_err("XOR file does not match expected file name format %s @ %s:%d",
                      full_filename, __FILE__, __LINE__
              );
            }
          }

          scr_meta_delete(meta);
        }
      }
    }

    /* delete the filemap */
    scr_filemap_delete(rank_map);
  }

  /* free the xor regular expression */
  regfree(&re_xor_file);

  /* delete the hash holding the file and directory names */
  scr_hash_delete(contents);

  return SCR_SUCCESS;
}

/* builds and writes the summary file for the given directory
 * Returns SCR_SUCCESS if the summary file exists or was written,
 * but this does not imply the dataset is valid, only that the summary
 * file was written */
int scr_summary_build(const char* dir)
{
  int rc = SCR_SUCCESS;

  /* create a new hash to store our index file data */
  scr_hash* summary = scr_hash_new();

  if (scr_summary_read(dir, summary) != SCR_SUCCESS) {
    /* now only return success if we successfully write the file */
    int rc = SCR_FAILURE;

    /* create a new hash to store our scan results */
    scr_hash* scan = scr_hash_new();

    /* scan the files in the given directory */
    scr_scan_files(dir, scan);

    /* determine whether we are missing any files */
    if (scr_inspect_scan(scan) != SCR_SUCCESS) {
      /* missing some files, see if we can rebuild them */
      if (scr_rebuild_scan(dir, scan) == SCR_SUCCESS) {
        /* the rebuild succeeded, clear our scan hash */
        scr_hash_unset_all(scan);

        /* rescan the files */
        scr_scan_files(dir, scan);

        /* reinspect the files */
        scr_inspect_scan(scan);
      }
    }

    /* build summary:
     *   should only have one dataset
     *   remove BUILD, MISSING, UNRECOVERABLE, INVALID, XOR
     *   delete XOR files from the file list, and adjust the expected number of files
     *   (maybe we should just leave these in here, at least the missing list?) */
    scr_hash_elem* list_elem = NULL;
    scr_hash* list_hash = scr_hash_get(scan, SCR_SCAN_KEY_DLIST);
    int list_size = scr_hash_size(list_hash);
    if (list_size == 1) {
      for (list_elem = scr_hash_elem_first(list_hash);
           list_elem != NULL;
           list_elem = scr_hash_elem_next(list_elem))
      {
        /* get the hash for this checkpoint */
        scr_hash* dset_hash = scr_hash_elem_hash(list_elem);

        /* unset the BUILD, MISSING, UNRECOVERABLE, INVALID, and XOR keys for this checkpoint */
        scr_hash_unset(dset_hash, SCR_SCAN_KEY_BUILD);
        scr_hash_unset(dset_hash, SCR_SCAN_KEY_MISSING);
        scr_hash_unset(dset_hash, SCR_SCAN_KEY_UNRECOVERABLE);
        scr_hash_unset(dset_hash, SCR_SCAN_KEY_INVALID);
        scr_hash_unset(dset_hash, SCR_SCAN_KEY_XOR);

        /* record the summary file version number */
        scr_hash_set_kv_int(dset_hash, SCR_SUMMARY_KEY_VERSION, SCR_SUMMARY_FILE_VERSION_6);

        /* write the summary file out */
        rc = scr_summary_write(dir, dset_hash);
      }
    }

    /* free the scan hash */
    scr_hash_delete(scan);
  }

  /* delete the summary hash */
  scr_hash_delete(summary);

  return rc;
}

/* returns SCR_SUCCESS only if named directory is explicitly marked as complete
 * in the index file */
int is_complete(const char* prefix, const char* dir)
{
  int rc = SCR_FAILURE;

  /* create a new hash to store our index file data */
  scr_hash* index = scr_hash_new();

  /* read index file from the prefix directory */
  scr_index_read(prefix, index); 

  /* lookup the dataset id based on the directory name */
  int id = 0;
  if (scr_index_get_id_by_dir(index, dir, &id) == SCR_SUCCESS) {
    if (id != -1) {
      /* found the dataset id, now lookup its COMPLETE value */
      int complete = 0;
      if (scr_index_get_complete(index, id, dir, &complete) == SCR_SUCCESS) {
        if (complete == 1) {
          /* only return success if we find a value for complete, and if that value is 1 */
          rc = SCR_SUCCESS;
        }
      }
    }
  }

  /* free the index hash */
  scr_hash_delete(index);

  return rc;
}

int index_list (const char* prefix)
{
  int rc = SCR_SUCCESS;

  /* create a new hash to store our index file data */
  scr_hash* index = scr_hash_new();

  /* read index file from the prefix directory */
  if (scr_index_read(prefix, index) != SCR_SUCCESS) {
    scr_err("Failed to read index file in %s @ %s:%d",
            prefix, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* TODO: we should bury this logic in scr_index_* functions */

  /* get a pointer to the checkpoint hash */
  scr_hash* dset_hash = scr_hash_get(index, SCR_INDEX_1_KEY_DATASET);

  /* sort datasets in descending order */
  scr_hash_sort_int(dset_hash, SCR_HASH_SORT_DESCENDING);

//  printf("FLAGS  FLUSHED              FETCH  LAST_FETCHED         CKPT  DIRECTORY\n");
  printf("FLAGS  FLUSHED              DSET  DIRECTORY\n");
  /* iterate over each of the datasets and print the id and other info */
  scr_hash_elem* elem;
  for (elem = scr_hash_elem_first(dset_hash);
       elem != NULL;
       elem = scr_hash_elem_next(elem))
  {
    /* get the dataset id */
    int dset = scr_hash_elem_key_int(elem);

    /* get the hash for this dataset */
    scr_hash* hash = scr_hash_elem_hash(elem);
    scr_hash* dir_hash = scr_hash_get(hash, SCR_INDEX_1_KEY_DIR);

    /* TODO: since directories have the date and time in their name,
     * this is a hacky way to list directories in order from most recent
     * to oldest. */
    /* sort directories in descending order */
    scr_hash_sort(dir_hash, SCR_HASH_SORT_DESCENDING);

    scr_hash_elem* dir_elem;
    for (dir_elem = scr_hash_elem_first(dir_hash);
         dir_elem != NULL;
         dir_elem = scr_hash_elem_next(dir_elem))
    {
      /* get the directory name for this dataset */
      char* dir = scr_hash_elem_key(dir_elem);

      /* get the directory hash */
      scr_hash* info_hash = scr_hash_elem_hash(dir_elem);

      /* determine whether this dataset is complete */
      int complete = 0;
      scr_hash_util_get_int(info_hash, SCR_INDEX_1_KEY_COMPLETE, &complete);

      /* determine time at which this checkpoint was marked as failed */
      char* failed_str = scr_hash_elem_get_first_val(info_hash, SCR_INDEX_1_KEY_FAILED);

      /* determine time at which this checkpoint was flushed */
      char* flushed_str = scr_hash_elem_get_first_val(info_hash, SCR_INDEX_1_KEY_FLUSHED);

      /* compute number of times (and last time) checkpoint has been fetched */
/*
      scr_hash* fetched_hash = scr_hash_get(info_hash, SCR_INDEX_1_KEY_FETCHED);
      int num_fetch = scr_hash_size(fetched_hash);
      scr_hash_sort(fetched_hash, SCR_HASH_SORT_DESCENDING);
      scr_hash_elem* fetched_elem = scr_hash_elem_first(fetched_hash);
      char* fetched_str = scr_hash_elem_key(fetched_elem);
*/

      if (complete != 1) {
        printf("x");
      } else {
        printf("c");
      }
      if (failed_str != NULL) {
        printf("f");
      } else {
        printf("-");
      }
      printf("   ");

      printf("  ");
      if (flushed_str != NULL) {
        printf("%s", flushed_str);
      } else {
        printf("                   ");
      }

/*
      printf("%7d", num_fetch);

      printf("  ");
      if (flushed_str != NULL) {
        printf("%s", fetched_str);
      } else {
        printf("                   ");
      }
*/

      printf("%6d", dset);

      printf("  ");
      if (dir != NULL) {
        printf("%s", dir);
      } else {
        printf("UNKNOWN_DIRECTORY");
      }
      printf("\n");
    }
  }

  /* free off our index hash */
  scr_hash_delete(index);

  return rc;
}

/* delete named directory from index (does not delete files) */
int index_remove_dir (const char* prefix, const char* subdir)
{
  int rc = SCR_SUCCESS;

  /* create a new hash to store our index file data */
  scr_hash* index = scr_hash_new();

  /* read index file from the prefix directory */
  if (scr_index_read(prefix, index) != SCR_SUCCESS) {
    scr_err("Failed to read index file in %s @ %s:%d",
            prefix, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* lookup the dataset id based on the directory name */
  int id;
  if (scr_index_get_id_by_dir(index, subdir, &id) == SCR_SUCCESS) {
    /* delete directory from the directory-to-dataset-id index */
    scr_hash_unset_kv(index, SCR_INDEX_1_KEY_DIR, subdir);

    /* get the hash for this dataset id */
    scr_hash* dset = scr_hash_get_kv_int(index, SCR_INDEX_1_KEY_DATASET, id);

    /* delete this directory from the hash for this dataset id */
    scr_hash_unset_kv(dset, SCR_INDEX_1_KEY_DIR, subdir);

    /* if that was the only directory for this dataset id,
     * also delete the dataset id field */
    if (scr_hash_size(dset) == 0) {
      scr_hash_unset_kv_int(index, SCR_INDEX_1_KEY_DATASET, id);
    }

    /* write out the new index file */
    scr_index_write(prefix, index); 
  } else {
    /* couldn't find the named directory, print an error */
    scr_err("Named directory was not found in index file: %s @ %s:%d",
            subdir, __FILE__, __LINE__
    );
    rc = SCR_FAILURE;
  }

  /* free off our index hash */
  scr_hash_delete(index);

  return rc;
}

/* given a prefix directory and a dataset directory,
 * attempt add the dataset directory to the index file.
 * Returns SCR_SUCCESS if dataset directory can be indexed,
 * either as complete or incomplete */
int index_add_dir (const char* prefix, const char* subdir)
{
  int rc = SCR_SUCCESS;

  /* create a new hash to store our index file data */
  scr_hash* index = scr_hash_new();

  /* read index file from the prefix directory */
  scr_index_read(prefix, index); 

  /* if named directory is already indexed, exit with success */
  int id;
  if (scr_index_get_id_by_dir(index, subdir, &id) != SCR_SUCCESS) {
    /* create a new hash to hold our summary file data */
    scr_hash* summary = scr_hash_new();

    /* read summary file from the dataset directory */
    char dataset_dir[SCR_MAX_FILENAME];
    scr_build_path(dataset_dir, sizeof(dataset_dir), prefix, subdir);
    if (scr_summary_read(dataset_dir, summary) != SCR_SUCCESS) {
      /* if summary file is missing, attempt to build it */
      if (scr_summary_build(dataset_dir) == SCR_SUCCESS) {
        /* if the build was successful, try the read again */
        scr_summary_read(dataset_dir, summary);
      }
    }
    
    /* get the dataset hash for this directory */
    scr_dataset* dataset = scr_hash_get(summary, SCR_SUMMARY_6_KEY_DATASET);
    if (dataset != NULL) {
      int dataset_id;
      if (scr_dataset_get_id(dataset, &dataset_id) == SCR_SUCCESS) {
        /* found the id, now check whether it's complete (assume that it's not) */
        int complete;
        if (scr_hash_util_get_int(summary, SCR_SUMMARY_6_KEY_COMPLETE, &complete) == SCR_SUCCESS) {
          char* dataset_name;
          if (scr_dataset_get_name(dataset, &dataset_name) == SCR_SUCCESS) {
            /* write values to the index file */
            scr_index_set_dataset(index, dataset, complete);
            scr_index_mark_flushed(index, dataset_id, dataset_name);
            scr_index_write(prefix, index); 
          } else {
            /* failed to read dataset name */
            rc = SCR_FAILURE;
          }
        } else {
          /* failed to read complete flag */
          rc = SCR_FAILURE;
        }
      } else {
        /* failed to find dataset id */
        rc = SCR_FAILURE;
      }
    } else {
      /* failed to find dataset hash in summary file, so we can't index it */
      rc = SCR_FAILURE;
    }
    
    /* free our summary file hash */
    scr_hash_delete(summary);
  }

  /* free our index hash */
  scr_hash_delete(index);

  return rc;
}

int print_usage()
{
  printf("\n");
  printf("  Usage: scr_index [options]\n");
  printf("\n");
  printf("  Options:\n");
  printf("    -l, --list          List indexed datasets (default behavior)\n");
  printf("    -a, --add=<dir>     Add dataset directory <dir> to index\n");
  printf("    -r, --remove=<dir>  Remove dataset directory <dir> from index (does not delete files)\n");
  printf("    -p, --prefix=<dir>  Specify prefix directory (defaults to current working directory)\n");
  printf("    -h, --help          Print usage\n");
  printf("\n");
  return SCR_SUCCESS;
}

struct arglist {
  char* prefix;
  char* dir;
  int list;
  int add;
  int remove;
};

/* free any memory allocation during get_args */
int free_args(struct arglist* args)
{
  if (args->prefix != NULL) {
    free(args->prefix);
    args->prefix = NULL;
  }

  if (args->dir != NULL) {
    free(args->dir);
    args->dir = NULL;
  }

  return SCR_SUCCESS;
}

int get_args(int argc, char **argv, struct arglist* args)
{
  /* set to default values */
  args->prefix = NULL;
  args->dir    = NULL;
  args->list   = 1;
  args->add    = 0;
  args->remove = 0;

  static const char *opt_string = "la:r:p:h";
  static struct option long_options[] = {
    {"list",   no_argument,       NULL, 'l'},
    {"add",    required_argument, NULL, 'a'},
    {"remove", required_argument, NULL, 'r'},
    {"prefix", required_argument, NULL, 'p'},
    {"help",   no_argument,       NULL, 'h'},
    {NULL,     no_argument,       NULL,   0}
  };

  int long_index = 0;
  int opt = getopt_long(argc, argv, opt_string, long_options, &long_index);
  while (opt != -1) {
    switch(opt) {
      case 'l':
        args->list = 1;
        break;
      case 'a':
        args->dir  = strdup(optarg);
        args->add  = 1;
        args->list = 0;
        break;
      case 'r':
        args->dir    = strdup(optarg);
        args->remove = 1;
        args->list   = 0;
        break;
      case 'p':
        args->prefix = strdup(optarg);
        break;
      case 'h':
        return SCR_FAILURE;
      default:
        return SCR_FAILURE;
        break;
    }

    /* get the next option */
    opt = getopt_long(argc, argv, opt_string, long_options, &long_index);
  }

  /* if the user didn't specify a prefix directory, use the current working directory */
  if (args->prefix == NULL) {
    char prefix[SCR_MAX_FILENAME];
    if (getcwd(prefix, sizeof(prefix)) == NULL) {
      scr_err("Problem reading current working directory (getcwd() errno=%d %m) @ %s:%d",
              errno, __FILE__, __LINE__
      );
      return SCR_FAILURE;
    }
    args->prefix = strdup(prefix);
  }

  return SCR_SUCCESS;
}

int main(int argc, char *argv[])
{
  int rc = SCR_FAILURE;

  /* get our command line arguments */
  struct arglist args;
  if (get_args(argc, argv, &args) != SCR_SUCCESS) {
    print_usage();
    return 1;
  }

  /* check that the named dataset directory is complete */
  if (args.add == 1) {
    /* check that we have a prefix and dataset directory defined */
    if (args.prefix == NULL || args.dir == NULL) {
      print_usage();
      return 1;
    }

    /* record the name of the prefix and dataset directories */
    char* prefix = args.prefix;
    char* dir = args.dir;

    /* add the dataset directory dir to the index.scr file in the prefix directory,
     * rebuild missing files if necessary */
    rc = SCR_FAILURE;
    if (index_add_dir(prefix, dir) == SCR_SUCCESS) {
      rc = is_complete(prefix, dir);
    }
  }

  /* remove the named directory from the index file (does not delete files) */
  if (args.remove == 1) {
    /* check that we have a prefix and dataset directory defined */
    if (args.prefix == NULL || args.dir == NULL) {
      print_usage();
      return 1;
    }

    /* record the name of the prefix and dataset directories */
    char* prefix = args.prefix;
    char* dir = args.dir;

    /* remove the directory */
    rc = index_remove_dir(prefix, dir);
  }

  /* list datasets recorded in index file */
  if (args.list == 1) {
    /* check that we have a prefix directory defined */
    if (args.prefix == NULL) {
      print_usage();
      return 1;
    }
    rc = index_list(args.prefix);
  }

  /* free any memory allocated for command line arguments */
  free_args(&args);

  /* translate our SCR return code into program return code */
  if (rc != SCR_SUCCESS) {
    return 1;
  }
  return 0;
}
