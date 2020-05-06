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

#define _GNU_SOURCE

/* read in the config.h to pick up any parameters set from configure
 * these values will override any settings below */
#include "config.h"

#include "scr_globals.h"
#include "scr_conf.h"
#include "scr.h"
#include "scr_io.h"
#include "scr_err.h"
#include "scr_util.h"
#include "scr_meta.h"
#include "scr_filemap.h"
#include "scr_param.h"
#include "scr_index_api.h"

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
#include <sys/wait.h>
#include <getopt.h>

#include <dirent.h>

#include <regex.h>

#define SCR_IO_KEY_DIR     ("DIR")
#define SCR_IO_KEY_FILE    ("FILE")
#define SCR_IO_KEY_UNKNOWN ("UNKNOWN")

#define SCR_SUMMARY_FILENAME "summary.scr"
#define BUILD_XOR_CMD     (X_BINDIR"/scr_rebuild_xor")
#define BUILD_PARTNER_CMD (X_BINDIR"/scr_rebuild_partner")

#define SCR_SCAN_KEY_MAP "MAP"

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
 *     PARTNER
 *       LEFT
 *         <partner_rank>
 *           FILE
 *             <xor_file_name>
 *           RANK
 *             <rank>
 *       RIGHT
 *         <partner_rank>
 *           FILE
 *             <xor_file_name>
 *           RANK
 *             <rank>
 *     XORMAP
 *       <xor_setid>
 *         MEMBERS
 *           <num_members_in_set>
 *         MEMBER
 *           <member_number>
 *             FILE
 *               <xor_file_name>
 *             RANK
 *               <rank>
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
 *     XORMAP
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

#define SCR_SCAN_KEY_PARTNER    ("PARTNER")
#define SCR_SCAN_KEY_MAPPARTNER ("MAPPARTNER")
#define SCR_SCAN_KEY_LEFT       ("LEFT")
#define SCR_SCAN_KEY_RIGHT      ("RIGHT")

#define SCR_SCAN_KEY_XOR      ("XOR")
#define SCR_SCAN_KEY_MAPXOR   ("MAPXOR")
#define SCR_SCAN_KEY_MEMBER   ("MEMBER")
#define SCR_SCAN_KEY_MEMBERS  ("MEMBERS")

#define SCR_SCAN_KEY_DLIST    ("DLIST")
#define SCR_SCAN_KEY_MISSING  ("MISSING")
#define SCR_SCAN_KEY_INVALID  ("INVALID")
#define SCR_SCAN_KEY_UNRECOVERABLE ("UNRECOVERABLE")
#define SCR_SCAN_KEY_BUILD    ("BUILD")

/* read the file and directory names from dir and return in hash */
int scr_read_dir(const spath* dir, kvtree* hash)
{
  int rc = SCR_SUCCESS;

  /* allocate directory in string form */
  char* dir_str = spath_strdup(dir);

  /* open the directory */
  DIR* dirp = opendir(dir_str);
  if (dirp == NULL) {
    scr_err("Failed to open directory %s (errno=%d %s) @ %s:%d",
      dir_str, errno, strerror(errno), __FILE__, __LINE__
    );
    scr_free(&dir_str);
    return SCR_FAILURE;
  }

  /* read each file from the directory */
  struct dirent* dp = NULL;
  do {
    errno = 0;
    dp = readdir(dirp);
    if (dp != NULL) {
      #ifdef _DIRENT_HAVE_D_TYPE
        /* distinguish between directories and files if we can */
        if (dp->d_type == DT_DIR) {
          kvtree_set_kv(hash, SCR_IO_KEY_DIR, dp->d_name);
        } else {
          kvtree_set_kv(hash, SCR_IO_KEY_FILE, dp->d_name);
        }
      #else
        /* TODO: throw a compile error here instead? */
        kvtree_set_kv(hash, SCR_IO_KEY_FILE, dp->d_name);
      #endif
    } else {
      if (errno != 0) {
        scr_err("Failed to read directory %s (errno=%d %s) @ %s:%d",
          dir_str, errno, strerror(errno), __FILE__, __LINE__
        );
        rc = SCR_FAILURE;
      }
    }
  } while (dp != NULL);

  /* close the directory */
  if (closedir(dirp) < 0) {
    scr_err("Failed to close directory %s (errno=%d %s) @ %s:%d",
      dir_str, errno, strerror(errno), __FILE__, __LINE__
    );
    scr_free(&dir_str);
    return SCR_FAILURE;
  }

  /* free our directory string */
  scr_free(&dir_str);

  return rc;
}

int scr_summary_read(const spath* dir, kvtree* hash)
{
  int rc = SCR_SUCCESS;

  /* build the filename for the summary file */
  spath* path = spath_dup(dir);
  spath_append_str(path, SCR_SUMMARY_FILENAME);
  char* summary_file = spath_strdup(path);

  /* TODO: need to try reading every file */

  /* check whether the file exists before we attempt to read it
   * (do this error to avoid printing an error in kvtree_read) */
  if (scr_file_exists(summary_file) != SCR_SUCCESS) {
    rc = SCR_FAILURE;
    goto cleanup;
  }

  /* now attempt to read the file contents into the hash */
  if (kvtree_read_file(summary_file, hash) != KVTREE_SUCCESS) {
    rc = SCR_FAILURE;
    goto cleanup;
  }

cleanup:
  /* free off objects and return with failure */
  scr_free(&summary_file);
  spath_delete(&path);

  return rc;
}

static int kvtree_write_scatter_file(const spath* meta_path, const char* filename, const kvtree* rank2file)
{
  int rc = SCR_SUCCESS;

  /* this is an ugly hack until we turn this into a parallel operation
   * format of rank2file is a tree of files which we hard-code to be
   * two-levels deep here */

  kvtree* ranks_hash = kvtree_get(rank2file, "RANK");
  kvtree_sort_int(ranks_hash, KVTREE_SORT_ASCENDING);

  /* create hash for primary rank2file map and encode level */
  kvtree* files_hash = kvtree_new();
  kvtree_set_kv_int(files_hash, "LEVEL", 1);

  /* iterate over each rank to record its info */
  int writer = 0;
  int max_rank = -1;
  kvtree_elem* elem = kvtree_elem_first(ranks_hash);
  while (elem != NULL) {
    /* build name for rank2file part */
    spath* rank2file_path = spath_dup(meta_path);
    spath_append_strf(rank2file_path, "%s.0.%d", filename, writer);

    /* create a hash to record an entry from each rank */
    kvtree* entries = kvtree_new();
    kvtree_set_kv_int(entries, "LEVEL", 0);

    /* record up to 8K entries */
    int count = 0;
    while (count < 8192) {
      /* get rank id */
      int rank = kvtree_elem_key_int(elem);
      if (rank > max_rank) {
        max_rank = rank;
      }

      /* copy hash of current rank under RANK/<rank> in entries */
      kvtree* elem_hash = kvtree_elem_hash(elem);
      kvtree* rank_hash = kvtree_set_kv_int(entries, "RANK", rank);
      kvtree_merge(rank_hash, elem_hash);
      count++;

      /* break early if we reach the end */
      elem = kvtree_elem_next(elem);
      if (elem == NULL) {
        break;
      }
    }

    /* record the number of ranks */
    kvtree_set_kv_int(entries, "RANKS", count);

    /* write hash to file rank2file part */
    if (kvtree_write_path(rank2file_path, entries) != KVTREE_SUCCESS) {
      rc = SCR_FAILURE;
      elem = NULL;
    }

    /* record file name of part in files hash, relative to prefix directory */
    char partname[1024];
    snprintf(partname, sizeof(partname), ".0.%d", writer);
    unsigned long offset = 0;
    kvtree* files_rank_hash = kvtree_set_kv_int(files_hash, "RANK", writer);
    kvtree_util_set_str(files_rank_hash, "FILE", partname);
    kvtree_util_set_bytecount(files_rank_hash, "OFFSET", offset);

    /* delete part hash and path */
    kvtree_delete(&entries);
    spath_delete(&rank2file_path);

    /* get id of next writer */
    writer += count;
  }

  /* TODO: a cleaner way to do this is to only write this info if the
   * rebuild is successful, then we simply count the total ranks */
  /* record total number of ranks in job as max rank + 1 */
  kvtree_set_kv_int(files_hash, "RANKS", max_rank+1);

  /* write out rank2file map */
  spath* files_path = spath_dup(meta_path);
  spath_append_str(files_path, filename);
  if (kvtree_write_path(files_path, files_hash) != KVTREE_SUCCESS) {
    rc = SCR_FAILURE;
  }
  spath_delete(&files_path);
  kvtree_delete(&files_hash);

  return rc;
}

int scr_summary_write(const spath* prefix, const spath* dir, kvtree* hash)
{
  int rc = SCR_SUCCESS;

  /* build the path for the scr metadata directory */
  spath* meta_path = spath_dup(dir);

  /* get pointer to RANK2FILE info sorted by rank */
  kvtree* rank2file  = kvtree_get(hash, SCR_SUMMARY_6_KEY_RANK2FILE);

  /* write rank2file map files */
  rc = kvtree_write_scatter_file(meta_path, "rank2file", rank2file);

  /* remove RANK2FILE from summary hash */
  kvtree_unset(hash, SCR_SUMMARY_6_KEY_RANK2FILE);

  /* TODO: write DATASET to summary file */

  /* write summary file */
  spath* summary_path = spath_dup(meta_path);
  spath_append_str(summary_path, SCR_SUMMARY_FILENAME);
  if (kvtree_write_path(summary_path, hash) != KVTREE_SUCCESS) {
    rc = SCR_FAILURE;
  }
  spath_delete(&summary_path);

  /* free off objects and return with failure */
  spath_delete(&meta_path);

  return rc;
}

/* forks and execs processes to rebuild missing files and waits for them to complete,
 * returns SCR_FAILURE if any dataset failed to rebuild, SCR_SUCCESS otherwise */
int scr_fork_rebuilds(const spath* dir, const char* build_cmd, kvtree* cmds)
{
  int rc = SCR_SUCCESS;

  /* count the number of build commands */
  int builds = kvtree_size(cmds);

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

  /* allocate character string for chdir */
  char* dir_str = spath_strdup(dir);

  /* TODO: flow control the number of builds ongoing at a time */

  /* step through and fork off each of our build commands */
  int pid_count = 0;
  kvtree_elem* elem = NULL;
  for (elem = kvtree_elem_first(cmds);
       elem != NULL;
       elem = kvtree_elem_next(elem))
  {
    /* get the hash of argv values for this command */
    kvtree* cmd_hash = kvtree_elem_hash(elem);

    /* sort the arguments by their index */
    kvtree_sort_int(cmd_hash, KVTREE_SORT_ASCENDING);

    /* print the command to screen, so the user knows what's happening */
    int offset = 0;
    char full_cmd[SCR_MAX_FILENAME];
    kvtree_elem* arg_elem = NULL;
    for (arg_elem = kvtree_elem_first(cmd_hash);
         arg_elem != NULL;
         arg_elem = kvtree_elem_next(arg_elem))
    {
      char* key = kvtree_elem_key(arg_elem);
      char* arg_str = kvtree_elem_get_first_val(cmd_hash, key);
      int remaining = sizeof(full_cmd) - offset;
      if (remaining > 0) {
        offset += snprintf(full_cmd + offset, remaining, "%s ", arg_str);
      }
    }
    scr_dbg(0, "Rebuild command: %s\n", full_cmd);

    /* count the number of command line arguments */
    int argc = kvtree_size(cmd_hash);

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
      kvtree_elem* arg_elem = NULL;
      for (arg_elem = kvtree_elem_first(cmd_hash);
           arg_elem != NULL;
           arg_elem = kvtree_elem_next(arg_elem))
      {
        char* key = kvtree_elem_key(arg_elem);
        argv[index] = kvtree_elem_get_first_val(cmd_hash, key);
        index++;
      }
      argv[index] = NULL;

      /* cd to current working directory */
      if (chdir(dir_str) != 0) {
        scr_err("Failed to change to directory %s @ %s:%d",
          dir_str, __FILE__, __LINE__
        );
        exit(1);
      }

      /* execv the build command */
      execv(build_cmd, argv);
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

  /* free the directory string */
  scr_free(&dir_str);

  /* free the pid array */
  scr_free(&pids);

  return rc;
}

static int scr_rebuild_partner(const spath* prefix, const spath* dir, int dset_id, kvtree* dset_hash, const kvtree* missing_hash, const char* type_key, const char* type_cmd)
{
  int rc = SCR_SUCCESS;

  /* at least one rank is missing files, attempt to rebuild them */
  int build_command_count = 0;

  /* for each item in missing hash, see if we have partner entry */
  kvtree_elem* elem;
  for (elem = kvtree_elem_first(missing_hash);
       elem != NULL;
       elem = kvtree_elem_next(elem))
  {
    /* get the rank id that we're missing */
    int missing_rank = kvtree_elem_key_int(elem);

    /* check whether we have an entry partner entry for this rank */
    kvtree* partner_hash = kvtree_get_kv_int(dset_hash, type_key, missing_rank);
    if (partner_hash == NULL) {
      /* no partner info for this rank, mark entire set as unrecoverable */
      kvtree_set_kv_int(dset_hash, SCR_SCAN_KEY_UNRECOVERABLE, missing_rank);
    } else {
      /* extract values for command from hash */
      kvtree* lhs_hash = kvtree_get(partner_hash, SCR_SCAN_KEY_LEFT);
      kvtree* rhs_hash = kvtree_get(partner_hash, SCR_SCAN_KEY_RIGHT);
      char* lhs_rank_str = kvtree_elem_get_first_val(lhs_hash, SCR_SUMMARY_6_KEY_RANK);
      char* lhs_filename = kvtree_elem_get_first_val(lhs_hash, SCR_SUMMARY_6_KEY_FILE);
      char* rhs_rank_str = kvtree_elem_get_first_val(rhs_hash, SCR_SUMMARY_6_KEY_RANK);
      char* rhs_filename = kvtree_elem_get_first_val(rhs_hash, SCR_SUMMARY_6_KEY_FILE);

      /* got partner info, put together a command to rebuild */
      kvtree* buildcmd_hash = kvtree_set_kv_int(dset_hash, SCR_SCAN_KEY_BUILD, build_command_count);
      build_command_count++;

      int argc = 0;

      /* write the command name */
      kvtree_setf(buildcmd_hash, NULL, "%d %s", argc, BUILD_PARTNER_CMD);
      argc++;

      /* whether to rebuild map or data files */
      kvtree_setf(buildcmd_hash, NULL, "%d %s", argc, type_cmd);
      argc++;

      /* left rank */
      kvtree_setf(buildcmd_hash, NULL, "%d %s", argc, lhs_rank_str);
      argc++;

      /* missing rank */
      kvtree_setf(buildcmd_hash, NULL, "%d %d", argc, missing_rank);
      argc++;

      /* right rank */
      kvtree_setf(buildcmd_hash, NULL, "%d %s", argc, rhs_rank_str);
      argc++;

      /* left partner file */
      kvtree_setf(buildcmd_hash, NULL, "%d %s", argc, lhs_filename);
      argc++;

      /* right partner file */
      kvtree_setf(buildcmd_hash, NULL, "%d %s", argc, rhs_filename);
      argc++;
    }
  }

  /* rebuild if we can */
  kvtree* unrecoverable = kvtree_get(dset_hash, SCR_SCAN_KEY_UNRECOVERABLE);
  char* dir_str = spath_strdup(dir);
  if (unrecoverable != NULL) {
    /* at least some files cannot be recovered */
    scr_err("Insufficient files to attempt rebuild of dataset %d in %s @ %s:%d",
      dset_id, dir_str, __FILE__, __LINE__
    );
    rc = SCR_FAILURE;
  } else {
    /* we have a shot to rebuild everything, let's give it a go */
    kvtree* builds_hash = kvtree_get(dset_hash, SCR_SCAN_KEY_BUILD);
    if (scr_fork_rebuilds(dir, BUILD_PARTNER_CMD, builds_hash) != SCR_SUCCESS) {
      scr_err("At least one rebuild failed for dataset %d in %s @ %s:%d",
        dset_id, dir_str, __FILE__, __LINE__
      );
      rc = SCR_FAILURE;
    }
  }
  scr_free(&dir_str);

  return rc;
}

static int scr_rebuild_xor(const spath* prefix, const spath* dir, int dset_id, kvtree* dset_hash, const kvtree* missing_hash, const char* type_key, const char* type_cmd)
{
  int rc = SCR_SUCCESS;

  /* at least one rank is missing files, attempt to rebuild them */
  int build_command_count = 0;

  /* step through each of our xor sets */
  kvtree_elem* xor_elem = NULL;
  kvtree* xors_hash = kvtree_get(dset_hash, type_key);
  for (xor_elem = kvtree_elem_first(xors_hash);
       xor_elem != NULL;
       xor_elem = kvtree_elem_next(xor_elem))
  {
    /* get the set id and the hash for this xor set */
    int xor_setid = kvtree_elem_key_int(xor_elem);
    kvtree* xor_hash = kvtree_elem_hash(xor_elem);

    /* TODO: Check that there is only one members value */

    /* get the number of members in this set */
    int members;
    if (kvtree_util_get_int(xor_hash, SCR_SCAN_KEY_MEMBERS, &members) != KVTREE_SUCCESS) {
      /* unknown number of members in this set, skip this set */
      scr_err("Unknown number of members in XOR set %d in dataset %d @ %s:%d",
        xor_setid, dset_id, __FILE__, __LINE__
      );
      rc = SCR_FAILURE;
      continue;
    }

    /* if we don't have all members, add rebuild command if we can */
    kvtree* members_hash = kvtree_get(xor_hash, SCR_SCAN_KEY_MEMBER);
    int members_have = kvtree_size(members_hash);
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
      kvtree* member_hash = kvtree_get_kv_int(xor_hash, SCR_SCAN_KEY_MEMBER, member);
      if (member_hash == NULL) {
        /* we're missing the XOR file for this member */
        missing_member = member;
        missing_count++;
      } else {
        /* get the rank this member corresponds to */
        char* rank_str;
        if (kvtree_util_get_str(member_hash, SCR_SUMMARY_6_KEY_RANK, &rank_str) == KVTREE_SUCCESS) {
          /* check whether we're missing any files for this rank */
          kvtree* missing_rank_hash = kvtree_get(missing_hash, rank_str);
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
      kvtree_set_kv_int(dset_hash, SCR_SCAN_KEY_UNRECOVERABLE, xor_setid);
    } else if (missing_count > 0) {
      kvtree* buildcmd_hash = kvtree_set_kv_int(dset_hash, SCR_SCAN_KEY_BUILD, build_command_count);
      build_command_count++;

      int argc = 0;

      /* write the command name */
      kvtree_setf(buildcmd_hash, NULL, "%d %s", argc, BUILD_XOR_CMD);
      argc++;

      /* command to rebuild data files from xor files */
      kvtree_setf(buildcmd_hash, NULL, "%d %s", argc, type_cmd);
      argc++;

      /* write the number of members in the xor set */
      kvtree_setf(buildcmd_hash, NULL, "%d %d", argc, members);
      argc++;

      /* write the index of the missing xor file (convert from 1-based index to 0-based index) */
      kvtree_setf(buildcmd_hash, NULL, "%d %d", argc, missing_member - 1);
      argc++;

      /* write each of the existing xor file names, skipping the missing member */
      for (member = 1; member <= members; member++) {
        if (member == missing_member) {
          continue;
        }
        kvtree* member_hash = kvtree_get_kv_int(xor_hash, SCR_SCAN_KEY_MEMBER, member);
        char* filename = kvtree_elem_get_first_val(member_hash, SCR_SUMMARY_6_KEY_FILE);
        kvtree_setf(buildcmd_hash, NULL, "%d %s", argc, filename);
        argc++;
      }
    }
  }

  /* rebuild if we can */
  kvtree* unrecoverable = kvtree_get(dset_hash, SCR_SCAN_KEY_UNRECOVERABLE);
  char* dir_str = spath_strdup(dir);
  if (unrecoverable != NULL) {
    /* at least some files cannot be recovered */
    scr_err("Insufficient files to attempt rebuild of dataset %d in %s @ %s:%d",
      dset_id, dir_str, __FILE__, __LINE__
    );
    rc = SCR_FAILURE;
  } else {
    /* we have a shot to rebuild everything, let's give it a go */
    kvtree* builds_hash = kvtree_get(dset_hash, SCR_SCAN_KEY_BUILD);
    if (scr_fork_rebuilds(dir, BUILD_XOR_CMD, builds_hash) != SCR_SUCCESS) {
      scr_err("At least one rebuild failed for dataset %d in %s @ %s:%d",
        dset_id, dir_str, __FILE__, __LINE__
      );
      rc = SCR_FAILURE;
    }
  }
  scr_free(&dir_str);

  return rc;
}

/* returns SCR_FAILURE if any dataset failed to rebuild, SCR_SUCCESS otherwise */
int scr_rebuild_scan(const spath* prefix, const spath* dir, kvtree* scan)
{
  /* assume we'll be successful */
  int rc = SCR_SUCCESS;

  /* step through and check each of our datasets */
  kvtree_elem* dset_elem = NULL;
  kvtree* dsets_hash = kvtree_get(scan, SCR_SCAN_KEY_DLIST);
  for (dset_elem = kvtree_elem_first(dsets_hash);
       dset_elem != NULL;
       dset_elem = kvtree_elem_next(dset_elem))
  {
    /* get id and the hash for this dataset */
    int dset_id = kvtree_elem_key_int(dset_elem);
    kvtree* dset_hash = kvtree_elem_hash(dset_elem);

    /* if the dataset is marked as inconsistent -- consider it to be beyond repair */
    kvtree* invalid = kvtree_get(dset_hash, SCR_SCAN_KEY_INVALID);
    if (invalid != NULL) {
      rc = SCR_FAILURE;
      continue;
    }

    /* check whether there are any missing files in this dataset */
    kvtree* missing_hash = kvtree_get(dset_hash, SCR_SCAN_KEY_MISSING);
    if (missing_hash != NULL) {
      /* need to rebuild some files, determine the encoding type
       * and call corresponding function to define rebuild command */

      /* rebuild filemap files with PARTNER */
      kvtree* mappartner_hash = kvtree_get(dset_hash, SCR_SCAN_KEY_MAPPARTNER);
      if (mappartner_hash != NULL) {
        int tmp_rc = scr_rebuild_partner(prefix, dir, dset_id, dset_hash, missing_hash, SCR_SCAN_KEY_MAPPARTNER, "map");
        if (tmp_rc != SCR_SUCCESS) {
          rc = SCR_FAILURE;
        }
      }

      /* rebuild filemap files with XOR */
      kvtree* mapxor_hash = kvtree_get(dset_hash, SCR_SCAN_KEY_MAPXOR);
      if (mapxor_hash != NULL) {
        int tmp_rc = scr_rebuild_xor(prefix, dir, dset_id, dset_hash, missing_hash, SCR_SCAN_KEY_MAPXOR, "map");
        if (tmp_rc != SCR_SUCCESS) {
          rc = SCR_FAILURE;
        }
      }

      /* rebuild data files with PARTNER */
      kvtree* partner_hash = kvtree_get(dset_hash, SCR_SCAN_KEY_PARTNER);
      if (partner_hash != NULL) {
        int tmp_rc = scr_rebuild_partner(prefix, dir, dset_id, dset_hash, missing_hash, SCR_SCAN_KEY_PARTNER, "partner");
        if (tmp_rc != SCR_SUCCESS) {
          rc = SCR_FAILURE;
        }
      }

      /* rebuild data files with XOR */
      kvtree* xor_hash = kvtree_get(dset_hash, SCR_SCAN_KEY_XOR);
      if (xor_hash != NULL) {
        int tmp_rc = scr_rebuild_xor(prefix, dir, dset_id, dset_hash, missing_hash, SCR_SCAN_KEY_XOR, "xor");
        if (tmp_rc != SCR_SUCCESS) {
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
int scr_inspect_scan(kvtree* scan)
{
  /* assume nothing is missing, we'll set this to 1 if we find anything that is */
  int any_missing = 0;

  /* look for missing files for each dataset */
  kvtree_elem* dset_elem = NULL;
  kvtree* dsets = kvtree_get(scan, SCR_SCAN_KEY_DLIST);
  for (dset_elem = kvtree_elem_first(dsets);
       dset_elem != NULL;
       dset_elem = kvtree_elem_next(dset_elem))
  {
    /* get the dataset id */
    int dset_id = kvtree_elem_key_int(dset_elem);

    /* get the dataset hash */
    kvtree* dset_hash = kvtree_elem_hash(dset_elem);

    /* get the hash for the RANKS key */
    kvtree* rank2file_hash   = kvtree_get(dset_hash, SCR_SUMMARY_6_KEY_RANK2FILE);
    kvtree* ranks_count_hash = kvtree_get(rank2file_hash, SCR_SUMMARY_6_KEY_RANKS);

    /* check that this dataset has only one value under the RANKS key */
    int ranks_size = kvtree_size(ranks_count_hash);
    if (ranks_size != 1) {
      /* found more than one RANKS value, mark it as inconsistent */
      any_missing = 1;
      kvtree_set_kv_int(dset_hash, SCR_SCAN_KEY_INVALID, 1);
      scr_err("Dataset %d has more than one value for the number of ranks @ %s:%d",
        dset_id, __FILE__, __LINE__
      );
      continue;
    }

    /* lookup the number of ranks */
    int ranks;
    kvtree_util_get_int(rank2file_hash, SCR_SUMMARY_6_KEY_RANKS, &ranks);

    /* assume this dataset is valid */
    int dataset_valid = 1;

    /* get the ranks hash and sort it by rank id */
    kvtree* ranks_hash = kvtree_get(rank2file_hash, SCR_SUMMARY_6_KEY_RANK);
    kvtree_sort_int(ranks_hash, KVTREE_SORT_ASCENDING);

    /* for each rank, check that we have each of its files */
    int expected_rank = 0;
    kvtree_elem* rank_elem = NULL;
    for (rank_elem = kvtree_elem_first(ranks_hash);
         rank_elem != NULL;
         rank_elem = kvtree_elem_next(rank_elem))
    {
      /* get the rank */
      int rank_id = kvtree_elem_key_int(rank_elem);

      /* get the hash for this rank */
      kvtree* rank_hash = kvtree_elem_hash(rank_elem);

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
        kvtree_set_kv_int(dset_hash, SCR_SCAN_KEY_MISSING, expected_rank);
        expected_rank++;
      }

      /* get the hash for the FILES key */
      kvtree* files_count_hash = kvtree_get(rank_hash, SCR_SUMMARY_6_KEY_FILES);

      /* check that this dataset has only one value for the FILES key */
      int files_size = kvtree_size(files_count_hash);
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
      int files;
      kvtree_util_get_int(rank_hash, SCR_SUMMARY_6_KEY_FILES, &files);

      /* get the files hash for this rank */
      kvtree* files_hash = kvtree_get(rank_hash, SCR_SUMMARY_6_KEY_FILE);

      /* check that each file is marked as complete */
      int file_count = 0;
      kvtree_elem* file_elem = NULL;
      for (file_elem = kvtree_elem_first(files_hash);
           file_elem != NULL;
           file_elem = kvtree_elem_next(file_elem))
      {
        /* get the file hash */
        kvtree* file_hash = kvtree_elem_hash(file_elem);

        /* check that the file is not marked as incomplete */
        int complete;
        if (kvtree_util_get_int(file_hash, SCR_SUMMARY_6_KEY_COMPLETE, &complete) == KVTREE_SUCCESS) {
          if (complete == 0) {
            /* file is explicitly marked as incomplete, add the rank to the missing list */
            kvtree_set_kv_int(dset_hash, SCR_SCAN_KEY_MISSING, rank_id);
          }
        }

        file_count++;
      }

      /* if we're missing any files, mark this rank as missing */
      if (file_count < files) {
        kvtree_set_kv_int(dset_hash, SCR_SCAN_KEY_MISSING, rank_id);
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
      kvtree_set_kv_int(dset_hash, SCR_SCAN_KEY_MISSING, expected_rank);
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
      kvtree_setf(dset_hash, NULL, "%s", SCR_SCAN_KEY_INVALID);
    }

    /* check whether we have any missing files for this dataset */
    kvtree* missing_hash = kvtree_get(dset_hash, SCR_SCAN_KEY_MISSING);
    if (missing_hash != NULL) {
      any_missing = 1;
    }

    /* if dataset is not marked invalid, and if there are no missing files, then mark it as complete */
    if (dataset_valid && missing_hash == NULL) {
      kvtree_set_kv_int(dset_hash, SCR_SUMMARY_6_KEY_COMPLETE, 1);
    }
  }

  if (any_missing) {
    return SCR_FAILURE;
  }
  return SCR_SUCCESS;
}

int scr_scan_flush(const spath* path_prefix, int dset_id, kvtree* scan)
{
  int rc = 0;

  /* lookup scan hash for this dataset id */
  kvtree* list_hash = kvtree_set_kv_int(scan, SCR_SCAN_KEY_DLIST, dset_id);

  /* read flush file from the .scr directory */
  kvtree* flush = kvtree_new();
  spath* flush_path = spath_dup(path_prefix);
  spath_append_str(flush_path, ".scr");
  spath_append_str(flush_path, "flush.scr");
  const char* flush_file = spath_strdup(flush_path);

  if (kvtree_read_file(flush_file, flush) == KVTREE_SUCCESS) {
    /* copy dataset kvtree from flush file data */
    kvtree* dataset = kvtree_new();
    kvtree* hash = kvtree_get_kv_int(flush, SCR_FLUSH_KEY_DATASET, dset_id);
    kvtree* dataset_hash = kvtree_get(hash, SCR_FLUSH_KEY_DSETDESC);
    kvtree_merge(dataset, dataset_hash);
    kvtree_set(list_hash, SCR_SUMMARY_6_KEY_DATASET, dataset);
  } else {
    /* failed to read flush file, so missing dataset info */
    rc = 1;
  }

  scr_free(&flush_file);
  spath_delete(&flush_path);
  kvtree_delete(&flush);

  return rc;
}

/* Reads fmap files from given dataset directory and adds them to scan hash.
 * Returns SCR_SUCCESS if the files could be scanned */
int scr_scan_filemap(const spath* path_prefix, const spath* path_filemap, int dset_id, int rank_id, int* ranks, kvtree* scan)
{
  /* create an empty filemap to store contents */
  scr_filemap* rank_map = scr_filemap_new();

  /* read in the filemap */
  if (scr_filemap_read(path_filemap, rank_map) != SCR_SUCCESS) {
    char* path_err = spath_strdup(path_filemap);
    scr_err("Error reading filemap: %s @ %s:%d",
      path_err, __FILE__, __LINE__
    );
    scr_free(&path_err);
    scr_filemap_delete(&rank_map);
    return SCR_FAILURE;
  }

  /* lookup scan hash for this dataset id */
  kvtree* list_hash = kvtree_set_kv_int(scan, SCR_SCAN_KEY_DLIST, dset_id);

  /* lookup rank2file hash for this dataset, allocate a new one if it's not found */
  kvtree* rank2file_hash = kvtree_get(list_hash, SCR_SUMMARY_6_KEY_RANK2FILE);
  if (rank2file_hash == NULL) {
    /* there is no existing rank2file hash, create a new one and add it */
    rank2file_hash = kvtree_new();
    kvtree_set(list_hash, SCR_SUMMARY_6_KEY_RANK2FILE, rank2file_hash);
  }

#if 0
  /* read dataset hash from filemap and record in summary */
  scr_dataset* rank_dset = scr_dataset_new();
  scr_filemap_get_dataset(rank_map, dset_id, rank_id, rank_dset);
  scr_dataset* current_dset = kvtree_get(list_hash, SCR_SUMMARY_6_KEY_DATASET);
  if (current_dset == NULL ) {
    /* there is no dataset hash currently assigned, so use the one for the current rank */
    kvtree_set(list_hash, SCR_SUMMARY_6_KEY_DATASET, rank_dset);
  } else {
    /* TODODSET */
    /* check that the dataset for this rank matches the one we already have */
    /* if rank_dset != current_dset, then problem */
    scr_dataset_delete(&rank_dset);
  }
#endif

  /* lookup rank hash for this rank */
  kvtree* rank_hash = kvtree_set_kv_int(rank2file_hash, SCR_SUMMARY_6_KEY_RANK, rank_id);

  /* set number of expected files for this rank */
  int num_expect = scr_filemap_num_files(rank_map);
  kvtree_set_kv_int(rank_hash, SCR_SUMMARY_6_KEY_FILES, num_expect);

  /* TODO: check that we have each named file for this rank */
  kvtree_elem* file_elem = NULL;
  for (file_elem = scr_filemap_first_file(rank_map);
       file_elem != NULL;
       file_elem = kvtree_elem_next(file_elem))
  {
    /* get the file name (relative to dir) */
    char* cache_file_name = kvtree_elem_key(file_elem);

    /* get meta data for this file */
    scr_meta* meta = scr_meta_new();
    if (scr_filemap_get_meta(rank_map, cache_file_name, meta) != SCR_SUCCESS) {
      scr_err("Failed to read meta data for %s from dataset %d @ %s:%d",
        cache_file_name, dset_id, __FILE__, __LINE__
      );
      continue;
    }

    /* get path to file build the full file name */
    char* meta_path;
    if (scr_meta_get_origpath(meta, &meta_path) != SCR_SUCCESS) {
      scr_err("Reading path from meta data from %s @ %s:%d",
        cache_file_name, __FILE__, __LINE__
      );
      scr_meta_delete(&meta);
      continue;
    }

    /* get name of file */
    char* meta_name;
    if (scr_meta_get_origname(meta, &meta_name) != SCR_SUCCESS) {
      scr_err("Reading path from meta data from %s @ %s:%d",
        cache_file_name, __FILE__, __LINE__
      );
      scr_meta_delete(&meta);
      continue;
    }

    /* build the full file name */
    spath* path_full_filename = spath_from_str(meta_path);
    spath_append_str(path_full_filename, meta_name);
    char* full_filename = spath_strdup(path_full_filename);

    /* compute path to file relative to prefix (for rank2file) */
    spath* path_relative = spath_relative(path_prefix, path_full_filename);
    char* relative_filename = spath_strdup(path_relative);
    spath_delete(&path_relative);

    spath_delete(&path_full_filename);

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
        full_filename, __FILE__, __LINE__
      );
      scr_meta_delete(&meta);
      scr_free(&full_filename);
      continue;
    }
#endif

    /* read the ranks from the meta data */
    int meta_ranks = -1;
    if (scr_meta_get_ranks(meta, &meta_ranks) != SCR_SUCCESS) {
      scr_err("Reading ranks from meta data from %s @ %s:%d",
        full_filename, __FILE__, __LINE__
      );
      scr_meta_delete(&meta);
      scr_free(&relative_filename);
      scr_free(&full_filename);
      continue;
    }

    /* read filesize from meta data */
    unsigned long meta_filesize = 0;
    if (scr_meta_get_filesize(meta, &meta_filesize) != SCR_SUCCESS) {
      scr_err("Reading filesize from meta data from %s @ %s:%d",
        full_filename, __FILE__, __LINE__
      );
      scr_meta_delete(&meta);
      scr_free(&relative_filename);
      scr_free(&full_filename);
      continue;
    }

    /* set our ranks if it's not been set */
    if (*ranks == -1) {
      *ranks = meta_ranks;
    }

    /* TODO: need to check directories on all of these file names */

    /* check that the file is complete */
    if (scr_meta_is_complete(meta) != SCR_SUCCESS) {
      scr_err("File is not complete: %s @ %s:%d",
        full_filename, __FILE__, __LINE__
      );
      scr_meta_delete(&meta);
      scr_free(&relative_filename);
      scr_free(&full_filename);
      continue;
    }

    /* check that the file exists */
    if (scr_file_exists(full_filename) != SCR_SUCCESS) {
      scr_err("File does not exist: %s @ %s:%d",
        full_filename, __FILE__, __LINE__
      );
      scr_meta_delete(&meta);
      scr_free(&relative_filename);
      scr_free(&full_filename);
      continue;
    }

    /* check that the file size matches */
    unsigned long size = scr_file_size(full_filename);
    if (meta_filesize != size) {
      scr_err("File is %lu bytes but expected to be %lu bytes: %s @ %s:%d",
        size, meta_filesize, full_filename, __FILE__, __LINE__
      );
      scr_meta_delete(&meta);
      scr_free(&relative_filename);
      scr_free(&full_filename);
      continue;
    }

    /* check that the ranks match */
    if (meta_ranks != *ranks) {
      scr_err("File was created with %d ranks, but expected %d ranks: %s @ %s:%d",
        meta_ranks, *ranks, full_filename, __FILE__, __LINE__
      );
      scr_meta_delete(&meta);
      scr_free(&relative_filename);
      scr_free(&full_filename);
      continue;
    }

    /* DLIST
     *   <dataset_id>
     *     MAP
     *       <full_path_to_file_in_cache>
     *         <full_path_to_file_in_prefix>
     *     RANK2FILE
     *       RANKS
     *         <num_ranks>
     *       RANK
     *         <rank>
     *           FILE
     *             <filename_relative_to_prefix>
     *               SIZE
     *                 <filesize>
     *               CRC
     *                 <crc> */
    /* TODODSET: rank2file_hash may not exist yet */
    kvtree* list_hash = kvtree_set_kv_int(scan, SCR_SCAN_KEY_DLIST, dset_id);
    //kvtree_setf(list_hash, NULL, "%s %s %s", SCR_SCAN_KEY_MAP, cache_file_name, full_filename);
    kvtree* rank2file_hash = kvtree_get(list_hash, SCR_SUMMARY_6_KEY_RANK2FILE);
    kvtree_set_kv_int(rank2file_hash, SCR_SUMMARY_6_KEY_RANKS, meta_ranks);
    kvtree* rank_hash = kvtree_set_kv_int(rank2file_hash, SCR_SUMMARY_6_KEY_RANK, rank_id);
    kvtree* file_hash = kvtree_set_kv(rank_hash, SCR_SUMMARY_6_KEY_FILE, relative_filename);
    //kvtree_util_set_bytecount(file_hash, SCR_SUMMARY_6_KEY_SIZE, meta_filesize);

    uLong meta_crc;
    if (scr_meta_get_crc32(meta, &meta_crc) == SCR_SUCCESS) {
    //  kvtree_util_set_crc32(file_hash, SCR_SUMMARY_6_KEY_CRC, meta_crc);
    }

    scr_meta_delete(&meta);
    scr_free(&relative_filename);
    scr_free(&full_filename);
  }

  /* delete the filemap */
  scr_filemap_delete(&rank_map);

  return SCR_SUCCESS;
}

int scr_scan_partner(const char* file_name, int dset_id, int lhs_rank, int rank, int rhs_rank, kvtree* scan)
{
  int rc = SCR_SUCCESS;

  /* lookup scan hash for this dataset id */
  kvtree* list_hash = kvtree_set_kv_int(scan, SCR_SCAN_KEY_DLIST, dset_id);

  /* add the XOR file entries into our scan hash */
  if (lhs_rank != -1 && rank != -1 && rhs_rank != -1) {
    /* DLIST
     *   <dataset_id>
     *     PARTNER
     *       <lhs_rank>
     *         RIGHT
     *           FILE
     *             <filename>
     *           RANK
     *             <rank>
     *       <rhs_rank>
     *         LEFT
     *           FILE
     *             <filename>
     *           RANK
     *             <rank> */

    /* this rank serves as the RIGHT partner for the lhs_rank */
    kvtree* hash = kvtree_new();
    kvtree_set_kv(hash, SCR_SUMMARY_6_KEY_FILE, file_name);
    kvtree_set_kv_int(hash, SCR_SUMMARY_6_KEY_RANK, rank);
    kvtree* partner_hash = kvtree_set_kv_int(list_hash, SCR_SCAN_KEY_PARTNER, lhs_rank);
    kvtree_set(partner_hash, SCR_SCAN_KEY_RIGHT, hash);

    /* this rank serves as the LEFT partner for the rhs_rank */
    hash = kvtree_new();
    kvtree_set_kv(hash, SCR_SUMMARY_6_KEY_FILE, file_name);
    kvtree_set_kv_int(hash, SCR_SUMMARY_6_KEY_RANK, rank);
    partner_hash = kvtree_set_kv_int(list_hash, SCR_SCAN_KEY_PARTNER, rhs_rank);
    kvtree_set(partner_hash, SCR_SCAN_KEY_LEFT, hash);
  } else {
    scr_err("Failed to extract XOR rank, set size, or set id from %s @ %s:%d",
      file_name, __FILE__, __LINE__
    );
    rc = SCR_FAILURE;
  }
  
  return rc;
}

int scr_scan_mappartner(const char* file_name, int dset_id, int lhs_rank, int rank, int rhs_rank, kvtree* scan)
{
  int rc = SCR_SUCCESS;

  /* lookup scan hash for this dataset id */
  kvtree* list_hash = kvtree_set_kv_int(scan, SCR_SCAN_KEY_DLIST, dset_id);

  /* add the XOR file entries into our scan hash */
  if (lhs_rank != -1 && rank != -1 && rhs_rank != -1) {
    /* DLIST
     *   <dataset_id>
     *     MAPPARTNER
     *       <lhs_rank>
     *         RIGHT
     *           FILE
     *             <filename>
     *           RANK
     *             <rank>
     *       <rhs_rank>
     *         LEFT
     *           FILE
     *             <filename>
     *           RANK
     *             <rank> */

    /* this rank serves as the RIGHT partner for the lhs_rank */
    kvtree* hash = kvtree_new();
    kvtree_set_kv(hash, SCR_SUMMARY_6_KEY_FILE, file_name);
    kvtree_set_kv_int(hash, SCR_SUMMARY_6_KEY_RANK, rank);
    kvtree* partner_hash = kvtree_set_kv_int(list_hash, SCR_SCAN_KEY_MAPPARTNER, lhs_rank);
    kvtree_set(partner_hash, SCR_SCAN_KEY_RIGHT, hash);

    /* this rank serves as the LEFT partner for the rhs_rank */
    hash = kvtree_new();
    kvtree_set_kv(hash, SCR_SUMMARY_6_KEY_FILE, file_name);
    kvtree_set_kv_int(hash, SCR_SUMMARY_6_KEY_RANK, rank);
    partner_hash = kvtree_set_kv_int(list_hash, SCR_SCAN_KEY_MAPPARTNER, rhs_rank);
    kvtree_set(partner_hash, SCR_SCAN_KEY_LEFT, hash);
  } else {
    scr_err("Failed to extract XOR rank, set size, or set id from %s @ %s:%d",
      file_name, __FILE__, __LINE__
    );
    rc = SCR_FAILURE;
  }
  
  return rc;
}

int scr_scan_xor(const char* file_name, int dset_id, int rank, int group_id, int group_rank, int group_size, kvtree* scan)
{
  int rc = SCR_SUCCESS;

  /* lookup scan hash for this dataset id */
  kvtree* list_hash = kvtree_set_kv_int(scan, SCR_SCAN_KEY_DLIST, dset_id);

  /* add the XOR file entries into our scan hash */
  if (group_rank != -1 && group_size != -1 && group_id != -1) {
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
    kvtree* xor_hash = kvtree_set_kv_int(list_hash, SCR_SCAN_KEY_XOR, group_id);
    kvtree_set_kv_int(xor_hash, SCR_SCAN_KEY_MEMBERS, group_size);
    kvtree* xor_rank_hash = kvtree_set_kv_int(xor_hash, SCR_SCAN_KEY_MEMBER, group_rank);
    kvtree_set_kv(xor_rank_hash, SCR_SUMMARY_6_KEY_FILE, file_name);
    kvtree_set_kv_int(xor_rank_hash, SCR_SUMMARY_6_KEY_RANK, rank);
  } else {
    scr_err("Failed to extract XOR rank, set size, or set id from %s @ %s:%d",
      file_name, __FILE__, __LINE__
    );
    rc = SCR_FAILURE;
  }
  
  return rc;
}

int scr_scan_mapxor(const char* file_name, int dset_id, int rank, int group_id, int group_rank, int group_size, kvtree* scan)
{
  int rc = SCR_SUCCESS;

  /* lookup scan hash for this dataset id */
  kvtree* list_hash = kvtree_set_kv_int(scan, SCR_SCAN_KEY_DLIST, dset_id);

  /* add the XOR file entries into our scan hash */
  if (group_rank != -1 && group_size != -1 && group_id != -1) {
    /* DLIST
     *   <dataset_id>
     *     MAPXOR
     *       <xor_setid>
     *         MEMBERS
     *           <num_members_in_xor_set>
     *         MEMBER
     *           <xor_set_member_id>
     *             FILE
     *               <filename>
     *             RANK
     *               <rank_id> */
    kvtree* xor_hash = kvtree_set_kv_int(list_hash, SCR_SCAN_KEY_MAPXOR, group_id);
    kvtree_set_kv_int(xor_hash, SCR_SCAN_KEY_MEMBERS, group_size);
    kvtree* xor_rank_hash = kvtree_set_kv_int(xor_hash, SCR_SCAN_KEY_MEMBER, group_rank);
    kvtree_set_kv(xor_rank_hash, SCR_SUMMARY_6_KEY_FILE, file_name);
    kvtree_set_kv_int(xor_rank_hash, SCR_SUMMARY_6_KEY_RANK, rank);
  } else {
    scr_err("Failed to extract XOR rank, set size, or set id from %s @ %s:%d",
      file_name, __FILE__, __LINE__
    );
    rc = SCR_FAILURE;
  }
  
  return rc;
}

/* returns 1 if name matches "filemap_<rank>" regex and extracts rank value,
 * returns 0 if no match */
int match_filemap(const char* name, const regex_t* re, int* rank)
{
  /* assume we don't match */
  int rc = 0;

  /* check whether the regex matches */
  size_t nmatch = 2;
  regmatch_t pmatch[nmatch];
  if (regexec(re, name, nmatch, pmatch, 0) == 0) {
    /* made a match, now extract the rank value */
    char* value = strndup(name + pmatch[1].rm_so, (size_t)(pmatch[1].rm_eo - pmatch[1].rm_so));
    if (value != NULL) {
      /* got the rank as a string */
      rc = 1;
      *rank = atoi(value);
      scr_free(&value);
    }
  }

  return rc;
}

/* returns 1 if name matches partner regex and extracts rank and partner rank,
 * returns 0 if no match */
int match_partner(const char* name, const regex_t* re, int* lhs_rank, int* rank, int* rhs_rank)
{
  /* assume we don't match */
  int rc = 0;

  /* if the file is an partner file, read in the partner parameters */
  size_t nmatch = 5;
  regmatch_t pmatch[nmatch];
  if (regexec(re, name, nmatch, pmatch, 0) == 0) {
    /* got a match */
    rc = 1;

    /* intialize output variables */
    *lhs_rank = -1;
    *rank     = -1;
    *rhs_rank = -1;

    /* get the rank of our left partner */
    char* value = strndup(name + pmatch[2].rm_so, (size_t)(pmatch[2].rm_eo - pmatch[2].rm_so));
    if (value != NULL) {
      *lhs_rank = atoi(value);
      scr_free(&value);
    }

    /* get the rank */
    value = strndup(name + pmatch[3].rm_so, (size_t)(pmatch[3].rm_eo - pmatch[3].rm_so));
    if (value != NULL) {
      *rank = atoi(value);
      scr_free(&value);
    }

    /* get the rank of our right partner */
    value = strndup(name + pmatch[4].rm_so, (size_t)(pmatch[4].rm_eo - pmatch[4].rm_so));
    if (value != NULL) {
      *rhs_rank = atoi(value);
      scr_free(&value);
    }
  }

  return rc;
}

/* returns 1 if name matches XOR regex and extracts rank, group, group size, and group rank,
 * returns 0 if no match */
int match_xor(const char* name, const regex_t* re, int* rank, int* group_id, int* group_rank, int* group_size)
{
  /* assume we don't match */
  int rc = 0;

  /* if the file is an XOR file, read in the XOR set parameters */
  size_t nmatch = 5;
  regmatch_t pmatch[nmatch];
  if (regexec(re, name, nmatch, pmatch, 0) == 0) {
    /* got a match */
    rc = 1;

    /* intialize output variables */
    *rank       = -1;
    *group_id   = -1;
    *group_rank = -1;
    *group_size = -1;

    /* get the rank of the xor set */
    char* value = strndup(name + pmatch[1].rm_so, (size_t)(pmatch[1].rm_eo - pmatch[1].rm_so));
    if (value != NULL) {
      *rank = atoi(value);
      scr_free(&value);
    }

    /* get the id of the xor set */
    value = strndup(name + pmatch[2].rm_so, (size_t)(pmatch[2].rm_eo - pmatch[2].rm_so));
    if (value != NULL) {
      *group_id = atoi(value);
      scr_free(&value);
    }

    /* get the rank in the xor set */
    value = strndup(name + pmatch[3].rm_so, (size_t)(pmatch[3].rm_eo - pmatch[3].rm_so));
    if (value != NULL) {
      *group_rank = atoi(value);
      scr_free(&value);
    }

    /* get the size of the xor set */
    value = strndup(name + pmatch[4].rm_so, (size_t)(pmatch[4].rm_eo - pmatch[4].rm_so));
    if (value != NULL) {
      *group_size = atoi(value);
      scr_free(&value);
    }
  }

  return rc;
}

/* Reads fmap files from given dataset directory and adds them to scan hash.
 * Returns SCR_SUCCESS if the files could be scanned */
int scr_scan_files(const spath* prefix, const spath* dir, int dset_id, kvtree* scan)
{
  int rc = SCR_SUCCESS;

  /* get dataset info from flush file */
  scr_scan_flush(prefix, dset_id, scan);

  /* create path to scr subdirectory */
  spath* meta_path = spath_dup(dir);

  /* allocate directory in string form */
  char* dir_str = spath_strdup(meta_path);

  /* regex to identify filemap files and extract rank from filename */
  regex_t re_filemap_file;
  regcomp(&re_filemap_file, "filemap_([0-9]+)", REG_EXTENDED);

  /* set up a regular expression so we can extract the partner set information from a file */
  regex_t re_partner_file;
  regcomp(&re_partner_file, "reddesc.er.([0-9]+).partner.([0-9]+)_([0-9]+)_([0-9]+).redset", REG_EXTENDED);

  /* set up a regular expression so we can extract the partner set information for filemaps */
  regex_t re_mappartner_file;
  regcomp(&re_mappartner_file, "reddescmap.er.([0-9]+).partner.([0-9]+)_([0-9]+)_([0-9]+).redset", REG_EXTENDED);

  /* set up a regular expression so we can extract the xor set information from a file */
  regex_t re_xor_file;
  regcomp(&re_xor_file, "reddesc.er.([0-9]+).xor.([0-9]+)_([0-9]+)_of_([0-9]+).redset", REG_EXTENDED);

  /* set up a regular expression so we can extract the xor set information for filemaps */
  regex_t re_mapxor_file;
  regcomp(&re_mapxor_file, "reddescmap.er.([0-9]+).xor.([0-9]+)_([0-9]+)_of_([0-9]+).redset", REG_EXTENDED);

  /* function to track ranks value across a set of files to be scanned */
  int ranks = -1;

  /* open the directory */
  DIR* dirp = opendir(dir_str);
  if (dirp == NULL) {
    scr_err("Failed to open directory %s (errno=%d %s) @ %s:%d",
      dir_str, errno, strerror(errno), __FILE__, __LINE__
    );
    rc = SCR_FAILURE;
    goto cleanup;
  }

  /* read each file from the directory */
  struct dirent* dp = NULL;
  int rank, lhs_rank, rhs_rank, group_id, group_rank, group_size;
  do {
    errno = 0;
    dp = readdir(dirp);
    if (dp != NULL) {
      const char* name = NULL;

      #ifdef _DIRENT_HAVE_D_TYPE
        /* distinguish between directories and files if we can */
        if (dp->d_type != DT_DIR) {
          name = dp->d_name;
        }
      #else
        /* TODO: throw a compile error here instead? */
        name = dp->d_name;
      #endif

      /* we only process fmap files */
      if (name != NULL) {
        if (match_filemap(name, &re_filemap_file, &rank)) {
          /* create a full path of the file name */
          spath* filemap_path = spath_dup(meta_path);
          spath_append_str(filemap_path, name);

          /* read file contents into our scan hash */
          int tmp_rc = scr_scan_filemap(prefix, filemap_path, dset_id, rank, &ranks, scan);
          if (tmp_rc != SCR_SUCCESS) {
            rc = tmp_rc;
            spath_delete(&filemap_path);
            break;
          }

          /* delete the path */
          spath_delete(&filemap_path);
        } else if (match_partner(name, &re_partner_file, &lhs_rank, &rank, &rhs_rank)) {
          /* add info for XOR file to our scan hash */
          int tmp_rc = scr_scan_partner(name, dset_id, lhs_rank, rank, rhs_rank, scan);
          if (tmp_rc != SCR_SUCCESS) {
            rc = tmp_rc;
            break;
          }
        } else if (match_partner(name, &re_mappartner_file, &lhs_rank, &rank, &rhs_rank)) {
          /* add info for XOR file to our scan hash */
          int tmp_rc = scr_scan_mappartner(name, dset_id, lhs_rank, rank, rhs_rank, scan);
          if (tmp_rc != SCR_SUCCESS) {
            rc = tmp_rc;
            break;
          }
        } else if (match_xor(name, &re_xor_file, &rank, &group_id, &group_rank, &group_size)) {
          /* add info for XOR file to our scan hash */
          int tmp_rc = scr_scan_xor(name, dset_id, rank, group_id, group_rank, group_size, scan);
          if (tmp_rc != SCR_SUCCESS) {
            rc = tmp_rc;
            break;
          }
        } else if (match_xor(name, &re_mapxor_file, &rank, &group_id, &group_rank, &group_size)) {
          /* add info for XOR file to our scan hash */
          int tmp_rc = scr_scan_mapxor(name, dset_id, rank, group_id, group_rank, group_size, scan);
          if (tmp_rc != SCR_SUCCESS) {
            rc = tmp_rc;
            break;
          }
        }
      }
    } else {
      if (errno != 0) {
        scr_err("Failed to read directory %s (errno=%d %s) @ %s:%d",
          dir_str, errno, strerror(errno), __FILE__, __LINE__
        );
        rc = SCR_FAILURE;
        break;
      }
    }
  } while (dp != NULL);

  /* close the directory */
  if (closedir(dirp) < 0) {
    scr_err("Failed to close directory %s (errno=%d %s) @ %s:%d",
      dir_str, errno, strerror(errno), __FILE__, __LINE__
    );
    rc = SCR_FAILURE;
  }

cleanup:
  /* free the xor regular expression */
  regfree(&re_filemap_file);
  regfree(&re_partner_file);
  regfree(&re_mappartner_file);
  regfree(&re_xor_file);
  regfree(&re_mapxor_file);

  /* free our directory string */
  scr_free(&dir_str);

  /* delete scr path */
  spath_delete(&meta_path);

  return rc;
}

/* builds and writes the summary file for the given dataset metadata directory
 * Returns SCR_SUCCESS if the summary file exists or was written,
 * but this does not imply the dataset is valid, only that the summary
 * file was written */
int scr_summary_build(const spath* prefix, const spath* dir, int id)
{
  int rc = SCR_SUCCESS;

  /* create a new hash to store our index file data */
  kvtree* summary = kvtree_new();

  if (scr_summary_read(dir, summary) != SCR_SUCCESS) {
    /* now only return success if we successfully write the file */
    int rc = SCR_FAILURE;

    /* create a new hash to store our scan results */
    kvtree* scan = kvtree_new();

    /* scan the files in the given directory */
    scr_scan_files(prefix, dir, id, scan);

    /* determine whether we are missing any files */
    if (scr_inspect_scan(scan) != SCR_SUCCESS) {
      /* missing some files, see if we can rebuild them */
      if (scr_rebuild_scan(prefix, dir, scan) == SCR_SUCCESS) {
        /* the rebuild succeeded, clear our scan hash */
        kvtree_unset_all(scan);

        /* rescan the files */
        scr_scan_files(prefix, dir, id, scan);

        /* reinspect the files */
        scr_inspect_scan(scan);
      }
    }

    /* build summary:
     *   should only have one dataset
     *   remove BUILD, MISSING, UNRECOVERABLE, INVALID, XOR
     *   delete XOR files from the file list, and adjust the expected number of files
     *   (maybe we should just leave these in here, at least the missing list?) */
    kvtree_elem* list_elem = NULL;
    kvtree* list_hash = kvtree_get(scan, SCR_SCAN_KEY_DLIST);
    int list_size = kvtree_size(list_hash);
    if (list_size == 1) {
      for (list_elem = kvtree_elem_first(list_hash);
           list_elem != NULL;
           list_elem = kvtree_elem_next(list_elem))
      {
        /* get the hash for this checkpoint */
        kvtree* dset_hash = kvtree_elem_hash(list_elem);

        /* unset the BUILD, MISSING, UNRECOVERABLE, INVALID, and XOR keys for this checkpoint */
        kvtree_unset(dset_hash, SCR_SCAN_KEY_MAP);
        kvtree_unset(dset_hash, SCR_SCAN_KEY_BUILD);
        kvtree_unset(dset_hash, SCR_SCAN_KEY_MISSING);
        kvtree_unset(dset_hash, SCR_SCAN_KEY_UNRECOVERABLE);
        kvtree_unset(dset_hash, SCR_SCAN_KEY_INVALID);
        kvtree_unset(dset_hash, SCR_SCAN_KEY_PARTNER);
        kvtree_unset(dset_hash, SCR_SCAN_KEY_MAPPARTNER);
        kvtree_unset(dset_hash, SCR_SCAN_KEY_XOR);
        kvtree_unset(dset_hash, SCR_SCAN_KEY_MAPXOR);

        /* record the summary file version number */
        kvtree_set_kv_int(dset_hash, SCR_SUMMARY_KEY_VERSION, SCR_SUMMARY_FILE_VERSION_6);

        /* write the summary file out */
        rc = scr_summary_write(prefix, dir, dset_hash);
      }
    }

    /* free the scan hash */
    kvtree_delete(&scan);
  }

  /* delete the summary hash */
  kvtree_delete(&summary);

  return rc;
}

int index_list(const spath* prefix)
{
  int rc = SCR_SUCCESS;

  /* get string version of prefix */
  char* prefix_str = spath_strdup(prefix);

  /* create a new hash to store our index file data */
  kvtree* index = kvtree_new();

  /* read index file from the prefix directory */
  if (scr_index_read(prefix, index) != SCR_SUCCESS) {
    scr_err("Failed to read index file in %s @ %s:%d",
      prefix_str, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* TODO: we should bury this logic in scr_index_* functions */

  /* lookup name of current dataset */
  char* current = NULL;
  scr_index_get_current(index, &current);

  /* get a pointer to the checkpoint hash */
  kvtree* dset_hash = kvtree_get(index, SCR_INDEX_1_KEY_DATASET);

  /* sort datasets in descending order */
  kvtree_sort_int(dset_hash, KVTREE_SORT_DESCENDING);

  /* print header */
  printf("   DSET VALID FLUSHED             NAME\n");

  /* iterate over each of the datasets and print the id and other info */
  kvtree_elem* elem;
  for (elem = kvtree_elem_first(dset_hash);
       elem != NULL;
       elem = kvtree_elem_next(elem))
  {
    /* get the dataset id */
    int dset = kvtree_elem_key_int(elem);

    /* get the hash for this dataset */
    kvtree* hash = kvtree_elem_hash(elem);
    kvtree* name_hash = kvtree_get(hash, SCR_INDEX_1_KEY_NAME);

    /* sort dataset names in descending order */
    kvtree_sort(name_hash, KVTREE_SORT_DESCENDING);

    kvtree_elem* name_elem;
    for (name_elem = kvtree_elem_first(name_hash);
         name_elem != NULL;
         name_elem = kvtree_elem_next(name_elem))
    {
      /* get the dataset name for this dataset */
      char* name = kvtree_elem_key(name_elem);

      /* get the directory hash */
      kvtree* info_hash = kvtree_elem_hash(name_elem);

      /* skip this dataset if it's not a checkpoint */
      kvtree* dataset_hash = kvtree_get(info_hash, SCR_INDEX_1_KEY_DATASET);
      if (! scr_dataset_is_ckpt(dataset_hash)) {
        continue;
      }

      /* determine whether this dataset is complete */
      int complete = 0;
      kvtree_util_get_int(info_hash, SCR_INDEX_1_KEY_COMPLETE, &complete);

      /* determine time at which this checkpoint was marked as failed */
      char* failed_str = NULL;
      kvtree_util_get_str(info_hash, SCR_INDEX_1_KEY_FAILED, &failed_str);

      /* determine time at which this checkpoint was flushed */
      char* flushed_str = NULL;
      kvtree_util_get_str(info_hash, SCR_INDEX_1_KEY_FLUSHED, &flushed_str);

      /* compute number of times (and last time) checkpoint has been fetched */
/*
      kvtree* fetched_hash = kvtree_get(info_hash, SCR_INDEX_1_KEY_FETCHED);
      int num_fetch = kvtree_size(fetched_hash);
      kvtree_sort(fetched_hash, KVTREE_SORT_DESCENDING);
      kvtree_elem* fetched_elem = kvtree_elem_first(fetched_hash);
      char* fetched_str = kvtree_elem_key(fetched_elem);
*/

      /* print a star beside the dataset directory marked as current */
      if (current != NULL && strcmp(name, current) == 0) {
        printf("*");
      } else {
        printf(" ");
      }

      printf("%6d", dset);

      printf(" ");

      /* to be valid, the dataset must be marked as vaild and it must
       * not have failed a fetch attempt */
      if (complete == 1 && failed_str == NULL) {
        printf("YES  ");
      } else {
        printf("NO   ");
      }

      printf(" ");

/*
      if (failed_str != NULL) {
        printf("YES   ");
      } else {
        printf("NO    ");
      }

      printf(" ");
*/

      if (flushed_str != NULL) {
        printf("%s", flushed_str);
      } else {
        printf("                   ");
      }

      printf(" ");

/*
      printf("%7d", num_fetch);

      printf("  ");
      if (fetched_str != NULL) {
        printf("%s", fetched_str);
      } else {
        printf("                   ");
      }
*/

      if (name != NULL) {
        printf("%s", name);
      } else {
        printf("UNKNOWN_NAME");
      }
      printf("\n");
    }
  }

  /* free off our index hash */
  kvtree_delete(&index);

  /* free our string */
  scr_free(&prefix_str);

  return rc;
}

/* delete named dataset from index (does not delete files) */
int index_remove(const spath* prefix, const char* name)
{
  int rc = SCR_SUCCESS;

  /* get string version of prefix */
  char* prefix_str = spath_strdup(prefix);

  /* create a new hash to store our index file data */
  kvtree* index = kvtree_new();

  /* read index file from the prefix directory */
  if (scr_index_read(prefix, index) != SCR_SUCCESS) {
    scr_err("Failed to read index file in %s @ %s:%d",
      prefix_str, __FILE__, __LINE__
    );
    rc = SCR_FAILURE;
    goto cleanup;
  }

  /* remove dataset from index */
  if (scr_index_remove(index, name) == SCR_SUCCESS) {
    /* write out new index file */
    scr_index_write(prefix, index);
  } else {
    /* couldn't find the named dataset, print an error */
    scr_err("Named dataset was not found in index file: %s @ %s:%d",
      name, __FILE__, __LINE__
    );
    rc = SCR_FAILURE;
    goto cleanup;
  }

cleanup:

  /* free off our index hash */
  kvtree_delete(&index);

  /* free our string */
  scr_free(&prefix_str);

  return rc;
}

/* set named dataset as restart */
int index_current(const spath* prefix, const char* name)
{
  int rc = SCR_SUCCESS;

  /* get string version of prefix */
  char* prefix_str = spath_strdup(prefix);

  /* create a new hash to store our index file data */
  kvtree* index = kvtree_new();

  /* read index file from the prefix directory */
  if (scr_index_read(prefix, index) != SCR_SUCCESS) {
    scr_err("Failed to read index file in %s @ %s:%d",
      prefix_str, __FILE__, __LINE__
    );
    rc = SCR_FAILURE;
    goto cleanup;
  }

  /* update current to point to specified name */
  if (scr_index_set_current(index, name) == SCR_SUCCESS) {
    /* write out new index file */
    scr_index_write(prefix, index);
  } else {
    /* couldn't find dataset or it's not a checkpoint, print an error */
    scr_err("Named dataset is not a checkpoint in index file: %s @ %s:%d",
      name, __FILE__, __LINE__
    );
    rc = SCR_FAILURE;
    goto cleanup;
  }

cleanup:

  /* free off our index hash */
  kvtree_delete(&index);

  /* free our string */
  scr_free(&prefix_str);

  return rc;
}

/* given a prefix directory and a dataset id,
 * attempt add the dataset to the index file.
 * Returns SCR_SUCCESS if dataset can be indexed,
 * either as complete or incomplete */
int index_add(const spath* prefix, int id, int* complete_flag)
{
  int rc = SCR_SUCCESS;

  /* assume dataset is not complete */
  *complete_flag = 0;

  /* get string versions of prefix */
  char* prefix_str = spath_strdup(prefix);

  /* create a new hash to store our index file data */
  kvtree* index = kvtree_new();

  /* read index file from the prefix directory */
  scr_index_read(prefix, index);

  /* create a new hash to hold our summary file data */
  kvtree* summary = kvtree_new();

  /* read summary file from the dataset directory */
  spath* dataset_path = spath_dup(prefix);
  spath_append_str(dataset_path, ".scr");
  spath_append_strf(dataset_path, "scr.dataset.%d", id);
  if (scr_summary_read(dataset_path, summary) != SCR_SUCCESS) {
    /* if summary file is missing, attempt to build it */
    if (scr_summary_build(prefix, dataset_path, id) == SCR_SUCCESS) {
      /* if the build was successful, try the read again */
      scr_summary_read(dataset_path, summary);
    }
  }
  spath_delete(&dataset_path);

  /* get the dataset hash for this directory */
  scr_dataset* dataset = kvtree_get(summary, SCR_SUMMARY_6_KEY_DATASET);
  if (dataset != NULL) {
    /* get the dataset name */
    char* dataset_name;
    if (scr_dataset_get_name(dataset, &dataset_name) == SCR_SUCCESS) {
      /* found the name, now check whether it's complete (assume that it's not) */
      int complete;
      if (kvtree_util_get_int(summary, SCR_SUMMARY_6_KEY_COMPLETE, &complete) == KVTREE_SUCCESS) {
        /* write values to the index file */
        scr_index_set_dataset(index, id, dataset_name, dataset, complete);
        scr_index_mark_flushed(index, id, dataset_name);
        scr_index_write(prefix, index);

        /* update return flag to indicate that dataset is complete */
        *complete_flag = complete;
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
  kvtree_delete(&summary);

  /* free our index hash */
  kvtree_delete(&index);

  /* free our strings */
  scr_free(&prefix_str);

  return rc;
}

int print_usage()
{
  printf("\n");
  printf("  Usage: scr_index [options]\n");
  printf("\n");
  printf("  Options:\n");
  printf("    -l, --list           List indexed datasets (default behavior)\n");
  printf("    -a, --add=<id>       Add dataset <id> to index\n");
  printf("    -r, --remove=<name>  Remove dataset <name> from index (does not delete files)\n");
  printf("    -c, --current=<name> Set <name> as current restart directory\n");
  printf("    -p, --prefix=<dir>   Specify prefix directory (defaults to current working directory)\n");
  printf("    -h, --help           Print usage\n");
  printf("\n");
  return SCR_SUCCESS;
}

struct arglist {
  spath* prefix;
  char* name;
  int id;
  int list;
  int add;
  int remove;
  int current;
};

/* free any memory allocation during get_args */
int free_args(struct arglist* args)
{
  spath_delete(&(args->prefix));
  scr_free(&(args->name));
  return SCR_SUCCESS;
}

int get_args(int argc, char **argv, struct arglist* args)
{
  /* set to default values */
  args->prefix  = NULL;
  args->name    = NULL;
  args->id      = -1;
  args->list    = 1;
  args->add     = 0;
  args->remove  = 0;
  args->current = 0;

  static const char *opt_string = "la:r:p:h";
  static struct option long_options[] = {
    {"list",    no_argument,       NULL, 'l'},
    {"add",     required_argument, NULL, 'a'},
    {"remove",  required_argument, NULL, 'r'},
    {"current", required_argument, NULL, 'c'},
    {"prefix",  required_argument, NULL, 'p'},
    {"help",    no_argument,       NULL, 'h'},
    {NULL,      no_argument,       NULL,   0}
  };

  int long_index = 0;
  int opt = getopt_long(argc, argv, opt_string, long_options, &long_index);
  while (opt != -1) {
    switch(opt) {
      case 'l':
        args->list = 1;
        break;
      case 'a':
        args->id   = atoi(optarg);
        args->add  = 1;
        args->list = 0;
        break;
      case 'r':
        args->name   = strdup(optarg);
        args->remove = 1;
        args->list   = 0;
        break;
      case 'c':
        args->name    = strdup(optarg);
        args->current = 1;
        args->list    = 0;
        break;
      case 'p':
        args->prefix = spath_from_str(optarg);
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
      scr_err("Problem reading current working directory (getcwd() errno=%d %s) @ %s:%d",
        errno, strerror(errno), __FILE__, __LINE__
      );
      return SCR_FAILURE;
    }
    args->prefix = spath_from_str(prefix);
  }

  /* reduce paths to remove any trailing '/' */
  spath_reduce(args->prefix);

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

  /* get references to prefix and subdirectory paths */
  spath* prefix = args.prefix;
  char* name = args.name;
  int id = args.id;

  /* these options all require a prefix directory */
  if (args.add == 1 || args.remove == 1 || args.current == 1 || args.list == 1) {
    if (spath_is_null(prefix)) {
      print_usage();
      return 1;
    }
  }

  /* these options all require a dataset name */
  if (args.remove == 1 || args.current == 1) {
    if (name == NULL) {
      print_usage();
      return 1;
    }
  }

  if (args.add == 1) {
    /* add the dataset id to the index.scr file in the prefix directory,
     * rebuild missing files if necessary */
    rc = SCR_FAILURE;
    int complete = 0;
    if (index_add(prefix, id, &complete) == SCR_SUCCESS) {
      if (complete == 1) {
        /* only return success if we find a value for complete, and if that value is 1 */
        rc = SCR_SUCCESS;
      }
    }
  } else if (args.remove == 1) {
    /* remove the named dataset from the index file (does not delete files) */
    rc = index_remove(prefix, name);
  } else if (args.current == 1) {
    /* set named dataset as current restart */
    rc = index_current(prefix, name);
  } else if (args.list == 1) {
    /* list datasets recorded in index file */
    rc = index_list(prefix);
  }

  /* free any memory allocated for command line arguments */
  free_args(&args);

  /* translate our SCR return code into program return code */
  if (rc != SCR_SUCCESS) {
    return 1;
  }
  return 0;
}
