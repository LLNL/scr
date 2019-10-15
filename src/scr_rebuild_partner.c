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

#define REDSET_KEY_COPY_PARTNER_DESC    "DESC"
#define REDSET_KEY_COPY_PARTNER_CURRENT "CURRENT"
#define REDSET_KEY_COPY_PARTNER_PARTNER "PARTNER"
#define REDSET_KEY_COPY_PARTNER_RANKS   "RANKS"
#define REDSET_KEY_COPY_PARTNER_RANK    "RANK"
#define REDSET_KEY_COPY_PARTNER_GROUP   "GROUP"
#define REDSET_KEY_COPY_PARTNER_FILES   "FILES"
#define REDSET_KEY_COPY_PARTNER_FILE    "FILE"
#define REDSET_KEY_COPY_PARTNER_SIZE    "SIZE"

#define LEFT   (0)
#define RIGHT  (1)
#define CENTER (2)

#ifdef SCR_GLOBALS_H
#error "globals.h accessed from tools"
#endif

int buffer_size = 128*1024;

/* execute xor operation with N-1 files and xor file: 
     open each redset file and read header to get info for user files
     open each user file
     open missing user file
     open missing redset file
     for all chunks
       read a chunk from missing file (xor file) into memory buffer A
       for each other file i
         read chunk from file i into memory buffer B
         merge chunks and store in memory buffer A
       write chunk in memory buffer A to missing file
     close all files
*/

static int scr_compute_crc(scr_filemap* map, const char* file)
{
  /* compute crc for the file */
  uLong crc_file;
  if (scr_crc32(file, &crc_file) != SCR_SUCCESS) {
    scr_err("Failed to compute crc for file %s @ %s:%d",
      file, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* allocate a new meta data object */
  scr_meta* meta = scr_meta_new();
  if (meta == NULL) {
    scr_abort(-1, "Failed to allocate meta data object @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* read meta data from filemap */
  if (scr_filemap_get_meta(map, file, meta) != SCR_SUCCESS) {
    return SCR_FAILURE;
  }

  int rc = SCR_SUCCESS;

  /* read crc value from meta data */
  uLong crc_meta;
  if (scr_meta_get_crc32(meta, &crc_meta) == SCR_SUCCESS) {
    /* check that the values are the same */
    if (crc_file != crc_meta) {
      rc = SCR_FAILURE;
    }
  } else {
    /* record crc in filemap */
    scr_meta_set_crc32(meta, crc_file);
    scr_filemap_set_meta(map, file, meta);
  }

  /* free our meta data object */
  scr_meta_delete(&meta);

  return rc;
}

/* given an XOR header, lookup and return global rank given rank in group */
static int lookup_rank(const kvtree* header, int group_rank, const char* file)
{
  int rank = -1;
  kvtree* group_hash = kvtree_get(header, REDSET_KEY_COPY_PARTNER_GROUP);
  kvtree* rank_hash  = kvtree_get_kv_int(group_hash, REDSET_KEY_COPY_PARTNER_RANK, group_rank);
  kvtree_elem* elem = kvtree_elem_first(rank_hash);
  if (elem != NULL) {
    rank = kvtree_elem_key_int(elem);
  } else {
    scr_err("Failed to read rank from XOR file header in %s @ %s:%d",
      file, __FILE__, __LINE__
    );
  }
  return rank;
}

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

static int apply_partner(
  char* files[],
  int fds[],
  int offsets[],
  int num_files[],
  char* user_files[],
  int user_fds[],
  unsigned long user_filesizes[])
{
  int i, j;
  int rc = 0;

  /* allocate buffers */
  char* buffer = malloc(buffer_size * sizeof(char));
  if (buffer == NULL) {
    scr_err("Failed to allocate buffer memory @ %s:%d",
      __FILE__, __LINE__
    );
    return 1;
  }

  /* get pointers to left and center arrays */
  int lhs_offset = offsets[LEFT];
  char** lhs_files             = &user_files[lhs_offset];
  int* lhs_fds                 = &user_fds[lhs_offset];
  unsigned long* lhs_filesizes = &user_filesizes[lhs_offset];

  int center_offset = offsets[CENTER];
  char** center_files             = &user_files[center_offset];
  int* center_fds                 = &user_fds[center_offset];
  unsigned long* center_filesizes = &user_filesizes[center_offset];

  /* compute total bytes we'll read from left partner,
   * sum of data for each user file of our left partner */
  off_t lhs_datasize = 0;
  for (i = 0; i < num_files[LEFT]; i++) {
    lhs_datasize += (off_t)lhs_filesizes[i];
  }

  /* compute total bytes we'll read from right partner,
   * sum of data for each of our own user files */
  off_t center_datasize = 0;
  for (i = 0; i < num_files[CENTER]; i++) {
    center_datasize += (off_t)center_filesizes[i];
  }

  /* copy data from left partner user files into our redset partner file */
  unsigned long read_pos = 0;
  off_t nread = 0;
  while (nread < lhs_datasize && rc == 0) {
    /* read up to buffer_size bytes at a time */
    size_t count = buffer_size;
    if ((lhs_datasize - nread) < (off_t)buffer_size) {
      count = (size_t)(lhs_datasize - nread);
    }

    /* read data from left partner user files */
    if (scr_read_pad_n(num_files[LEFT], lhs_files, lhs_fds, buffer, count, read_pos, lhs_filesizes) != SCR_SUCCESS) {
      /* our read failed, set the return code to an error */
      rc = 1;
      count = 0;
    }
    read_pos += count;

    /* write data to partner file for the missing rank */
    if (scr_write_attempt(files[CENTER], fds[CENTER], buffer, count) != count) {
      /* our write failed, set the return code to an error */
      rc = 1;
    }

    nread += count;
  }

  /* copy data from right partner redset file into our user files */
  unsigned long write_pos = 0;
  nread = 0;
  while (nread < center_datasize && rc == 0) {
    /* read up to buffer_size bytes at a time */
    size_t count = buffer_size;
    if ((center_datasize - nread) < (off_t)buffer_size) {
      count = (size_t)(center_datasize - nread);
    }

    /* read data from right partner file */
    if (scr_read_attempt(files[RIGHT], fds[RIGHT], buffer, count) != count) {
      /* our read failed, set the return code to an error */
      rc = 1;
      count = 0;
    }

    /* write data to our user files */
    if (scr_write_pad_n(num_files[CENTER], center_files, center_fds, buffer, count, write_pos, center_filesizes) != SCR_SUCCESS) {
      /* our write failed, set the return code to an error */
      rc = 1;
    }
    write_pos += count;

    nread += count;
  }

  scr_free(&buffer);

  return rc;
}

int rebuild(const spath* path_prefix, int build_data, int index, char* argv[])
{
  int i, j;

  /* allocate memory for data structures based on the XOR set size */
  int     ranks[3];
  int     num_files[3];
  int     offsets[3];
  char*   files[3];
  int     fds[3];
  kvtree* headers[3];
  scr_filemap* filemaps[3];

  headers[LEFT]   = kvtree_new();
  headers[RIGHT]  = kvtree_new();
  headers[CENTER] = kvtree_new();

  filemaps[LEFT]   = scr_filemap_new();
  filemaps[RIGHT]  = scr_filemap_new();
  filemaps[CENTER] = scr_filemap_new();

  /* read in the rank of the left partner */
  ranks[LEFT] = (int) strtol(argv[index++], (char **)NULL, 10);
  if (ranks[LEFT] < 0) {
    scr_err("Invalid rank argument %s @ %s:%d",
      argv[index-1], __FILE__, __LINE__
    );
    return 1;
  }

  /* read in the missing rank */
  ranks[CENTER] = (int) strtol(argv[index++], (char **)NULL, 10);
  if (ranks[CENTER] < 0) {
    scr_err("Invalid rank argument %s @ %s:%d",
      argv[index-1], __FILE__, __LINE__
    );
    return 1;
  }

  /* read in the right rank */
  ranks[RIGHT] = (int) strtol(argv[index++], (char **)NULL, 10);
  if (ranks[RIGHT] < 0) {
    scr_err("Invalid rank argument %s @ %s:%d",
      argv[index-1], __FILE__, __LINE__
    );
    return 1;
  }

  /* copy the file name of left partner */
  files[LEFT] = strdup(argv[index++]);
  if (files[LEFT] == NULL) {
    scr_err("Failed to dup filename @ %s:%d",
      __FILE__, __LINE__
    );
    return 1;
  }

  /* copy the file name of right partner */
  files[RIGHT] = strdup(argv[index++]);
  if (files[RIGHT] == NULL) {
    scr_err("Failed to dup filename @ %s:%d",
      __FILE__, __LINE__
    );
    return 1;
  }

  /* open partner files for reading and read in headers */
  for (i = 0; i < 2; i++) {
    /* open file for reading */
    fds[i] = scr_open(files[i], O_RDONLY);
    if (fds[i] < 0) {
      scr_err("Opening partner file: scr_open(%s) errno=%d %s @ %s:%d",
        files[i], errno, strerror(errno), __FILE__, __LINE__
      );
      return 1;
    }

    /* read the header from this xor file */
    if (kvtree_read_fd(files[i], fds[i], headers[i]) < 0) {
      scr_err("Failed to read header from %s @ %s:%d",
        files[i], __FILE__, __LINE__
      );
      return 1;
    }
  }

  /* build header for missing partner file */
  kvtree_merge(headers[CENTER], headers[RIGHT]);

  /* fetch our own file list from rank to our right */
  kvtree* rhs_hash = kvtree_get(headers[RIGHT], REDSET_KEY_COPY_PARTNER_PARTNER);
  kvtree* current_hash = kvtree_new();
  kvtree_merge(current_hash, rhs_hash);
  kvtree_set(headers[CENTER], REDSET_KEY_COPY_PARTNER_CURRENT, current_hash);

  /* we are the partner to the rank to our left */
  kvtree* lhs_hash = kvtree_get(headers[LEFT], REDSET_KEY_COPY_PARTNER_CURRENT);
  kvtree* partner_hash = kvtree_new();
  kvtree_merge(partner_hash, lhs_hash);
  kvtree_set(headers[CENTER], REDSET_KEY_COPY_PARTNER_PARTNER, partner_hash);

  /* get a pointer to the current hash for the missing rank */
  kvtree* missing_current_hash = kvtree_get(headers[CENTER], REDSET_KEY_COPY_PARTNER_CURRENT);

  /* define name for missing partner file */
  char missing_name[1024];
  if (build_data) {
    snprintf(missing_name, sizeof(missing_name), "reddesc.er.%d.partner.%d_%d_%d.redset", ranks[CENTER], ranks[LEFT], ranks[CENTER], ranks[RIGHT]);
  } else {
    snprintf(missing_name, sizeof(missing_name), "reddescmap.er.%d.partner.%d_%d_%d.redset", ranks[CENTER], ranks[LEFT], ranks[CENTER], ranks[RIGHT]);
  }
  files[CENTER] = strdup(missing_name);

  /* determine number of files each member wrote */
  for (i=0; i < 3; i++) {
    /* record the number of files for this rank */
    kvtree* current_hash = kvtree_get(headers[i], REDSET_KEY_COPY_PARTNER_CURRENT);
    if (kvtree_util_get_int(current_hash, REDSET_KEY_COPY_PARTNER_FILES, &num_files[i]) != KVTREE_SUCCESS) {
      scr_err("Failed to read number of files from %s @ %s:%d",
        files[i], __FILE__, __LINE__
      );
      return 1;
    }
  }
  
  /* count the total number of files and set the offsets array */
  int total_num_files = 0;
  for (i=0; i < 3; i++) {
    offsets[i] = total_num_files;
    total_num_files += num_files[i];
  }

  /* allocate space for a file descriptor, file name pointer, and filesize for each user file */
  int* user_fds                 = (int*)           malloc(total_num_files * sizeof(int));
  char** user_files             = (char**)         malloc(total_num_files * sizeof(char*));
  unsigned long* user_filesizes = (unsigned long*) malloc(total_num_files * sizeof(unsigned long));
  if (user_fds == NULL || user_files == NULL || user_filesizes == NULL) {
    scr_err("Failed to allocate buffer memory @ %s:%d",
      __FILE__, __LINE__
    );
    return 1;
  }

  /* get file name, file size, and open each of the user files that we have */
  for (i=0; i < 3; i++) {
    /* read in filemap for this member */
    if (build_data) {
      spath* filemap_path = spath_dup(path_prefix);
      spath_append_strf(filemap_path, "filemap_%d", ranks[i]);
      scr_filemap_read(filemap_path, filemaps[i]);
      spath_delete(&filemap_path);
    }

    /* for each file belonging to this rank, get filename, filesize, and open file */
    kvtree* current_hash = kvtree_get(headers[i], REDSET_KEY_COPY_PARTNER_CURRENT);
    
    for (j=0; j < num_files[i]; j++) {
      /* compute offset into total files array */
      int offset = offsets[i] + j;

      /* get hash for this file */
      kvtree* file_hash = kvtree_get_kv_int(current_hash, REDSET_KEY_COPY_PARTNER_FILE, j);

      /* should just have one element */
      kvtree_elem* elem = kvtree_elem_first(file_hash);

      /* full path to file */
      const char* fullpath = kvtree_elem_key(elem);

      /* meta data for file */
      kvtree* meta_hash = kvtree_elem_hash(elem);

      /* record the filesize of this file */
      unsigned long filesize = 0;
      if (kvtree_util_get_bytecount(meta_hash, REDSET_KEY_COPY_PARTNER_SIZE, &filesize) != KVTREE_SUCCESS) {
        scr_err("Failed to read filesize field for file %d in %s @ %s:%d",
          j, files[i], __FILE__, __LINE__
        );
        return 1;
      }
      user_filesizes[offset] = filesize;

      /* get path to user file */
      spath* path_name = spath_from_str(fullpath);
      if (build_data) {
        /* for data files, we have to remap based on filemap info */
        const char* path_file = spath_strdup(path_name);
        user_files[offset] = lookup_path(filemaps[i], path_file);
        scr_free(&path_file);
      } else {
        /* for map files, we're running in the same directory,
         * just use the basename to open the file */
        spath_basename(path_name);
        user_files[offset] = spath_strdup(path_name);
      }
      spath_delete(&path_name);

      /* open the file */
      if (i == CENTER) {
        /* create directory for file */
        spath* user_dir_path = spath_from_str(user_files[offset]);
        spath_reduce(user_dir_path);
        spath_dirname(user_dir_path);
        if (! spath_is_null(user_dir_path)) {
          char* user_dir = spath_strdup(user_dir_path);
          mode_t mode_dir = scr_getmode(1, 1, 1);
          if (scr_mkdir(user_dir, mode_dir) != SCR_SUCCESS) {
            scr_err("Failed to create directory for user file %s @ %s:%d",
              user_dir, __FILE__, __LINE__
            );
            return 1;
          }
          scr_free(&user_dir);
        }
        spath_delete(&user_dir_path);

        /* open missing file for writing */
        mode_t mode_file = scr_getmode(1, 1, 0);
        user_fds[offset] = scr_open(user_files[offset], O_WRONLY | O_CREAT | O_TRUNC, mode_file);
        if (user_fds[offset] < 0) {
          scr_err("Opening user file for writing: scr_open(%s) errno=%d %s @ %s:%d",
            user_files[offset], errno, strerror(errno), __FILE__, __LINE__
          );
          return 1;
        }
      } else {
        /* open existing file for reading */
        user_fds[offset] = scr_open(user_files[offset], O_RDONLY);
        if (user_fds[offset] < 0) {
          scr_err("Opening user file for reading: scr_open(%s) errno=%d %s @ %s:%d",
            user_files[offset], errno, strerror(errno), __FILE__, __LINE__
          );
          return 1;
        }
      }
    }
  }

  /* finally, open the partner file for the missing rank */
  mode_t mode_file = scr_getmode(1, 1, 0);
  fds[CENTER] = scr_open(files[CENTER], O_WRONLY | O_CREAT | O_TRUNC, mode_file);
  if (fds[CENTER] < 0) {
    scr_err("Opening partner file to be reconstructed: scr_open(%s) errno=%d %s @ %s:%d",
      files[CENTER], errno, strerror(errno), __FILE__, __LINE__
    );
    return 1;
  }

  int rc = 0;

  /* write the header to the XOR file of the missing rank */
  if (kvtree_write_fd(files[CENTER], fds[CENTER], headers[CENTER]) < 0) {
    rc = 1;
  }

  /* apply xor encoding */
  if (rc == 0) {
    rc = apply_partner(files, fds, offsets, num_files, user_files, user_fds, user_filesizes);
  }

  /* close each of the user files */
  for (i=0; i < total_num_files; i++) {
    if (scr_close(user_files[i], user_fds[i]) != SCR_SUCCESS) {
      rc = 1;
    }
  }

  /* close each of the partner files */
  for (i=0; i < 3; i++) {
    if (scr_close(files[i], fds[i]) != SCR_SUCCESS) {
      rc = 1;
    }
  }

  /* if the write failed, delete the files we just wrote, and return an error */
  if (rc != 0) {
    for (j=0; j < num_files[0]; j++) {
      scr_file_unlink(user_files[j]);
    }
    scr_file_unlink(files[0]);
    return 1;
  }

  /* check that filesizes are correct */
  unsigned long filesize;
  for (j=0; j < num_files[0]; j++) {
    filesize = scr_file_size(user_files[j]);
    if (filesize != user_filesizes[j]) {
      /* the filesize check failed, so delete the file */
      scr_file_unlink(user_files[j]);
      rc = 1;
    }
  }
  /* TODO: we didn't record the filesize of the partner file for the missing rank anywhere */

  for (i=0; i < total_num_files; i++) {
    scr_free(&user_files[i]);
  }

  scr_free(&user_filesizes);
  scr_free(&user_files);
  scr_free(&user_fds);

  for (i=0; i < 3; i++) {
    kvtree_delete(&headers[i]);
    scr_filemap_delete(&filemaps[i]);
  }

  for (i=0; i < 3; i++) {
    scr_free(&files[i]);
  }

  return rc;
}

int main(int argc, char* argv[])
{
  int index = 1;

  /* print usage if not enough arguments were given */
  if (argc < 2) {
    printf("Usage: scr_rebuild_partner <xor|map> <left_partner> <right_partner>\n");
    return 1;
  }

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
    rc = rebuild(path_prefix, 0, index, argv);
  } else {
    rc = rebuild(path_prefix, 1, index, argv);
  }

  spath_delete(&path_prefix);

  return rc;
}
