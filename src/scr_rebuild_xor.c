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

#define REDSET_KEY_COPY_XOR_DESC    "DESC"
#define REDSET_KEY_COPY_XOR_CURRENT "CURRENT"
#define REDSET_KEY_COPY_XOR_PARTNER "PARTNER"
#define REDSET_KEY_COPY_XOR_RANKS   "RANKS"
#define REDSET_KEY_COPY_XOR_RANK    "RANK"
#define REDSET_KEY_COPY_XOR_GROUP   "GROUP"
#define REDSET_KEY_COPY_XOR_FILES   "FILES"
#define REDSET_KEY_COPY_XOR_FILE    "FILE"
#define REDSET_KEY_COPY_XOR_SIZE    "SIZE"
#define REDSET_KEY_COPY_XOR_CHUNK   "CHUNK"

int buffer_size = 128*1024;

/* execute xor operation with N-1 files and xor file: 
     open each XOR file and read header to get info for user files
     open each user file
     open missing user file
     open missing XOR file
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
  kvtree* group_hash = kvtree_get(header, REDSET_KEY_COPY_XOR_GROUP);
  kvtree* rank_hash  = kvtree_get_kv_int(group_hash, REDSET_KEY_COPY_XOR_RANK, group_rank);
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

static int apply_xor(
  int xor_set_size,
  int root,
  char* xor_files[],
  int xor_fds[],
  int offsets[],
  int num_files[],
  char* user_files[],
  int user_fds[],
  unsigned long user_filesizes[],
  size_t chunk_size)
{
  int i, j;
  int rc = 0;

  /* allocate buffers */
  char* buffer_A = malloc(buffer_size * sizeof(char));
  char* buffer_B = malloc(buffer_size * sizeof(char));
  if (buffer_A == NULL) {
    scr_err("Failed to allocate buffer memory @ %s:%d",
      __FILE__, __LINE__
    );
    return 1;
  }
  if (buffer_B == NULL) {
    scr_err("Failed to allocate buffer memory @ %s:%d",
      __FILE__, __LINE__
    );
    free(buffer_A);
    return 1;
  }

  /* this offset array records the current position we are in the logical file for each rank */
  unsigned long* offset = malloc(xor_set_size * sizeof(unsigned long));
  if (offset == NULL) {
    scr_err("Failed to allocate buffer memory @ %s:%d",
      __FILE__, __LINE__
    );
    return 1;
  }
  for (i=0; i < xor_set_size; i++) {
    offset[i] = 0;
  }

  unsigned long write_pos = 0;
  int chunk_id;
  for (chunk_id = 0; chunk_id < xor_set_size && rc == 0; chunk_id++) {
    size_t nread = 0;
    while (nread < chunk_size && rc == 0) {
      /* read upto buffer_size bytes at a time */
      size_t count = chunk_size - nread;
      if (count > buffer_size) {
        count = buffer_size;
      }

      /* clear our buffer */
      memset(buffer_A, 0, count);

      /* read a segment from each rank and XOR it into our buffer */
      for (i=1; i < xor_set_size; i++) {
        /* read the next set of bytes for this chunk from my file into send_buf */
        if (chunk_id != ((i + root) % xor_set_size)) {
          /* read chunk from the logical file for this rank */
          if (scr_read_pad_n(num_files[i], &user_files[offsets[i]], &user_fds[offsets[i]],
                             buffer_B, count, offset[i], &user_filesizes[offsets[i]]) != SCR_SUCCESS)
          {
            /* our read failed, set the return code to an error */
            rc = 1;
            count = 0;
          }
          offset[i] += count;
        } else {
          /* read chunk from the XOR file for this rank */
          if (scr_read_attempt(xor_files[i], xor_fds[i], buffer_B, count) != count) {
            /* our read failed, set the return code to an error */
            rc = 1;
            count = 0;
          }
        }

        /* TODO: XORing with unsigned long would be faster here (if chunk size is multiple of this size) */
        /* merge the blocks via xor operation */
        for (j = 0; j < count; j++) {
          buffer_A[j] ^= buffer_B[j];
        }
      }

      /* at this point, we have the data from the missing rank, write it out */
      if (chunk_id != root) {
        /* write chunk to logical file for the missing rank */
        if (scr_write_pad_n(num_files[0], &user_files[0], &user_fds[0],
                            buffer_A, count, write_pos, &user_filesizes[0]) != SCR_SUCCESS)
        {
          /* our write failed, set the return code to an error */
          rc = 1;
        }
        write_pos += count;
      } else {
        /* write chunk to xor file for the missing rank */
        if (scr_write_attempt(xor_files[0], xor_fds[0], buffer_A, count) != count) {
          /* our write failed, set the return code to an error */
          rc = 1;
        }
      }

      nread += count;
    }
  }

  scr_free(&offset);

  scr_free(&buffer_B);
  scr_free(&buffer_A);

  return rc;
}

int rebuild(const spath* path_prefix, int build_data, int index, char* argv[])
{
  int i, j;

  /* read in the size of the XOR set */
  int xor_set_size = (int) strtol(argv[index++], (char **)NULL, 10);
  if (xor_set_size <= 0) {
    scr_err("Invalid XOR set size argument %s @ %s:%d",
      argv[index-1], __FILE__, __LINE__
    );
    return 1;
  }

  /* allocate memory for data structures based on the XOR set size */
  int*   num_files  = malloc(xor_set_size * sizeof(int));
  int*   offsets    = malloc(xor_set_size * sizeof(int));
  char** xor_files  = malloc(xor_set_size * sizeof(char*));
  int*   xor_fds    = malloc(xor_set_size * sizeof(int));
  kvtree** xor_headers = malloc(xor_set_size * sizeof(kvtree*));
  scr_filemap** filemaps = malloc(xor_set_size * sizeof(scr_filemap*));
  if (num_files == NULL || offsets == NULL || xor_files == NULL || xor_fds == NULL || xor_headers == NULL || filemaps == NULL) {
    scr_err("Failed to allocate buffer memory @ %s:%d",
      __FILE__, __LINE__
    );
    return 1;
  }

  /* read in the rank of the missing process (the root) */
  int root = (int) strtol(argv[index++], (char **)NULL, 10);
  if (root < 0 || root >= xor_set_size) {
    scr_err("Invalid root argument %s @ %s:%d",
      argv[index-1], __FILE__, __LINE__
    );
    return 1;
  }

  /* read in the xor filenames (expected to be in order of XOR segment number) */
  /* we order ranks so that root is index 0, the rank to the right of root is index 1, and so on */
  for (i=0; i < xor_set_size; i++) {
    xor_headers[i] = kvtree_new();
    filemaps[i] = scr_filemap_new();

    /* we'll get the XOR file name for root from the header stored in the XOR file of the partner */
    if (i == root) {
      continue;
    }

    /* adjust the index relative to root */
    j = i - root;
    if (j < 0) {
      j += xor_set_size;
    }

    /* copy the XOR file name */
    xor_files[j] = strdup(argv[index++]);
    if (xor_files[j] == NULL) {
      scr_err("Failed to dup XOR filename @ %s:%d",
        __FILE__, __LINE__
      );
      return 1;
    }
  }

  /* open each of the xor files and read in the headers */
  for (i=1; i < xor_set_size; i++) {
    /* open each xor file for reading */
    xor_fds[i] = scr_open(xor_files[i], O_RDONLY);
    if (xor_fds[i] < 0) {
      scr_err("Opening xor segment file: scr_open(%s) errno=%d %s @ %s:%d",
        xor_files[i], errno, strerror(errno), __FILE__, __LINE__
      );
      return 1;
    }

    /* read the header from this xor file */
    if (kvtree_read_fd(xor_files[i], xor_fds[i], xor_headers[i]) < 0) {
      scr_err("Failed to read XOR header from %s @ %s:%d",
        xor_files[i], __FILE__, __LINE__
      );
      return 1;
    }
  }

  /* build header for missing XOR file */
  int partner_rank = -1;
  if (xor_set_size >= 2) {
    kvtree_merge(xor_headers[0], xor_headers[1]);

    /* fetch our own file list from rank to our right */
    kvtree* rhs_hash = kvtree_get(xor_headers[1], REDSET_KEY_COPY_XOR_PARTNER);
    kvtree* current_hash = kvtree_new();
    kvtree_merge(current_hash, rhs_hash);
    kvtree_set(xor_headers[0], REDSET_KEY_COPY_XOR_CURRENT, current_hash);

    /* we are the partner to the rank to our left */
    kvtree* lhs_hash = kvtree_get(xor_headers[xor_set_size-1], REDSET_KEY_COPY_XOR_CURRENT);
    kvtree* partner_hash = kvtree_new();
    kvtree_merge(partner_hash, lhs_hash);
    kvtree_set(xor_headers[0], REDSET_KEY_COPY_XOR_PARTNER, partner_hash);

    /* get global rank of partner */
    kvtree* desc_hash = kvtree_get(lhs_hash, REDSET_KEY_COPY_XOR_DESC);
    if (kvtree_util_get_int(desc_hash, REDSET_KEY_COPY_XOR_RANK, &partner_rank) != SCR_SUCCESS) {
      scr_err("Failed to read partner rank from XOR file header in %s @ %s:%d",
        xor_files[xor_set_size-1], __FILE__, __LINE__
      );
      return 1;
    }
  }

  /* get a pointer to the current hash for the missing rank */
  kvtree* missing_current_hash = kvtree_get(xor_headers[0], REDSET_KEY_COPY_XOR_CURRENT);

  /* get XOR set id */
  int xor_set_id = -1;
  kvtree* desc_hash  = kvtree_get(missing_current_hash, REDSET_KEY_COPY_XOR_DESC);
  if (kvtree_util_get_int(desc_hash, REDSET_KEY_COPY_XOR_GROUP, &xor_set_id) != KVTREE_SUCCESS) {
    scr_err("Failed to read set id from XOR file header in %s @ %s:%d",
      xor_files[1], __FILE__, __LINE__
    );
    return 1;
  }

  /* get our global MPI rank from GROUP map */
  int my_rank = lookup_rank(xor_headers[1], root, xor_files[1]);
  if (my_rank == -1) {
    scr_err("Failed to read rank from XOR file header in %s @ %s:%d",
      xor_files[1], __FILE__, __LINE__
    );
    return 1;
  }

  /* define name for missing XOR file */
  char xorname[1024];
  if (build_data) {
    snprintf(xorname, sizeof(xorname), "reddesc.er.%d.xor.%d_%d_of_%d.redset", my_rank, xor_set_id, root+1, xor_set_size);
  } else {
    snprintf(xorname, sizeof(xorname), "reddescmap.er.%d.xor.%d_%d_of_%d.redset", my_rank, xor_set_id, root+1, xor_set_size);
  }
  xor_files[0] = strdup(xorname);

  /* read the chunk size */
  unsigned long chunk_size = 0;
  if (kvtree_util_get_unsigned_long(xor_headers[0], REDSET_KEY_COPY_XOR_CHUNK, &chunk_size) != SCR_SUCCESS) {
    scr_err("Failed to read chunk size from XOR file header in %s @ %s:%d",
      xor_files[0], __FILE__, __LINE__
    );
    return 1;
  }

  /* determine number of files each member wrote in XOR set */
  for (i=0; i < xor_set_size; i++) {
    /* record the number of files for this rank */
    kvtree* current_hash = kvtree_get(xor_headers[i], REDSET_KEY_COPY_XOR_CURRENT);
    if (kvtree_util_get_int(current_hash, REDSET_KEY_COPY_XOR_FILES, &num_files[i]) != SCR_SUCCESS) {
      scr_err("Failed to read number of files from %s @ %s:%d",
        xor_files[i], __FILE__, __LINE__
      );
      return 1;
    }
  }
  
  /* count the total number of files and set the offsets array */
  int total_num_files = 0;
  for (i=0; i < xor_set_size; i++) {
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
  for (i=0; i < xor_set_size; i++) {
    /* lookup global mpi rank for this group rank */
    int rank = lookup_rank(xor_headers[0], i, xor_files[0]);

    /* read in filemap for this member */
    if (build_data) {
      spath* filemap_path = spath_dup(path_prefix);
      spath_append_strf(filemap_path, "filemap_%d", rank);
      scr_filemap_read(filemap_path, filemaps[i]);
      spath_delete(&filemap_path);
    }

    kvtree* current_hash = kvtree_get(xor_headers[i], REDSET_KEY_COPY_XOR_CURRENT);

    /* for each file belonging to this rank, get filename, filesize, and open file */
    for (j=0; j < num_files[i]; j++) {
      /* compute offset into total files array */
      int offset = offsets[i] + j;

      /* get the meta data for this file */
      kvtree* file_hash = kvtree_get_kv_int(current_hash, REDSET_KEY_COPY_XOR_FILE, j);
      if (file_hash == NULL) {
        scr_err("Failed to read hash data for file %d in %s @ %s:%d",
          j, xor_files[i], __FILE__, __LINE__
        );
        return 1;
      }

      /* should just have one element */
      kvtree_elem* elem = kvtree_elem_first(file_hash);

      /* full path to file */
      const char* fullpath = kvtree_elem_key(elem);

      /* meta data for file */
      kvtree* meta_hash = kvtree_elem_hash(elem);

      /* record the filesize of this file */
      unsigned long filesize = 0;
      if (kvtree_util_get_bytecount(meta_hash, REDSET_KEY_COPY_XOR_SIZE, &filesize) != KVTREE_SUCCESS) {
        scr_err("Failed to read filesize field for file %d in %s @ %s:%d",
          j, xor_files[i], __FILE__, __LINE__
        );
        return 1;
      }
      user_filesizes[offset] = filesize;

      /* get path of file */
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
      if (i == 0) {
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

  /* finally, open the xor file for the missing rank */
  mode_t mode_file = scr_getmode(1, 1, 0);
  xor_fds[0] = scr_open(xor_files[0], O_WRONLY | O_CREAT | O_TRUNC, mode_file);
  if (xor_fds[0] < 0) {
    scr_err("Opening xor file to be reconstructed: scr_open(%s) errno=%d %s @ %s:%d",
      xor_files[0], errno, strerror(errno), __FILE__, __LINE__
    );
    return 1;
  }

  int rc = 0;

  /* write the header to the XOR file of the missing rank */
  if (kvtree_write_fd(xor_files[0], xor_fds[0], xor_headers[0]) < 0) {
    rc = 1;
  }

  /* apply xor encoding */
  if (rc == 0) {
    rc = apply_xor(xor_set_size, root, xor_files, xor_fds, offsets, num_files, user_files, user_fds, user_filesizes, chunk_size);
  }

  /* close each of the user files */
  for (i=0; i < total_num_files; i++) {
    if (scr_close(user_files[i], user_fds[i]) != SCR_SUCCESS) {
      rc = 1;
    }
  }

  /* close each of the XOR files */
  for (i=0; i < xor_set_size; i++) {
    if (scr_close(xor_files[i], xor_fds[i]) != SCR_SUCCESS) {
      rc = 1;
    }
  }

  /* if the write failed, delete the files we just wrote, and return an error */
  if (rc != 0) {
    for (j=0; j < num_files[0]; j++) {
      scr_file_unlink(user_files[j]);
    }
    scr_file_unlink(xor_files[0]);
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
  /* TODO: we didn't record the filesize of the XOR file for the missing rank anywhere */

  for (i=0; i < total_num_files; i++) {
    scr_free(&user_files[i]);
  }

  scr_free(&user_filesizes);
  scr_free(&user_files);
  scr_free(&user_fds);

  for (i=0; i < xor_set_size; i++) {
    kvtree_delete(&xor_headers[i]);
    scr_filemap_delete(&filemaps[i]);
  }

  for (i=0; i < xor_set_size; i++) {
    scr_free(&xor_files[i]);
  }

  scr_free(&filemaps);
  scr_free(&xor_headers);
  scr_free(&xor_fds);
  scr_free(&xor_files);
  scr_free(&offsets);
  scr_free(&num_files);

  return rc;
}

int main(int argc, char* argv[])
{
  int i, j;
  int index = 1;

  /* print usage if not enough arguments were given */
  if (argc < 2) {
    printf("Usage: scr_rebuild_xor <xor|map> <size> <root> <ordered_remaining_xor_filenames>\n");
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
