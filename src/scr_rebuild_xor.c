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
#include "scr_path.h"
#include "scr_meta.h"
#include "scr_err.h"
#include "scr_util.h"
#include "scr_filemap.h"
#include "scr_dataset.h"

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

static int scr_compute_crc(scr_filemap* map, int id, int rank, const char* file)
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
  if (scr_filemap_get_meta(map, id, rank, file, meta) != SCR_SUCCESS) {
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
    scr_filemap_set_meta(map, id, rank, file, meta);
  }

  /* free our meta data object */
  scr_meta_delete(&meta);

  return rc;
}

int main(int argc, char* argv[])
{
  int i, j;
  int index = 1;

  /* print usage if not enough arguments were given */
  if (argc < 2) {
    printf("Usage: scr_rebuild_xor <size> <root> <missing_xor_filename> <ordered_remaining_xor_filenames>\n");
    return 1;
  }

  /* TODO: want to pass this on command line? */
  /* get current working directory */
  char dsetdir[SCR_MAX_FILENAME];
  scr_getcwd(dsetdir, sizeof(dsetdir));

  /* create and reduce path for dataset */
  scr_path* path_dset = scr_path_from_str(dsetdir);
  scr_path_reduce(path_dset);

  /* allocate buffers */
  char* buffer_A = malloc(buffer_size * sizeof(char));
  char* buffer_B = malloc(buffer_size * sizeof(char));
  if (buffer_A == NULL || buffer_B == NULL) {
    scr_err("Failed to allocate buffer memory @ %s:%d",
      __FILE__, __LINE__
    );
    return 1;
  }

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
  scr_hash** xor_headers = malloc(xor_set_size * sizeof(scr_hash*));
  if (num_files == NULL || offsets == NULL || xor_files == NULL || xor_fds == NULL || xor_headers == NULL) {
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

  /* read in the missing xor filename */
  xor_files[0] = strdup(argv[index++]);
  if (xor_files[0] == NULL) {
    scr_err("Failed to dup XOR filename @ %s:%d",
      __FILE__, __LINE__
    );
    return 1;
  }

  /* read in the xor filenames (expected to be in order of XOR segment number) */
  /* we order ranks so that root is index 0, the rank to the right of root is index 1, and so on */
  for (i=0; i < xor_set_size; i++) {
    xor_headers[i] = scr_hash_new();

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
    if (scr_hash_read_fd(xor_files[i], xor_fds[i], xor_headers[i]) < 0) {
      scr_err("Failed to read XOR header from %s @ %s:%d",
        xor_files[i], __FILE__, __LINE__
      );
      return 1;
    }
  }

  /* build header for missing XOR file */
  int partner_rank = -1;
  if (xor_set_size >= 2) {
    scr_hash_merge(xor_headers[0], xor_headers[1]);

    /* fetch our own file list from rank to our right */
    scr_hash* rhs_hash = scr_hash_get(xor_headers[1], SCR_KEY_COPY_XOR_PARTNER);
    scr_hash* current_hash = scr_hash_new();
    scr_hash_merge(current_hash, rhs_hash);
    scr_hash_set(xor_headers[0], SCR_KEY_COPY_XOR_CURRENT, current_hash);

    /* we are the partner to the rank to our left */
    scr_hash* lhs_hash = scr_hash_get(xor_headers[xor_set_size-1], SCR_KEY_COPY_XOR_CURRENT);
    scr_hash* partner_hash = scr_hash_new();
    scr_hash_merge(partner_hash, lhs_hash);
    scr_hash_set(xor_headers[0], SCR_KEY_COPY_XOR_PARTNER, partner_hash);

    /* get global rank of partner */
    if (scr_hash_util_get_int(lhs_hash, SCR_KEY_COPY_XOR_RANK, &partner_rank) != SCR_SUCCESS) {
      scr_err("Failed to read partner rank from XOR file header in %s @ %s:%d",
        xor_files[xor_set_size-1], __FILE__, __LINE__
      );
      return 1;
    }
  }

  /* get a pointer to the current hash for the missing rank */
  scr_hash* missing_current_hash = scr_hash_get(xor_headers[0], SCR_KEY_COPY_XOR_CURRENT);

  /* read the rank */
  int my_rank = -1;
  if (scr_hash_util_get_int(missing_current_hash, SCR_KEY_COPY_XOR_RANK, &my_rank) != SCR_SUCCESS) {
    scr_err("Failed to read rank from XOR file header in %s @ %s:%d",
      xor_files[0], __FILE__, __LINE__
    );
    return 1;
  }

  /* get the dataset */
  scr_dataset* dataset = scr_hash_get(xor_headers[0], SCR_KEY_COPY_XOR_DATASET);

  /* read the dataset id */
  int dset_id = -1;
  if (scr_dataset_get_id(dataset, &dset_id) != SCR_SUCCESS) {
    scr_err("Failed to read dataset id from XOR file header in %s @ %s:%d",
      xor_files[0], __FILE__, __LINE__
    );
    return 1;
  }

  /* read the ranks */
  int num_ranks = -1;
  if (scr_hash_util_get_int(xor_headers[0], SCR_KEY_COPY_XOR_RANKS, &num_ranks) != SCR_SUCCESS) {
    scr_err("Failed to read ranks from XOR file header in %s @ %s:%d",
      xor_files[0], __FILE__, __LINE__
    );
    return 1;
  }

  /* get name of partner's .scrfilemap */
  scr_path* path_partner_map = scr_path_from_str(".scr");
  scr_path_append_strf(path_partner_map, "%d.scrfilemap", partner_rank);

  /* extract partner's flush descriptor */
  scr_hash* flushdesc = scr_hash_new();
  scr_filemap* partner_map = scr_filemap_new();
  scr_filemap_read(path_partner_map, partner_map);
  scr_filemap_get_flushdesc(partner_map, dset_id, partner_rank, flushdesc);
  scr_filemap_delete(&partner_map);

  /* delete partner map path */
  scr_path_delete(&path_partner_map);

  /* determine whether we should preserve user directories */
  int preserve_dirs = 0;
  scr_hash_util_get_int(flushdesc, SCR_SCAVENGE_KEY_PRESERVE, &preserve_dirs);

  /* read the chunk size */
  unsigned long chunk_size = 0;
  if (scr_hash_util_get_unsigned_long(xor_headers[0], SCR_KEY_COPY_XOR_CHUNK, &chunk_size) != SCR_SUCCESS) {
    scr_err("Failed to read chunk size from XOR file header in %s @ %s:%d",
      xor_files[0], __FILE__, __LINE__
    );
    return 1;
  }

  /* determine number of files each member wrote in XOR set */
  for (i=0; i < xor_set_size; i++) {
    /* record the number of files for this rank */
    scr_hash* current_hash = scr_hash_get(xor_headers[i], SCR_KEY_COPY_XOR_CURRENT);
    if (scr_hash_util_get_int(current_hash, SCR_KEY_COPY_XOR_FILES, &num_files[i]) != SCR_SUCCESS) {
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
  char** user_rel_files         = (char**)         malloc(total_num_files * sizeof(char*));
  unsigned long* user_filesizes = (unsigned long*) malloc(total_num_files * sizeof(unsigned long));
  if (user_fds == NULL || user_files == NULL || user_rel_files == NULL || user_filesizes == NULL) {
    scr_err("Failed to allocate buffer memory @ %s:%d",
      __FILE__, __LINE__
    );
    return 1;
  }

  /* get file name, file size, and open each of the user files that we have */
  for (i=0; i < xor_set_size; i++) {
    scr_hash* current_hash = scr_hash_get(xor_headers[i], SCR_KEY_COPY_XOR_CURRENT);

    /* for each file belonging to this rank, get filename, filesize, and open file */
    for (j=0; j < num_files[i]; j++) {
      int offset = offsets[i] + j;

      /* get the meta data for this file */
      scr_meta* meta = scr_hash_get_kv_int(current_hash, SCR_KEY_COPY_XOR_FILE, j);
      if (meta == NULL) {
        scr_err("Failed to read meta data for file %d in %s @ %s:%d",
          j, xor_files[i], __FILE__, __LINE__
        );
        return 1;
      }

      /* record the filesize of this file */
      if (scr_meta_get_filesize(meta, &user_filesizes[offset]) != SCR_SUCCESS) {
        scr_err("Failed to read filesize field for file %d in %s @ %s:%d",
          j, xor_files[i], __FILE__, __LINE__
        );
        return 1;
      }

      /* get filename */
      char* origname;
      if (scr_meta_get_origname(meta, &origname) != SCR_SUCCESS) {
        scr_err("Failed to read original name for file %d in %s @ %s:%d",
          j, xor_files[i], __FILE__, __LINE__
        );
        return 1;
      }

      /* construct full path to user file */
      scr_path* path_user_full = scr_path_from_str(origname);
      if (preserve_dirs) {
        /* get original path of file */
        char* origpath;
        if (scr_meta_get_origpath(meta, &origpath) != SCR_SUCCESS) {
          scr_err("Failed to read original path for file %d in %s @ %s:%d",
            j, xor_files[i], __FILE__, __LINE__
          );
          return 1;
        }

        /* construct full path to file */
        scr_path_prepend_str(path_user_full, origpath);
      } else {
        /* construct full path to file */
        scr_path_prepend(path_user_full, path_dset);
      }

      /* reduce path to user file */
      scr_path_reduce(path_user_full);

      /* make a copy of the full path */
      user_files[offset] = scr_path_strdup(path_user_full);

      /* make a copy of relative path */
      scr_path* path_user_rel = scr_path_relative(path_dset, path_user_full);
      user_rel_files[offset] = scr_path_strdup(path_user_rel);
      scr_path_delete(&path_user_rel);

      /* free the full path */
      scr_path_delete(&path_user_full);

      /* open the file */
      if (i == 0) {
        /* create directory for file */
        scr_path* user_dir_path = scr_path_from_str(user_files[offset]);
        scr_path_reduce(user_dir_path);
        scr_path_dirname(user_dir_path);
        if (! scr_path_is_null(user_dir_path)) {
          char* user_dir = scr_path_strdup(user_dir_path);
          mode_t mode_dir = scr_getmode(1, 1, 1);
          if (scr_mkdir(user_dir, mode_dir) != SCR_SUCCESS) {
            scr_err("Failed to create directory for user file %s @ %s:%d",
              user_dir, __FILE__, __LINE__
            );
            return 1;
          }
          scr_free(&user_dir);
        }
        scr_path_delete(&user_dir_path);

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
  if (scr_hash_write_fd(xor_files[0], xor_fds[0], xor_headers[0]) < 0) {
    rc = 1;
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

      /* mark the file as incomplete */
      scr_meta* meta = scr_hash_get_kv_int(missing_current_hash, SCR_KEY_COPY_XOR_FILE, j);
      scr_meta_set_complete(meta, 0);

      rc = 1;
    }
  }
  /* TODO: we didn't record the filesize of the XOR file for the missing rank anywhere */

  /* create a filemap for this rank */
  scr_filemap* map = scr_filemap_new();
  if (map == NULL) {
    scr_err("Failed to allocate filemap @ %s:%d",
      __FILE__, __LINE__
    );
    return 1;
  }

  /* record the dataset information in the filemap */
  scr_filemap_set_dataset(map, dset_id, my_rank, dataset);

  /* write meta data for each of the user files and add each one to the filemap */
  for (j=0; j < num_files[0]; j++) {
    /* add user file to filemap and record meta data */
    char* user_file_relative = user_rel_files[j];
    scr_filemap_add_file(map, dset_id, my_rank, user_file_relative);
    scr_meta* meta = scr_hash_get_kv_int(missing_current_hash, SCR_KEY_COPY_XOR_FILE, j);
    scr_filemap_set_meta(map, dset_id, my_rank, user_file_relative, meta);
  }

  /* write meta data for xor file and add it to the filemap */
  scr_filemap_add_file(map, dset_id, my_rank, xor_files[0]);
  unsigned long full_chunk_filesize = scr_file_size(xor_files[0]);
  int missing_complete = 1;
  scr_meta* meta_chunk = scr_meta_new();
  scr_meta_set_filename(meta_chunk, xor_files[0]);
  scr_meta_set_filetype(meta_chunk, SCR_META_FILE_XOR);
  scr_meta_set_filesize(meta_chunk, full_chunk_filesize);
  /* TODO: remove this from meta file, for now it's needed in scr_index.c */
  scr_meta_set_ranks(meta_chunk, num_ranks);
  scr_meta_set_complete(meta_chunk, missing_complete);
  scr_filemap_set_meta(map, dset_id, my_rank, xor_files[0], meta_chunk);

  /* set expected number of files for the missing rank */
  int expected_num_files = scr_filemap_num_files(map, dset_id, my_rank);
  scr_filemap_set_expected_files(map, dset_id, my_rank, expected_num_files);

  /* compute, check, and store crc values with files */
  for (j=0; j < num_files[0]; j++) {
    /* compute crc on user file */
    char* user_file_relative = user_rel_files[j];
    if (scr_compute_crc(map, dset_id, my_rank, user_file_relative) != SCR_SUCCESS) {
      /* the crc check failed, so delete the file */
      scr_file_unlink(user_files[j]);
      rc = 1;
    }
  }
  if (scr_compute_crc(map, dset_id, my_rank, xor_files[0]) != SCR_SUCCESS) {
    /* the crc check failed, so delete the file */
    scr_file_unlink(xor_files[0]);
    rc = 1;
  }

  /* store flush descriptor */
  scr_filemap_set_flushdesc(map, dset_id, my_rank, flushdesc);

  /* write filemap for this rank */
  scr_path* path_map = scr_path_from_str(".scr");
  scr_path_append_strf(path_map, "%d.scrfilemap", my_rank);
  if (scr_filemap_write(path_map, map) != SCR_SUCCESS) {
    rc = 1;
  }
  scr_path_delete(&path_map);

  /* delete the map */
  scr_filemap_delete(&map);

  scr_meta_delete(&meta_chunk);

  /* delete the flush/scavenge descriptor */
  scr_hash_delete(&flushdesc);

  scr_free(&offset);

  for (i=0; i < total_num_files; i++) {
    scr_free(&user_rel_files[i]);
    scr_free(&user_files[i]);
  }

  scr_free(&user_filesizes);
  scr_free(&user_rel_files);
  scr_free(&user_files);
  scr_free(&user_fds);

  for (i=0; i < xor_set_size; i++) {
    scr_hash_delete(&xor_headers[i]);
  }

  for (i=0; i < xor_set_size; i++) {
    scr_free(&xor_files[i]);
  }

  scr_free(&xor_headers);
  scr_free(&xor_fds);
  scr_free(&xor_files);
  scr_free(&offsets);
  scr_free(&num_files);

  scr_free(&buffer_B);
  scr_free(&buffer_A);

  scr_path_delete(&path_dset);

  return rc;
}
