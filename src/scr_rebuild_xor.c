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
#include "scr_copy_xor.h"
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

/* execute xor operation with N-1 files and xor file: 
     open each XOR file and read header to get info for full files
     open each full file
     open missing full file
     open missing XOR file
     for all chunks
       read a chunk from missing file (xor file) into memory buffer A
       for each other file i
         read chunk from file i into memory buffer B
         merge chunks and store in memory buffer A
       write chunk in memory buffer A to missing file
     close all files
*/

int main(int argc, char* argv[])
{
  int i, j;
  int index = 1;

  /* print usage if not enough arguments were given */
  if (argc < 2) {
    printf("Usage: scr_rebuild_xor <size> <root> <missing_xor_filename> <ordered_remaining_xor_filenames>\n");
    return 1;
  }

  /* allocate buffers */
  char*  buffer_A = malloc(buffer_size * sizeof(char));
  char*  buffer_B = malloc(buffer_size * sizeof(char));
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
  struct scr_copy_xor_header* xor_headers = malloc(xor_set_size * sizeof(struct scr_copy_xor_header));
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
      scr_err("Opening xor segment file: scr_open(%s) errno=%d %m @ %s:%d",
              xor_files[i], errno, __FILE__, __LINE__
      );
      return 1;
    }

    /* read the header from this xor file */
    struct scr_copy_xor_header* h = &xor_headers[i];
    if (scr_copy_xor_header_read(xor_fds[i], h) != SCR_SUCCESS) {
      scr_err("Failed to read XOR header from %s @ %s:%d",
              xor_files[i], __FILE__, __LINE__
      );
      return 1;
    }
    scr_copy_xor_header_print(h);

    /* record the number of files for this rank, as well as, the number of files of his partner */
    num_files[i]   = h->my_nfiles;
    num_files[i-1] = h->partner_nfiles;
  }
  
  /* count the total number of files and set the offsets array */
  int total_num_files = 0;
  for (i=0; i < xor_set_size; i++) {
    offsets[i] = total_num_files;
    total_num_files += num_files[i];
  }

  /* allocate space for a file descriptor, file name pointer, and filesize for each full file */
  int* full_fds                 = (int*)           malloc(total_num_files * sizeof(int));
  char** full_files             = (char**)         malloc(total_num_files * sizeof(char*));
  unsigned long* full_filesizes = (unsigned long*) malloc(total_num_files * sizeof(unsigned long));
  if (full_fds == NULL || full_files == NULL || full_filesizes == NULL) {
    scr_err("Failed to allocate buffer memory @ %s:%d",
           __FILE__, __LINE__
    );
    return 1;
  }

  /* get file name, file size, and open each of the full files that we have */
  for (i=1; i < xor_set_size; i++) {
    struct scr_copy_xor_header* h = &xor_headers[i];
    for (j=0; j < num_files[i]; j++) {
      int offset = offsets[i] + j;
      struct scr_meta* m = &(h->my_files[j]);
      full_files[offset]     = m->filename;
      full_filesizes[offset] = m->filesize; 
      full_fds[offset]       = scr_open(full_files[offset], O_RDONLY);
      if (full_fds[offset] < 0) {
        scr_err("Opening full file for reading: scr_open(%s) errno=%d %m @ %s:%d",
                full_files[offset], errno, __FILE__, __LINE__
        );
        return 1;
      }
    }
  }

  /* now get file names, file sizes, and open files for the missing rank */
  /* assumes ranks to the right of the missing rank is the correct partner */
  i = 0;
  struct scr_copy_xor_header* h = &xor_headers[1];
  for (j=0; j < num_files[0]; j++) {
    int offset = offsets[0] + j;
    struct scr_meta* m = &(h->partner_files[j]);
    full_files[offset]     = m->filename;
    full_filesizes[offset] = m->filesize; 
    full_fds[offset]       = scr_open(full_files[offset], O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
    if (full_fds[offset] < 0) {
      scr_err("Opening full file for writing: scr_open(%s) errno=%d %m @ %s:%d",
              full_files[offset], errno, __FILE__, __LINE__
      );
      return 1;
    }
  }

  /* finally, open the xor file for the missing rank */
  xor_fds[0] = scr_open(xor_files[0], O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
  if (xor_fds[0] < 0) {
    scr_err("Opening xor file to be reconstructed: scr_open(%s) errno=%d %m @ %s:%d",
            xor_files[0], errno, __FILE__, __LINE__
    );
    return 1;
  }

  /* fill in the XOR header values for the XOR file of the missing rank */
  xor_headers[0].checkpoint_id = xor_headers[1].checkpoint_id;
  xor_headers[0].chunk_size    = xor_headers[1].chunk_size;
  xor_headers[0].nranks        = xor_headers[1].nranks;
  xor_headers[0].xor_nranks    = xor_headers[1].xor_nranks;

  /* record the list of ranks in this XOR set */
  xor_headers[0].xor_ranks = malloc(xor_headers[0].xor_nranks * sizeof(int));
  if (xor_headers[0].xor_ranks == NULL) {
    scr_err("Failed to allocate buffer memory @ %s:%d",
           __FILE__, __LINE__
    );
    return 1;
  }
  for (i=0; i < xor_headers[1].xor_nranks; i++) {
    xor_headers[0].xor_ranks[i] = xor_headers[1].xor_ranks[i];
  }

  /* get our rank and the meta data for each of our files */
  xor_headers[0].my_rank = xor_headers[1].partner_rank;
  scr_copy_xor_header_alloc_my_files(&xor_headers[0], xor_headers[1].partner_rank, xor_headers[1].partner_nfiles);
  if (xor_headers[1].partner_nfiles > 0 && xor_headers[0].my_files == NULL) {
    scr_err("Failed to allocate meta data buffers for my files @ %s:%d",
           __FILE__, __LINE__
    );
    return 1;
  }
  for (i=0; i < xor_headers[1].partner_nfiles; i++) {
    scr_meta_copy(&(xor_headers[0].my_files[i]), &(xor_headers[1].partner_files[i]));
  }

  /* get the rank and file meta data for our partner */
  xor_headers[0].partner_rank = xor_headers[xor_set_size-1].my_rank;
  scr_copy_xor_header_alloc_partner_files(&xor_headers[0], xor_headers[xor_set_size-1].my_rank, xor_headers[xor_set_size-1].my_nfiles);
  if (xor_headers[xor_set_size-1].my_nfiles > 0 && xor_headers[0].partner_files == NULL) {
    scr_err("Failed to allocate meta data buffers for partner files @ %s:%d",
           __FILE__, __LINE__
    );
    return 1;
  }
  for (i=0; i < xor_headers[xor_set_size-1].my_nfiles; i++) {
    scr_meta_copy(&(xor_headers[0].partner_files[i]), &(xor_headers[xor_set_size-1].my_files[i]));
  }

  int rc = 0;

  /* write the header to the XOR file of the missing rank */
  if (scr_copy_xor_header_write(xor_fds[0], &xor_headers[0]) != SCR_SUCCESS) {
    rc = 1;
  }
  scr_copy_xor_header_print(&xor_headers[0]);

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
  unsigned long chunk_size = xor_headers[0].chunk_size;
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
          if (scr_read_pad_n(num_files[i], &full_files[offsets[i]], &full_fds[offsets[i]],
                             buffer_B, count, offset[i], &full_filesizes[offsets[i]]) != SCR_SUCCESS)
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
        if (scr_write_pad_n(num_files[0], &full_files[0], &full_fds[0],
                            buffer_A, count, write_pos, &full_filesizes[0]) != SCR_SUCCESS)
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

  /* close each of the full files */
  for (i=0; i < total_num_files; i++) {
    if (scr_close(full_files[i], full_fds[i]) != SCR_SUCCESS) {
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
      unlink(full_files[j]);
    }
    unlink(xor_files[0]);
    return 1;
  }

  /* check that filesizes are correct */
  unsigned long filesize;
  for (j=0; j < num_files[0]; j++) {
    filesize = scr_filesize(full_files[j]);
    if (filesize != xor_headers[0].my_files[j].filesize) {
      /* the filesize check failed, so delete the file */
      unlink(full_files[j]);

      xor_headers[0].my_files[j].complete = 0;

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

  /* write .scr file for each of the full files and add each one to the filemap */
  for (j=0; j < num_files[0]; j++) {
    if (scr_meta_write(full_files[j], &(xor_headers[0].my_files[j])) != SCR_SUCCESS) {
      rc = 1;
    }
    scr_filemap_add_file(map, xor_headers[0].checkpoint_id, xor_headers[0].my_rank, full_files[j]);
  }

  /* write .scr file for xor file and add it to the filemap */
  int missing_complete = 1;
  struct scr_meta meta;
  scr_meta_set(&meta, xor_files[0], xor_headers[0].my_rank, xor_headers[0].nranks, xor_headers[0].checkpoint_id, SCR_FILE_XOR, missing_complete);
  if (scr_meta_write(xor_files[0], &meta) != SCR_SUCCESS) {
    rc = 1;
  }
  scr_filemap_add_file(map, xor_headers[0].checkpoint_id, xor_headers[0].my_rank, xor_files[0]);

  /* set expected number of files for the missing rank */
  scr_filemap_set_expected_files(map, xor_headers[0].checkpoint_id, xor_headers[0].my_rank,
           scr_filemap_num_files(map, xor_headers[0].checkpoint_id, xor_headers[0].my_rank)
  );

  /* write filemap for this rank, and delete the map */
  char map_file[SCR_MAX_FILENAME];
  sprintf(map_file, "%d.scrfilemap", xor_headers[0].my_rank);
  if (scr_filemap_write(map_file, map) != SCR_SUCCESS) {
    rc = 1;
  }
  scr_filemap_delete(map);

  /* compute, check, and store crc values with files */
  for (j=0; j < num_files[0]; j++) {
    if (scr_compute_crc(full_files[j]) != SCR_SUCCESS) {
      /* the crc check failed, so delete the file */
      unlink(full_files[j]);

      /* also record that the file is incomplete in the meta file */
      xor_headers[0].my_files[j].complete = 0;
      scr_meta_write(full_files[j], &(xor_headers[0].my_files[j]));

      rc = 1;
    }
  }
  if (scr_compute_crc(xor_files[0]) != SCR_SUCCESS) {
    /* the crc check failed, so delete the file */
    unlink(xor_files[0]);

    /* also record that the file is incomplete in the meta file */
    meta.complete = 0;
    scr_meta_write(xor_files[0], &meta);

    rc = 1;
  }

  if (offset != NULL) {
    free(offset);
    offset = NULL;
  }

  if (full_filesizes != NULL) {
    free(full_filesizes);
    full_filesizes = NULL;
  }
  if (full_files != NULL) {
    free(full_files);
    full_files = NULL;
  }
  if (full_fds != NULL) {
    free(full_fds);
    full_fds = NULL;
  }

  for (i=0; i < xor_set_size; i++) {
    scr_copy_xor_header_free(&xor_headers[i]);
  }

  for (i=0; i < xor_set_size; i++) {
    free(xor_files[i]);
    xor_files[i] = NULL;
  }

  if (xor_headers != NULL) {
    free(xor_headers);
    xor_headers = NULL;
  }
  if (xor_fds != NULL) {
    free(xor_fds);
    xor_fds = NULL;
  }
  if (xor_files != NULL) {
    free(xor_files);
    xor_files = NULL;
  }
  if (offsets != NULL) {
    free(offsets);
    offsets = NULL;
  }
  if (num_files != NULL) {
    free(num_files);
    num_files = NULL;
  }

  if (buffer_B != NULL) {
    free(buffer_B);
    buffer_B = NULL;
  }
  if (buffer_A != NULL) {
    free(buffer_A);
    buffer_A = NULL;
  }

  return rc;
}
