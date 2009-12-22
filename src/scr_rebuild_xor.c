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
  char*  buffer_A = malloc(sizeof(char) * buffer_size);
  char*  buffer_B = malloc(sizeof(char) * buffer_size);

  /* read in the size of the XOR set */
  int xor_set_size = atoi(argv[index++]);

  /* allocate memory for data structures based on the XOR set size */
  char** xor_files  = malloc(sizeof(char*) * xor_set_size);
  int*   xor_fds    = malloc(sizeof(int) * xor_set_size);
  struct scr_copy_xor_header* xor_headers = malloc(sizeof(struct scr_copy_xor_header) * xor_set_size);
  int*   num_files  = malloc(sizeof(int) * xor_set_size);
  int*   offsets    = malloc(sizeof(int) * xor_set_size);

  /* read in the rank of the missing process (the root) */
  int root = atoi(argv[index++]);

  /* read in the missing xor filename */
  xor_files[0] = strdup(argv[index++]);

  /* read in the xor filenames (expected to be in order of XOR segment number) */
  for (i=0; i < xor_set_size; i++) {
    if (i == root) { continue; }
    j = i - root;
    if (j < 0) { j+= xor_set_size; }
    xor_files[j] = strdup(argv[index++]);
  }

  /* open each of the xor files and read in the headers */
  for (i=1; i < xor_set_size; i++) {
    /* open each xor file for reading */
    xor_fds[i] = scr_open(xor_files[i], O_RDONLY);
    if (xor_fds[i] < 0) {
      scr_err("Opening xor segment file: scr_open(%s) errno=%d %m @ file %s:%d",
              xor_files[i], errno, __FILE__, __LINE__
      );
      return 1;
    }

    /* read the header from this xor file */
    struct scr_copy_xor_header* h = &xor_headers[i];
    scr_copy_xor_header_read(xor_fds[i], h);
    scr_copy_xor_header_print(h);

    num_files[i]   = h->my_nfiles;
    num_files[i-1] = h->partner_nfiles;
  }
  
  /* get the total number of files and set the offsets array */
  int total_num_files = 0;
  for (i=0; i < xor_set_size; i++) {
    offsets[i] = total_num_files;
    total_num_files += num_files[i];
  }

  /* allocate space for a file descriptor, file name pointer, and filesize for each file */
  int* full_fds                 = (int*) malloc(sizeof(int) * total_num_files);
  char** full_files             = (char**) malloc(sizeof(char*) * total_num_files);
  unsigned long* full_filesizes = (unsigned long*) malloc(sizeof(unsigned long) * total_num_files);

  /* fill in the values for all xor files that we currently have */
  for (i=1; i < xor_set_size; i++) {
    struct scr_copy_xor_header* h = &xor_headers[i];
    for (j=0; j < num_files[i]; j++) {
      int offset = offsets[i] + j;
      struct scr_meta* m = &(h->my_files[j]);
      full_files[offset]     = m->filename;
      full_filesizes[offset] = m->filesize; 
      full_fds[offset]       = scr_open(full_files[offset], O_RDONLY);
      if (full_fds[offset] < 0) {
        scr_err("Opening full file for reading: scr_open(%s) errno=%d %m @ file %s:%d",
                full_files[offset], errno, __FILE__, __LINE__
        );
        return 1;
      }
    }
  }

  /* TODO: assumes ranks to the right of the missing rank is the correct partner */
  /* fill in the values for the missing rank */
  i = 0;
  struct scr_copy_xor_header* h = &xor_headers[1];
  for (j=0; j < num_files[0]; j++) {
    int offset = offsets[0] + j;
    struct scr_meta* m = &(h->partner_files[j]);
    full_files[offset]     = m->filename;
    full_filesizes[offset] = m->filesize; 
    full_fds[offset]       = scr_open(full_files[offset], O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
    if (full_fds[offset] < 0) {
      scr_err("Opening full file for writing: scr_open(%s) errno=%d %m @ file %s:%d",
              full_files[offset], errno, __FILE__, __LINE__
      );
      return 1;
    }
  }

  /* execute xor operation with N-1 files and xor file: 
       open missing file for read/write
       open other files as read-only
       for all chunks
         read a chunk from missing file (xor file) into memory buffer A
         for each other file i
           read chunk from file i into memory buffer B
           merge chunks and store in memory buffer A
         write chunk in memory buffer A to missing file
       close all files
  */

  /* open missing xor file for writing */
  xor_fds[0] = scr_open(xor_files[0], O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
  if (xor_fds[0] < 0) {
    scr_err("Opening xor file to be reconstructed: scr_open(%s) errno=%d %m @ file %s:%d",
            xor_files[0], errno, __FILE__, __LINE__
    );
    return 1;
  }

  int nranks               = xor_headers[1].nranks;
  int missing_rank         = xor_headers[1].partner_rank;
  int checkpoint_id        = xor_headers[1].checkpoint_id;
  unsigned long chunk_size = xor_headers[1].chunk_size;

  /* write length of filename and trailing NULL, filename, then filesize to chunk file */
  xor_headers[0].nranks = xor_headers[1].nranks;
  xor_headers[0].xor_nranks = xor_headers[1].xor_nranks;
  xor_headers[0].xor_ranks = malloc(sizeof(int) * xor_headers[0].xor_nranks);
  for (i=0; i < xor_headers[1].xor_nranks; i++) {
    xor_headers[0].xor_ranks[i] = xor_headers[1].xor_ranks[i];
  }

  xor_headers[0].checkpoint_id = xor_headers[1].checkpoint_id;
  xor_headers[0].chunk_size = xor_headers[1].chunk_size;

  xor_headers[0].my_rank = xor_headers[1].partner_rank;
  scr_copy_xor_header_alloc_my_files(&xor_headers[0], xor_headers[1].partner_rank, xor_headers[1].partner_nfiles);
  for (i=0; i < xor_headers[1].partner_nfiles; i++) {
    scr_copy_meta(&(xor_headers[0].my_files[i]), &(xor_headers[1].partner_files[i]));
  }

  xor_headers[0].partner_rank = xor_headers[xor_set_size-1].my_rank;
  scr_copy_xor_header_alloc_partner_files(&xor_headers[0], xor_headers[xor_set_size-1].my_rank, xor_headers[xor_set_size-1].my_nfiles);
  for (i=0; i < xor_headers[xor_set_size-1].my_nfiles; i++) {
    scr_copy_meta(&(xor_headers[0].partner_files[i]), &(xor_headers[xor_set_size-1].my_files[i]));
  }

  scr_copy_xor_header_write(xor_fds[0], &xor_headers[0]);
  scr_copy_xor_header_print(&xor_headers[0]);

  /* Pipelined XOR Reduce to root */
  unsigned long* offset = malloc(xor_set_size * sizeof(unsigned long));
  for (i=0; i < xor_set_size; i++) { offset[i] = 0; }

  unsigned long write_pos = 0;
  int chunk_id;
  for (chunk_id = 0; chunk_id < xor_set_size; chunk_id++) {
    size_t nread = 0;
    while (nread < chunk_size) {
      size_t count = chunk_size - nread;
      if (count > buffer_size) { count = buffer_size; }

      memset(buffer_A, 0, count);

      for (i=1; i < xor_set_size; i++) {
        /* read the next set of bytes for this chunk from my file into send_buf */
        if (chunk_id != ((i + root) % xor_set_size)) {
          scr_read_pad_n(num_files[i], &full_fds[offsets[i]], buffer_B, count, offset[i], &full_filesizes[offsets[i]]);
          offset[i] += count;
        } else {
          scr_read(xor_fds[i], buffer_B, count);
        }

        /* TODO: XORing with unsigned long would be faster here (if chunk size is multiple of this size) */
        /* merge the blocks via xor operation */
        for (j = 0; j < count; j++) { buffer_A[j] ^= buffer_B[j]; }
      }

      if (chunk_id != root) {
        /* write send block to send chunk file */
        scr_write_pad_n(num_files[0], &full_fds[0], buffer_A, count, write_pos, &full_filesizes[0]);
        write_pos += count;
      } else {
        /* TODO: write chunk to xor file */
        scr_write(xor_fds[0], buffer_A, count);
      }

      nread += count;
    }
  }

  /* don't need fsync here (read-only) */
  for (i=0; i < total_num_files; i++) {
    close(full_fds[i]);
  }

  for (i=0; i < xor_set_size; i++) {
    close(xor_fds[i]);
  }

  // TODO: need to get real rank, maybe store this info in chunk header (along with checkpoint_id, complete, crc?)
  // TODO: copy header read/write logic to functions

  /* create a filemap for this rank */
  struct scr_filemap* map = scr_filemap_new();

  /* write .scr file for full file and add the file to the filemap */
  for (j=0; j < num_files[0]; j++) {
    scr_write_meta(full_files[j], &(xor_headers[0].my_files[j]));
    scr_filemap_add_file(map, checkpoint_id, missing_rank, full_files[j]);
  }

  /* write .scr file for xor file and add it to the filemap */
  int missing_complete = 1;
  struct scr_meta meta;
  scr_set_meta(&meta, xor_files[0], missing_rank, nranks, checkpoint_id, SCR_FILE_XOR, missing_complete);
  scr_write_meta(xor_files[0], &meta);
  scr_filemap_add_file(map, checkpoint_id, missing_rank, xor_files[0]);

  /* set expected number of files for this rank, write filemap for this rank, and delete the map */
  char map_file[SCR_MAX_FILENAME];
  sprintf(map_file, "%d.scrfilemap", missing_rank);
  scr_filemap_set_expected_files(map, checkpoint_id, missing_rank,
        scr_filemap_num_files(map, checkpoint_id, missing_rank)
  );
  scr_filemap_write(map_file, map);
  scr_filemap_delete(map);

  free(full_fds);
  free(full_files);
  free(full_filesizes);

  free(xor_files);
  free(xor_fds);
  free(xor_headers);
  free(num_files);
  free(offsets);

  free(buffer_A);
  free(buffer_B);

  return 0;
}
