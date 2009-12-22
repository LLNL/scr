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

#ifndef SCR_COPY_XOR_H
#define SCR_COPY_XOR_H

#include "scr.h"
#include "scr_io.h"
#include "scr_meta.h"

/* TODO: could record the other ranks in this xor set, or the xor set id? */
struct scr_copy_xor_header {
  int version;
  int nranks;
  int xor_nranks;
  int* xor_ranks;
  int checkpoint_id;
  unsigned long chunk_size;
  int my_rank;
  int my_nfiles;
  struct scr_meta* my_files;
  int partner_rank;
  int partner_nfiles;
  struct scr_meta* partner_files;
};

/* print contents of header structure */
int scr_copy_xor_header_print(struct scr_copy_xor_header* h);

/* given a meta structure, file out an xor file header structure */
int scr_copy_xor_header_alloc_my_files(struct scr_copy_xor_header* h, int rank, int nfiles);

/* given a meta structure, file out an xor file header structure */
int scr_copy_xor_header_alloc_partner_files(struct scr_copy_xor_header* h, int rank, int nfiles);

int scr_copy_xor_header_free(struct scr_copy_xor_header* h);

int scr_copy_xor_meta_read(int fd, struct scr_meta* m);

int scr_copy_xor_meta_write(int fd, struct scr_meta* m);

/* given an open file descriptor, read xor header from file */
int scr_copy_xor_header_read(int fd, struct scr_copy_xor_header* h);

/* given an open file descriptor, write xor header to file */
int scr_copy_xor_header_write(int fd, struct scr_copy_xor_header* h);

#endif
