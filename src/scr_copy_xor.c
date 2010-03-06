/*
 * Copyright (c) 2009, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory.
 * Written by Adam Moody <moody20@llnl.gov>.
 * LLNL-CODE-411039.
 * All rights reserved.
 * This file is part of The Scalablel Checkpoint / Restart (SCR) library.
 * For details, see https://sourceforge.net/projects/scalablecr/
 * Please also read this file: LICENSE.TXT.
*/

#include "scr.h"
#include "scr_copy_xor.h"
#include "scr_io.h"
#include "scr_meta.h"
#include "scr_err.h"

#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>

/* variable length args */
#include <stdarg.h>
#include <errno.h>

#define SCR_XOR_VERSION (3)

/* print contents of header structure */
int scr_copy_xor_header_print(struct scr_copy_xor_header* h)
{
  printf("version: %d, nranks: %d, xor_nranks: %d, checkpoint_id: %d, chunk_size: %lu, my_rank: %d, my_nfiles: %d, partner_rank: %d, partner_nfiles: %d\n",
         h->version, h->nranks, h->xor_nranks, h->checkpoint_id, h->chunk_size, h->my_rank, h->my_nfiles, h->partner_rank, h->partner_nfiles
  );
  return SCR_SUCCESS;
}

/* given a meta structure, file out an xor file header structure */
int scr_copy_xor_header_alloc_my_files(struct scr_copy_xor_header* h, int rank, int nfiles)
{
  h->my_rank = rank;
  h->my_nfiles = nfiles;
  if (nfiles > 0) {
    h->my_files  = (struct scr_meta*) malloc(nfiles * sizeof(struct scr_meta));
    /* TODO: check for null */
  } else {
    h->my_files = NULL;
  }

  return SCR_SUCCESS;
}

/* given a meta structure, file out an xor file header structure */
int scr_copy_xor_header_alloc_partner_files(struct scr_copy_xor_header* h, int rank, int nfiles)
{
  h->partner_rank = rank;
  h->partner_nfiles = nfiles;
  if (nfiles > 0) {
    h->partner_files  = (struct scr_meta*) malloc(nfiles * sizeof(struct scr_meta));
    /* TODO: check for null */
  } else {
    h->partner_files = NULL;
  }

  return SCR_SUCCESS;
}

int scr_copy_xor_header_free(struct scr_copy_xor_header* h)
{
  h->xor_nranks = 0;
  if (h->xor_ranks != NULL) {
    free(h->xor_ranks);
    h->xor_ranks = NULL;
  }

  h->my_nfiles  = 0;
  if (h->my_files != NULL) {
    free(h->my_files);
    h->my_files  = NULL;
  }

  h->partner_nfiles = 0;
  if (h->partner_files != NULL) {
    free(h->partner_files);
    h->partner_files = NULL;
  }

  return SCR_SUCCESS;
}

int scr_copy_xor_meta_read(int fd, struct scr_meta* m)
{
  int filename_length = 0;

  scr_read(fd, &m->rank,           sizeof(m->rank));
  scr_read(fd, &m->ranks,          sizeof(m->ranks));
  scr_read(fd, &m->checkpoint_id,  sizeof(m->checkpoint_id));
  scr_read(fd, &m->filetype,       sizeof(m->filetype));

  scr_read(fd, &filename_length,   sizeof(filename_length));
  scr_read(fd, m->filename,        filename_length);
  scr_read(fd, &m->filesize,       sizeof(m->filesize));
  scr_read(fd, &m->complete,       sizeof(m->complete));
  scr_read(fd, &m->crc32_computed, sizeof(m->crc32_computed));
  scr_read(fd, &m->crc32,          sizeof(m->crc32));

  return SCR_SUCCESS;
}

int scr_copy_xor_meta_write(int fd, struct scr_meta* m)
{
  int filename_length = strlen(m->filename) + 1;

  scr_write(fd, &m->rank,           sizeof(m->rank));
  scr_write(fd, &m->ranks,          sizeof(m->ranks));
  scr_write(fd, &m->checkpoint_id,  sizeof(m->checkpoint_id));
  scr_write(fd, &m->filetype,       sizeof(m->filetype));

  scr_write(fd, &filename_length,   sizeof(filename_length));
  scr_write(fd, m->filename,        filename_length);
  scr_write(fd, &m->filesize,       sizeof(m->filesize));
  scr_write(fd, &m->complete,       sizeof(m->complete));
  scr_write(fd, &m->crc32_computed, sizeof(m->crc32_computed));
  scr_write(fd, &m->crc32,          sizeof(m->crc32));

  return SCR_SUCCESS;
}

/* given an open file descriptor, read xor header from file */
int scr_copy_xor_header_read(int fd, struct scr_copy_xor_header* h)
{
  /* read the header from the xor file */
  int i;
  int xor_version;
  scr_read(fd, &xor_version, sizeof(xor_version));
  h->version = xor_version;
  if (xor_version == 3) {
    /* read the number of ranks and the ranks in the xor set used to build this file */
    scr_read(fd, &h->nranks,         sizeof(h->nranks));
    scr_read(fd, &h->xor_nranks,     sizeof(h->xor_nranks));
    if (h->xor_nranks > 0) {
      h->xor_ranks = (int*) malloc(h->xor_nranks * sizeof(int));
      for (i=0; i < h->xor_nranks; i++) {
        scr_read(fd, &(h->xor_ranks[i]), sizeof(int));
      }
    } else {
      h->xor_ranks = NULL;
    }

    /* read the checkpoint_id and the chunk size */
    scr_read(fd, &h->checkpoint_id,  sizeof(h->checkpoint_id));
    scr_read(fd, &h->chunk_size,     sizeof(h->chunk_size));

    /* read my rank and the meta data for each of my files */
    scr_read(fd, &h->my_rank,        sizeof(h->my_rank));
    scr_read(fd, &h->my_nfiles,      sizeof(h->my_nfiles));
    if (h->my_nfiles > 0) {
      h->my_files = (struct scr_meta*) malloc(h->my_nfiles * sizeof(struct scr_meta));
      for (i=0; i < h->my_nfiles; i++) {
        scr_copy_xor_meta_read(fd, &(h->my_files[i]));
      }
    } else {
      h->my_files = NULL;
    }

    /* read my partner's rank and the meta data for each of his files */
    scr_read(fd, &h->partner_rank,        sizeof(h->partner_rank));
    scr_read(fd, &h->partner_nfiles,      sizeof(h->partner_nfiles));
    if (h->partner_nfiles > 0) {
      h->partner_files = (struct scr_meta*) malloc(h->partner_nfiles * sizeof(struct scr_meta));
      for (i=0; i < h->partner_nfiles; i++) {
        scr_copy_xor_meta_read(fd, &(h->partner_files[i]));
      }
    } else {
      h->partner_files = NULL;
    }

  } else {
    scr_abort(-1, "Unknown XOR file format: %d", xor_version);
  }

  return SCR_SUCCESS;
}

/* given an open file descriptor, write xor header to file */
int scr_copy_xor_header_write(int fd, struct scr_copy_xor_header* h)
{
  /* write length of filename and trailing NULL, filename, then filesize to chunk file */
  int i;

  /* write the file version */
  int xor_version = SCR_XOR_VERSION;
  scr_write(fd, &xor_version, sizeof(xor_version));
  h->version = xor_version;

  /* write the number of ranks and the list of ranks in our xor set */
  scr_write(fd, &h->nranks,     sizeof(h->nranks));
  scr_write(fd, &h->xor_nranks, sizeof(h->xor_nranks));
  for (i=0; i < h->xor_nranks; i++) {
    scr_write(fd, &(h->xor_ranks[i]), sizeof(int));
  }

  /* write the checkpoint id and the chunk size */
  scr_write(fd, &h->checkpoint_id,  sizeof(h->checkpoint_id));
  scr_write(fd, &h->chunk_size,     sizeof(h->chunk_size));

  /* write my rank and the meta data for each of my files */
  scr_write(fd, &h->my_rank,        sizeof(h->my_rank));
  scr_write(fd, &h->my_nfiles,      sizeof(h->my_nfiles));
  for (i=0; i < h->my_nfiles; i++) {
    scr_copy_xor_meta_write(fd, &(h->my_files[i]));
  }

  /* write my partner's rank and the meta data for each of his files */
  scr_write(fd, &h->partner_rank,   sizeof(h->partner_rank));
  scr_write(fd, &h->partner_nfiles, sizeof(h->partner_nfiles));
  for (i=0; i < h->partner_nfiles; i++) {
    scr_copy_xor_meta_write(fd, &(h->partner_files[i]));
  }

  return SCR_SUCCESS;
}
