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

#ifndef SCR_REDDESC_APPLY_H
#define SCR_REDDESC_APPLY_H

#include "scr_hash.h"
#include "scr_meta.h"
#include "scr_filemap.h"
#include "scr_reddesc.h"

/* copy file operation flags: copy file vs. move file */
#define COPY_FILES 0
#define MOVE_FILES 1

/*
=========================================
File Copy Functions
=========================================
*/

int scr_swap_file_names(
  const char* file_send, int rank_send,
        char* file_recv, size_t size_recv, int rank_recv,
  const char* dir_recv, MPI_Comm comm
);

/* scr_swap_files -- copy or move a file from one node to another
 * if swap_type = COPY_FILES
 *   if file_send != NULL, send file_send to rank_send, who will make a copy,
 *   copy file from rank_recv if there is one to receive
 * if swap_type = MOVE_FILES
 *   if file_send != NULL, move file_send to rank_send
 *   save file from rank_recv if there is one to receive
 *   To conserve space (e.g., RAM disc), if file_send exists,
 *   any incoming file will overwrite file_send in place, one block at a time.
 *   It is then truncated and renamed according the size and name of the incoming file,
 *   or it is deleted (moved) if there is no incoming file.
 */
int scr_swap_files(
  int swap_type,
  const char* file_send, scr_meta* meta_send, int rank_send,
  const char* file_recv, scr_meta* meta_recv, int rank_recv,
  MPI_Comm comm
);

/* apply redundancy scheme to file and return number of bytes copied in bytes parameter */
int scr_reddesc_apply(scr_filemap* map, const scr_reddesc* c, int id, double* bytes);

#endif
