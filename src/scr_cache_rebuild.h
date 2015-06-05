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

#ifndef SCR_CACHE_REBUILD_H
#define SCR_CACHE_REBUILD_H

#include "mpi.h"
#include "scr_filemap.h"

/*
=========================================
File Copy Functions
=========================================
*/

/* copy file operation flags: copy file vs. move file */
#define COPY_FILES 0
#define MOVE_FILES 1

/* given a file name, a rank to send to, a rank to receive from,
 * and a directory to receive into, return full path of incoming file */
int scr_swap_file_names(
  const char* file_send, /* INPUT - full path to file to send */
  int rank_send,         /* INPUT - rank to send file to */
  char* file_recv,       /* OUTPUT - name of file that will be received, if any */
  size_t size_recv,      /* INPUT - size of file_recv buffer in bytes */
  int rank_recv,         /* INPUT - rank to receive file from */
  const char* dir_recv,  /* INPUT - directory in which to receive file */
  MPI_Comm comm          /* communicator for ranks */
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

/* since on a restart we may end up with more or fewer ranks on a node than the previous run,
 * rely on the master to read in and distribute the filemap to other ranks on the node */
int scr_scatter_filemaps(scr_filemap* my_map);

/* distribute and rebuild files in cache */
int scr_cache_rebuild(scr_filemap* map);

/* remove any dataset ids from flush file which are not in cache,
 * and add any datasets in cache that are not in the flush file */
int scr_flush_file_rebuild(const scr_filemap* map);

#endif
