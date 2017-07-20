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

/* All rights reserved. This program and the accompanying materials
 * are made available under the terms of the BSD-3 license which accompanies this
 * distribution in LICENSE.TXT
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the BSD-3  License in
 * LICENSE.TXT for more details.
 *
 * GOVERNMENT LICENSE RIGHTS-OPEN SOURCE SOFTWARE
 * The Government's rights to use, modify, reproduce, release, perform,
 * display, or disclose this software are subject to the terms of the BSD-3
 * License as provided in Contract No. B609815.
 * Any reproduction of computer software, computer software documentation, or
 * portions thereof marked with this legend must also reproduce the markings.
 *
 * Author: Christopher Holguin <christopher.a.holguin@intel.com>
 *
 * (C) Copyright 2015-2016 Intel Corporation.
 */

#include "scr_globals.h"

/*
=========================================
File Copy Functions
=========================================
*/

/* copy file operation flags: copy file vs. move file */
#define COPY_FILES 0
#define MOVE_FILES 1

int scr_swap_file_names(
  const char* file_send, int rank_send,
        char* file_recv, size_t size_recv, int rank_recv,
  const char* dir_recv, MPI_Comm comm)
{
  int rc = SCR_SUCCESS;

  /* determine whether we have a file to send */
  int have_outgoing = 0;
  if (rank_send != MPI_PROC_NULL &&
      file_send != NULL &&
      strcmp(file_send, "") != 0)
  {
    have_outgoing = 1;
  } else {
    /* nothing to send, make sure to use PROC_NULL in sendrecv call */
    rank_send = MPI_PROC_NULL;
  }

  /* determine whether we are expecting to receive a file */
  int have_incoming = 0;
  if (rank_recv != MPI_PROC_NULL &&
      dir_recv != NULL &&
      strcmp(dir_recv, "") != 0)
  {
    have_incoming = 1;
  } else {
    /* nothing to recv, make sure to use PROC_NULL in sendrecv call */
    rank_recv = MPI_PROC_NULL;
  }

  /* exchange file names with partners, note that we initialize
   * file_recv_orig to NULL in case we recv from MPI_PROC_NULL */
  char* file_recv_orig = NULL;
  scr_str_sendrecv(file_send, rank_send, &file_recv_orig, rank_recv, comm);

  /* define the path to store our partner's file */
  if (have_incoming) {
    /* set path to file name */
    scr_path* path_recv = scr_path_from_str(file_recv_orig);
    scr_path_basename(path_recv);
    scr_path_prepend_str(path_recv, dir_recv);
    scr_path_strcpy(file_recv, size_recv, path_recv);
    scr_path_delete(&path_recv);

    /* free the file name we received */
    scr_free(&file_recv_orig);
  }

  return rc;
}

static int scr_swap_files_copy(
  int have_outgoing, const char* file_send, scr_meta* meta_send, int rank_send, uLong* crc32_send,
  int have_incoming, const char* file_recv, scr_meta* meta_recv, int rank_recv, uLong* crc32_recv,
  MPI_Comm comm)
{
  int rc = SCR_SUCCESS;
  MPI_Request request[2];
  MPI_Status  status[2];

  /* allocate MPI send buffer */
  char *buf_send = NULL;
  if (have_outgoing) {
    buf_send = (char*) scr_align_malloc(scr_mpi_buf_size, scr_page_size);
    if (buf_send == NULL) {
      scr_abort(-1, "Allocating memory: malloc(%ld) errno=%d %s @ %s:%d",
              scr_mpi_buf_size, errno, strerror(errno), __FILE__, __LINE__
      );
      return SCR_FAILURE;
    }
  }

  /* allocate MPI recv buffer */
  char *buf_recv = NULL;
  if (have_incoming) {
    buf_recv = (char*) scr_align_malloc(scr_mpi_buf_size, scr_page_size);
    if (buf_recv == NULL) {
      scr_abort(-1, "Allocating memory: malloc(%ld) errno=%d %s @ %s:%d",
              scr_mpi_buf_size, errno, strerror(errno), __FILE__, __LINE__
      );
      return SCR_FAILURE;
    }
  }

  /* open the file to send: read-only mode */
  int fd_send = -1;
  if (have_outgoing) {
    fd_send = scr_open(file_send, O_RDONLY);
    if (fd_send < 0) {
      scr_abort(-1, "Opening file for send: scr_open(%s, O_RDONLY) errno=%d %s @ %s:%d",
              file_send, errno, strerror(errno), __FILE__, __LINE__
      );
    }
  }

  /* open the file to recv: truncate, write-only mode */
  int fd_recv = -1;
  if (have_incoming) {
    mode_t mode_file = scr_getmode(1, 1, 0);
    fd_recv = scr_open(file_recv, O_WRONLY | O_CREAT | O_TRUNC, mode_file);
    if (fd_recv < 0) {
      scr_abort(-1, "Opening file for recv: scr_open(%s, O_WRONLY | O_CREAT | O_TRUNC, ...) errno=%d %s @ %s:%d",
              file_recv, errno, strerror(errno), __FILE__, __LINE__
      );
    }
  }

  /* exchange file chunks */
  int nread, nwrite;
  int sending = 0;
  if (have_outgoing) {
    sending = 1;
  }
  int receiving = 0;
  if (have_incoming) {
    receiving = 1;
  }
  while (sending || receiving) {
    /* if we are still receiving a file, post a receive */
    if (receiving) {
      MPI_Irecv(buf_recv, scr_mpi_buf_size, MPI_BYTE, rank_recv, 0, comm, &request[0]);
    }

    /* if we are still sending a file, read a chunk, send it, and wait */
    if (sending) {
      nread = scr_read(file_send, fd_send, buf_send, scr_mpi_buf_size);
      if (scr_crc_on_copy && nread > 0) {
        *crc32_send = crc32(*crc32_send, (const Bytef*) buf_send, (uInt) nread);
      }
      if (nread < 0) {
        nread = 0;
      }
      MPI_Isend(buf_send, nread, MPI_BYTE, rank_send, 0, comm, &request[1]);
      MPI_Wait(&request[1], &status[1]);
      if (nread < scr_mpi_buf_size) {
        sending = 0;
      }
    }

    /* if we are still receiving a file,
     * wait on our receive to complete and write the data */
    if (receiving) {
      MPI_Wait(&request[0], &status[0]);
      MPI_Get_count(&status[0], MPI_BYTE, &nwrite);
      if (scr_crc_on_copy && nwrite > 0) {
        *crc32_recv = crc32(*crc32_recv, (const Bytef*) buf_recv, (uInt) nwrite);
      }
      scr_write(file_recv, fd_recv, buf_recv, nwrite);
      if (nwrite < scr_mpi_buf_size) {
        receiving = 0;
      }
    }
  }

  /* close the files */
  if (have_outgoing) {
    scr_close(file_send, fd_send);
  }
  if (have_incoming) {
    scr_close(file_recv, fd_recv);
  }

  /* set crc field on our file if it hasn't been set already */
  if (scr_crc_on_copy && have_outgoing) {
    uLong meta_send_crc;
    if (scr_meta_get_crc32(meta_send, &meta_send_crc) != SCR_SUCCESS) {
      scr_meta_set_crc32(meta_send, *crc32_send);
    } else {
      /* TODO: we could check that the crc on the sent file matches and take some action if not */
    }
  }

  /* free the MPI buffers */
  scr_align_free(&buf_recv);
  scr_align_free(&buf_send);

  return rc;
}

static int scr_swap_files_move(
  int have_outgoing, const char* file_send, scr_meta* meta_send, int rank_send, uLong* crc32_send,
  int have_incoming, const char* file_recv, scr_meta* meta_recv, int rank_recv, uLong* crc32_recv,
  MPI_Comm comm)
{
  int rc = SCR_SUCCESS;
  MPI_Request request[2];
  MPI_Status  status[2];

  /* allocate MPI send buffer */
  char *buf_send = NULL;
  if (have_outgoing) {
    buf_send = (char*) scr_align_malloc(scr_mpi_buf_size, scr_page_size);
    if (buf_send == NULL) {
      scr_abort(-1, "Allocating memory: malloc(%ld) errno=%d %s @ %s:%d",
              scr_mpi_buf_size, errno, strerror(errno), __FILE__, __LINE__
      );
      return SCR_FAILURE;
    }
  }

  /* allocate MPI recv buffer */
  char *buf_recv = NULL;
  if (have_incoming) {
    buf_recv = (char*) scr_align_malloc(scr_mpi_buf_size, scr_page_size);
    if (buf_recv == NULL) {
      scr_abort(-1, "Allocating memory: malloc(%ld) errno=%d %s @ %s:%d",
              scr_mpi_buf_size, errno, strerror(errno), __FILE__, __LINE__
      );
      return SCR_FAILURE;
    }
  }

  /* since we'll overwrite our send file in place with the recv file,
   * which may be larger, we need to keep track of how many bytes we've
   * sent and whether we've sent them all */
  unsigned long filesize_send = 0;

  /* open our file */
  int fd = -1;
  if (have_outgoing) {
    /* we'll overwrite our send file (or just read it if there is no incoming) */
    filesize_send = scr_file_size(file_send);
    fd = scr_open(file_send, O_RDWR);
    if (fd < 0) {
      /* TODO: skip writes and return error? */
      scr_abort(-1, "Opening file for send/recv: scr_open(%s, O_RDWR) errno=%d %s @ %s:%d",
              file_send, errno, strerror(errno), __FILE__, __LINE__
      );
    }
  } else if (have_incoming) {
    /* if we're in this branch, then we only have an incoming file,
     * so we'll write our recv file from scratch */
    mode_t mode_file = scr_getmode(1, 1, 0);
    fd = scr_open(file_recv, O_WRONLY | O_CREAT | O_TRUNC, mode_file);
    if (fd < 0) {
      /* TODO: skip writes and return error? */
      scr_abort(-1, "Opening file for recv: scr_open(%s, O_WRONLY | O_CREAT | O_TRUNC, ...) errno=%d %s @ %s:%d",
              file_recv, errno, strerror(errno), __FILE__, __LINE__
      );
    }
  }

  /* exchange file chunks */
  int sending = 0;
  if (have_outgoing) {
    sending = 1;
  }
  int receiving = 0;
  if (have_incoming) {
    receiving = 1;
  }
  int nread, nwrite;
  off_t read_pos = 0, write_pos = 0;
  while (sending || receiving) {
    if (receiving) {
      /* prepare a buffer to receive up to scr_mpi_buf_size bytes */
      MPI_Irecv(buf_recv, scr_mpi_buf_size, MPI_BYTE, rank_recv, 0, comm, &request[0]);
    }

    if (sending) {
      /* compute number of bytes to read */
      unsigned long count = filesize_send - read_pos;
      if (count > scr_mpi_buf_size) {
        count = scr_mpi_buf_size;
      }

      /* read a chunk of up to scr_mpi_buf_size bytes into buf_send */
      lseek(fd, read_pos, SEEK_SET); /* seek to read position */
      nread = scr_read(file_send, fd, buf_send, count);
      if (scr_crc_on_copy && nread > 0) {
        *crc32_send = crc32(*crc32_send, (const Bytef*) buf_send, (uInt) nread);
      }
      if (nread < 0) {
        nread = 0;
      }
      read_pos += (off_t) nread; /* update read pointer */

      /* send chunk (if nread is smaller than scr_mpi_buf_size,
       * then we've read the whole file) */
      MPI_Isend(buf_send, nread, MPI_BYTE, rank_send, 0, comm, &request[1]);
      MPI_Wait(&request[1], &status[1]);

      /* check whether we've read the whole file */
      if (filesize_send == read_pos && count < scr_mpi_buf_size) {
        sending = 0;
      }
    }

    if (receiving) {
      /* count the number of bytes received */
      MPI_Wait(&request[0], &status[0]);
      MPI_Get_count(&status[0], MPI_BYTE, &nwrite);
      if (scr_crc_on_copy && nwrite > 0) {
        *crc32_recv = crc32(*crc32_recv, (const Bytef*) buf_recv, (uInt) nwrite);
      }

      /* write those bytes to file (if nwrite is smaller than scr_mpi_buf_size,
       * then we've received the whole file) */
      lseek(fd, write_pos, SEEK_SET); /* seek to write position */
      scr_write(file_recv, fd, buf_recv, nwrite);
      write_pos += (off_t) nwrite; /* update write pointer */

      /* if nwrite is smaller than scr_mpi_buf_size,
       * then assume we've received the whole file */
      if (nwrite < scr_mpi_buf_size) {
        receiving = 0;
      }
    }
  }

  /* close file and cleanup */
  if (have_outgoing && have_incoming) {
    /* sent and received a file; close it, truncate it to corect size, rename it */
    scr_close(file_send, fd);
    truncate(file_send, write_pos);
    rename(file_send, file_recv);
  } else if (have_outgoing) {
    /* only sent a file; close it, delete it, and remove its completion marker */
    scr_close(file_send, fd);
    scr_file_unlink(file_send);
  } else if (have_incoming) {
    /* only received a file; just need to close it */
    scr_close(file_recv, fd);
  }

  if (scr_crc_on_copy && have_outgoing) {
    uLong meta_send_crc;
    if (scr_meta_get_crc32(meta_send, &meta_send_crc) != SCR_SUCCESS) {
      /* we transfer this meta data across below,
       * so may as well update these fields so we can use them */
      scr_meta_set_crc32(meta_send, *crc32_send);
      /* do not complete file send, we just deleted it above */
    } else {
      /* TODO: we could check that the crc on the sent file matches and take some action if not */
    }
  }

  /* free the MPI buffers */
  scr_align_free(&buf_recv);
  scr_align_free(&buf_send);

  return rc;
}

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
  MPI_Comm comm)
{
  int rc = SCR_SUCCESS;

  /* determine whether we have a file to send */
  int have_outgoing = 0;
  if (rank_send != MPI_PROC_NULL &&
      file_send != NULL &&
      strcmp(file_send, "") != 0)
  {
    have_outgoing = 1;
  }

  /* determine whether we are expecting to receive a file */
  int have_incoming = 0;
  if (rank_recv != MPI_PROC_NULL &&
      file_recv != NULL &&
      strcmp(file_recv, "") != 0)
  {
    have_incoming = 1;
  }

  /* exchange meta file info with partners */
  scr_hash_sendrecv(meta_send, rank_send, meta_recv, rank_recv, comm);

  /* initialize crc values */
  uLong crc32_send = crc32(0L, Z_NULL, 0);
  uLong crc32_recv = crc32(0L, Z_NULL, 0);

  /* exchange files */
  if (swap_type == COPY_FILES) {
    scr_swap_files_copy(
      have_outgoing, file_send, meta_send, rank_send, &crc32_send,
      have_incoming, file_recv, meta_recv, rank_recv, &crc32_recv,
      comm
    );
  } else if (swap_type == MOVE_FILES) {
    scr_swap_files_move(
      have_outgoing, file_send, meta_send, rank_send, &crc32_send,
      have_incoming, file_recv, meta_recv, rank_recv, &crc32_recv,
      comm
    );
  } else {
    scr_err("Unknown file transfer type: %d @ %s:%d",
            swap_type, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* mark received file as complete */
  if (have_incoming) {
    /* check that our written file is the correct size */
    unsigned long filesize_wrote = scr_file_size(file_recv);
    if (scr_meta_check_filesize(meta_recv, filesize_wrote) != SCR_SUCCESS) {
      scr_err("Received file does not match expected size %s @ %s:%d",
              file_recv, __FILE__, __LINE__
      );
      scr_meta_set_complete(meta_recv, 0);
      rc = SCR_FAILURE;
    }

    /* check that there was no corruption in receiving the file */
    if (scr_crc_on_copy) {
      /* if we computed crc during the copy, and crc is set in the received meta data
       * check that our computed value matches */
      uLong crc32_recv_meta;
      if (scr_meta_get_crc32(meta_recv, &crc32_recv_meta) == SCR_SUCCESS) {
        if (crc32_recv != crc32_recv_meta) {
          scr_err("CRC32 mismatch detected when receiving file %s @ %s:%d",
                  file_recv, __FILE__, __LINE__
          );
          scr_meta_set_complete(meta_recv, 0);
          rc = SCR_FAILURE;
        }
      }
    }
  }

  return rc;
}

/*
=========================================
Distribute and file rebuild functions
=========================================
*/

/* since on a restart we may end up with more or fewer ranks on a node than the
 * previous run, rely on the master to read in and distribute the filemap to
 * other ranks on the node */
int scr_scatter_filemaps(scr_filemap* my_map)
{
  /* TODO: if the control directory is on a device shared by lots of procs,
   * we should read and distribute this data in a more scalable way */

  /* allocate empty send hash */
  scr_hash* send_hash = scr_hash_new();

  /* if i'm the master on this node, read in all filemaps */
  if (scr_storedesc_cntl->rank == 0) {
    /* create an empty filemap */
    scr_filemap* all_map = scr_filemap_new();

    /* read in the master map */
    scr_hash* hash = scr_hash_new();
    scr_hash_read_path(scr_master_map_file, hash);

    /* for each filemap listed in the master map */
    scr_hash_elem* elem;
    for (elem = scr_hash_elem_first(scr_hash_get(hash, "Filemap"));
         elem != NULL;
         elem = scr_hash_elem_next(elem))
    {
      /* get the filename of this filemap */
      char* file = scr_hash_elem_key(elem);

      /* TODO MEMFS: mount storage for each filemap */

      /* read in the filemap */
      scr_filemap* tmp_map = scr_filemap_new();
      scr_path* path_file = scr_path_from_str(file);
      scr_filemap_read(path_file, tmp_map);
      scr_path_delete(&path_file);

      /* merge it with the all_map */
      scr_filemap_merge(all_map, tmp_map);

      /* delete filemap */
      scr_filemap_delete(&tmp_map);

      /* TODO: note that if we fail after unlinking this file but before
       * writing out the new file, we'll lose information */

      /* delete the file */
      scr_file_unlink(file);
    }

    /* free the hash object */
    scr_hash_delete(&hash);

    /* write out new local 0 filemap */
    if (scr_filemap_num_ranks(all_map) > 0) {
      scr_filemap_write(scr_map_file, all_map);
    }

    /* get global rank of each rank */
    int* ranks = (int*) SCR_MALLOC(scr_storedesc_cntl->ranks * sizeof(int));
    MPI_Gather(
      &scr_my_rank_world, 1, MPI_INT, ranks, 1, MPI_INT,
      0, scr_storedesc_cntl->comm
    );

    /* for each rank, send them their own file data if we have it */
    int i;
    for (i=0; i < scr_storedesc_cntl->ranks; i++) {
      int rank = ranks[i];
      if (scr_filemap_have_rank(all_map, rank)) {
        /* extract the filemap for this rank */
        scr_filemap* tmp_map = scr_filemap_extract_rank(all_map, rank);

        /* get a reference to the hash object that we'll send to this rank,
         * and merge this filemap into it */
        scr_hash* tmp_hash = scr_hash_getf(send_hash, "%d", i);
        if (tmp_hash == NULL) {
          /* if we don't find an existing entry in the send_hash,
           * create an empty hash and insert it */
          scr_hash* empty_hash = scr_hash_new();
          scr_hash_setf(send_hash, empty_hash, "%d", i);
          tmp_hash = empty_hash;
        }
        scr_hash_merge(tmp_hash, tmp_map);

        /* delete the filemap for this rank */
        scr_filemap_delete(&tmp_map);
      }
    }

    /* free our rank list */
    scr_free(&ranks);

    /* now just round robin the remainder across the set (load balancing) */
    int num;
    int* remaining_ranks = NULL;
    scr_filemap_list_ranks(all_map, &num, &remaining_ranks);

    int j = 0;
    while (j < num) {
      /* pick a rank in to send to */
      i = j % scr_storedesc_cntl->ranks;

      /* extract the filemap for this rank */
      scr_filemap* tmp_map = scr_filemap_extract_rank(all_map, remaining_ranks[j]);

      /* get a reference to the hash object that we'll send to this rank,
       * and merge this filemap into it */
      scr_hash* tmp_hash = scr_hash_getf(send_hash, "%d", i);
      if (tmp_hash == NULL) {
        /* if we don't find an existing entry in the send_hash,
         * create an empty hash and insert it */
        scr_hash* empty_hash = scr_hash_new();
        scr_hash_setf(send_hash, empty_hash, "%d", i);
        tmp_hash = empty_hash;
      }
      scr_hash_merge(tmp_hash, tmp_map);

      /* delete the filemap for this rank */
      scr_filemap_delete(&tmp_map);
      j++;
    }

    scr_free(&remaining_ranks);

    /* delete the filemap */
    scr_filemap_delete(&all_map);

    /* write out the new master filemap */
    hash = scr_hash_new();
    char file[SCR_MAX_FILENAME];
    for (i=0; i < scr_storedesc_cntl->ranks; i++) {
      sprintf(file, "%s/filemap_%d.scrinfo", scr_cntl_prefix, i);
      scr_hash_set_kv(hash, "Filemap", file);
    }
    scr_hash_write_path(scr_master_map_file, hash);
    scr_hash_delete(&hash);
  } else {
    /* send our global rank to the master */
    MPI_Gather(
      &scr_my_rank_world, 1, MPI_INT, NULL, 1, MPI_INT,
      0, scr_storedesc_cntl->comm
    );
  }

  /* receive our filemap from master */
  scr_hash* recv_hash = scr_hash_new();
  scr_hash_exchange(send_hash, recv_hash, scr_storedesc_cntl->comm);

  /* merge map sent from master into our map */
  scr_hash* map_from_master = scr_hash_getf(recv_hash, "%d", 0);
  if (map_from_master != NULL) {
    scr_hash_merge(my_map, map_from_master);
  }

  /* write out our local filemap */
  if (scr_filemap_num_ranks(my_map) > 0) {
    scr_filemap_write(scr_map_file, my_map);
  }

  /* free off our send and receive hashes */
  scr_hash_delete(&recv_hash);
  scr_hash_delete(&send_hash);

  return SCR_SUCCESS;
}

/* broadcast dataset hash from smallest rank we can find that has a copy */
static int scr_distribute_datasets(scr_filemap* map, int id)
{
  int i;

  /* create a new hash to record dataset descriptor */
  scr_hash* send_hash = scr_hash_new();

  /* for this dataset, get list of ranks we have data for */
  int  nranks = 0;
  int* ranks = NULL;
  scr_filemap_list_ranks_by_dataset(map, id, &nranks, &ranks);

  /* for each rank we have files for,
   * check whether we also have its dataset descriptor */
  int invalid_rank_found = 0;
  int have_dset = 0;
  for (i=0; i < nranks; i++) {
    /* get the rank id */
    int rank = ranks[i];

    /* check that the rank is within range */
    if (rank < 0 || rank >= scr_ranks_world) {
      scr_err("Invalid rank id %d in world of %d @ %s:%d",
        rank, scr_ranks_world, __FILE__, __LINE__
      );
      invalid_rank_found = 1;
    }

    /* lookup the dataset descriptor hash for this rank */
    scr_hash* desc = scr_hash_new();
    scr_filemap_get_dataset(map, id, rank, desc);

    /* if this descriptor has entries, add it to our send hash,
     * delete the hash otherwise */
    if (scr_hash_size(desc) > 0) {
      have_dset = 1;
      scr_hash_merge(send_hash, desc);
      scr_hash_delete(&desc);
      break;
    } else {
      scr_hash_delete(&desc);
    }
  }

  /* free off our list of ranks */
  scr_free(&ranks);

  /* check that we didn't find an invalid rank on any process */
  if (! scr_alltrue(invalid_rank_found == 0)) {
    scr_hash_delete(&send_hash);
    return SCR_FAILURE;
  }

  /* identify the smallest rank that has the dataset */
  int source_rank = scr_ranks_world;
  if (have_dset) {
    source_rank = scr_my_rank_world;
  }
  int min_rank;
  MPI_Allreduce(&source_rank, &min_rank, 1, MPI_INT, MPI_MIN, scr_comm_world);

  /* if there is no rank, return with failure */
  if (min_rank >= scr_ranks_world) {
    scr_hash_delete(&send_hash);
    return SCR_FAILURE;
  }

  /* otherwise, bcast the dataset from the minimum rank */
  if (scr_my_rank_world != min_rank) {
    scr_hash_unset_all(send_hash);
  }
  scr_hash_bcast(send_hash, min_rank, scr_comm_world);

  /* record the descriptor in our filemap */
  scr_filemap_set_dataset(map, id, scr_my_rank_world, send_hash);
  scr_filemap_write(scr_map_file, map);

  /* TODO: at this point, we could delete descriptors for other
   * ranks for this checkpoint */

  /* free off our send hash */
  scr_hash_delete(&send_hash);

  return SCR_SUCCESS;
}

/* this transfers redundancy descriptors for the given dataset id */
static int scr_distribute_reddescs(scr_filemap* map, int id, scr_reddesc* red)
{
  int i;

  /* create a new hash to record redundancy descriptors that we have */
  scr_hash* send_hash = scr_hash_new();

  /* for this dataset, get list of ranks we have data for */
  int  nranks = 0;
  int* ranks = NULL;
  scr_filemap_list_ranks_by_dataset(map, id, &nranks, &ranks);

  /* for each rank we have files for, check whether we also have
   * its redundancy descriptor */
  int invalid_rank_found = 0;
  for (i=0; i < nranks; i++) {
    /* get the rank id */
    int rank = ranks[i];

    /* check that the rank is within range */
    if (rank < 0 || rank >= scr_ranks_world) {
      scr_err("Invalid rank id %d in world of %d @ %s:%d",
        rank, scr_ranks_world, __FILE__, __LINE__
      );
      invalid_rank_found = 1;
    }

    /* lookup the redundancy descriptor hash for this rank */
    scr_hash* desc = scr_hash_new();
    scr_filemap_get_desc(map, id, rank, desc);

    /* if this descriptor has entries, add it to our send hash,
     * delete the hash otherwise */
    if (scr_hash_size(desc) > 0) {
      scr_hash_setf(send_hash, desc, "%d", rank);
    } else {
      scr_hash_delete(&desc);
    }
  }

  /* free off our list of ranks */
  scr_free(&ranks);

  /* check that we didn't find an invalid rank on any process */
  if (! scr_alltrue(invalid_rank_found == 0)) {
    scr_hash_delete(&send_hash);
    return SCR_FAILURE;
  }

  /* create an empty hash to receive any incoming descriptors */
  /* exchange descriptors with other ranks */
  scr_hash* recv_hash = scr_hash_new();
  scr_hash_exchange(send_hash, recv_hash, scr_comm_world);

  /* check that everyone can get their descriptor */
  int num_desc = scr_hash_size(recv_hash);
  if (! scr_alltrue(num_desc > 0)) {
    scr_hash_delete(&recv_hash);
    scr_hash_delete(&send_hash);
    scr_dbg(2, "Cannot find process that has my redundancy descriptor @ %s:%d",
      __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* just go with the first redundancy descriptor in our list,
   * they should all be the same */
  scr_hash_elem* desc_elem = scr_hash_elem_first(recv_hash);
  scr_hash* desc_hash = scr_hash_elem_hash(desc_elem);

  /* record the descriptor in our filemap */
  scr_filemap_set_desc(map, id, scr_my_rank_world, desc_hash);
  scr_filemap_write(scr_map_file, map);

  /* TODO: at this point, we could delete descriptors for other
   * ranks for this checkpoint */

  /* create our redundancy descriptor struct from the map */
  scr_reddesc_create_from_filemap(map, id, scr_my_rank_world, red);

  /* free off our send and receive hashes */
  scr_hash_delete(&recv_hash);
  scr_hash_delete(&send_hash);

  return SCR_SUCCESS;
}

/* this moves all files of the specified dataset in the cache to
 * make them accessible to new rank mapping */
static int scr_distribute_files(scr_filemap* map, const scr_reddesc* red, int id)
{
  int i, round;
  int rc = SCR_SUCCESS;

  /* TODO: mark dataset as being distributed in filemap,
   * because if we fail in the middle of a distribute,
   * we can't trust the contents of the files anymore,
   * at which point it should be deleted */

  /* clean out any incomplete files before we start */
  scr_cache_clean(map);

  /* for this dataset, get list of ranks we have data for */
  int  nranks = 0;
  int* ranks = NULL;
  scr_filemap_list_ranks_by_dataset(map, id, &nranks, &ranks);

  /* walk backwards through the list of ranks, and set our start index
   * to the rank which is the first rank that is equal to or higher
   * than our own rank -- when we assign round ids below, this offsetting
   * helps distribute the load */
  int start_index = 0;
  int invalid_rank_found = 0;
  for (i = nranks-1; i >= 0; i--) {
    int rank = ranks[i];

    /* pick the first rank whose rank id is equal to or higher than our own */
    if (rank >= scr_my_rank_world) {
      start_index = i;
    }

    /* while we're at it, check that the rank is within range */
    if (rank < 0 || rank >= scr_ranks_world) {
      scr_err("Invalid rank id %d in world of %d @ %s:%d",
        rank, scr_ranks_world, __FILE__, __LINE__
      );
      invalid_rank_found = 1;
    }
  }

  /* check that we didn't find an invalid rank on any process */
  if (! scr_alltrue(invalid_rank_found == 0)) {
    scr_free(&ranks);
    return SCR_FAILURE;
  }

  /* allocate array to record the rank we can send to in each round */
  int* have_rank_by_round = (int*) SCR_MALLOC(sizeof(int) * nranks);
  int* send_flag_by_round = (int*) SCR_MALLOC(sizeof(int) * nranks);

  /* check that we have all of the files for each rank,
   * and determine the round we can send them */
  scr_hash* send_hash = scr_hash_new();
  scr_hash* recv_hash = scr_hash_new();
  for (round = 0; round < nranks; round++) {
    /* get the rank id */
    int index = (start_index + round) % nranks;
    int rank = ranks[index];

    /* record the rank indexed by the round number */
    have_rank_by_round[round] = rank;

    /* assume we won't be sending to this rank in this round */
    send_flag_by_round[round] = 0;

    /* if we have files for this rank, specify the round we can
     * send those files in */
    if (scr_bool_have_files(map, id, rank)) {
      scr_hash_setf(send_hash, NULL, "%d %d", rank, round);
    }
  }
  scr_hash_exchange(send_hash, recv_hash, scr_comm_world);

  /* search for the minimum round we can get our files */
  int retrieve_rank  = -1;
  int retrieve_round = -1;
  scr_hash_elem* elem = NULL;
  for (elem = scr_hash_elem_first(recv_hash);
       elem != NULL;
       elem = scr_hash_elem_next(elem))
  {
    /* get the rank id */
    int rank = scr_hash_elem_key_int(elem);

    /* get the round id */
    scr_hash* round_hash = scr_hash_elem_hash(elem);
    scr_hash_elem* round_elem = scr_hash_elem_first(round_hash);
    char* round_str = scr_hash_elem_key(round_elem);
    int round = atoi(round_str);

    /* record this round and rank number if it's less than the current round */
    if (round < retrieve_round || retrieve_round == -1) {
      retrieve_round = round;
      retrieve_rank  = rank;
    }
  }

  /* done with the round hashes, free them off */
  scr_hash_delete(&recv_hash);
  scr_hash_delete(&send_hash);

  /* free off our list of ranks */
  scr_free(&ranks);

  /* for some redundancy schemes, we know at this point whether we
   * can recover all files */
  int can_get_files = (retrieve_rank != -1);
  if (red->copy_type != SCR_COPY_XOR && !scr_alltrue(can_get_files)) {
    /* print a debug message indicating which rank is missing files */
    if (! can_get_files) {
      scr_dbg(2, "Cannot find process that has my checkpoint files @ %s:%d",
        __FILE__, __LINE__
      );
    }
    return SCR_FAILURE;
  }

  /* get the maximum retrieve round */
  int max_rounds = 0;
  MPI_Allreduce(
    &retrieve_round, &max_rounds, 1, MPI_INT, MPI_MAX, scr_comm_world
  );

  /* tell destination which round we'll take our files in */
  send_hash = scr_hash_new();
  recv_hash = scr_hash_new();
  if (retrieve_rank != -1) {
    scr_hash_setf(send_hash, NULL, "%d %d", retrieve_rank, retrieve_round);
  }
  scr_hash_exchange(send_hash, recv_hash, scr_comm_world);

  /* determine which ranks want to fetch their files from us */
  for(elem = scr_hash_elem_first(recv_hash);
      elem != NULL;
      elem = scr_hash_elem_next(elem))
  {
    /* get the round id */
    scr_hash* round_hash = scr_hash_elem_hash(elem);
    scr_hash_elem* round_elem = scr_hash_elem_first(round_hash);
    char* round_str = scr_hash_elem_key(round_elem);
    int round = atoi(round_str);

    /* record whether this rank wants its files from us */
    if (round >= 0 && round < nranks) {
      send_flag_by_round[round] = 1;
    }
  }

  /* done with the round hashes, free them off */
  scr_hash_delete(&recv_hash);
  scr_hash_delete(&send_hash);

  int tmp_rc = 0;

  /* run through rounds and exchange files */
  for (round = 0; round <= max_rounds; round++) {
    /* assume we don't need to send or receive any files this round */
    int send_rank = MPI_PROC_NULL;
    int recv_rank = MPI_PROC_NULL;
    int send_num  = 0;
    int recv_num  = 0;

    /* check whether I can potentially send to anyone in this round */
    if (round < nranks) {
      /* have someone's files, check whether they are asking
       * for them this round */
      if (send_flag_by_round[round]) {
        /* need to send files this round, remember to whom and how many */
        int dst_rank = have_rank_by_round[round];
        send_rank = dst_rank;
        send_num  = scr_filemap_num_files(map, id, dst_rank);
      }
    }

    /* if I'm supposed to get my files this round, set the recv_rank */
    if (retrieve_round == round) {
      recv_rank = retrieve_rank;
    }

    /* TODO: another special case is to just move files if the
     * processes are on the same node */

    /* if i'm sending to myself, just move (rename) each file */
    if (send_rank == scr_my_rank_world) {
      /* get our file list */
      int numfiles = 0;
      char** files = NULL;
      scr_filemap_list_files(map, id, send_rank, &numfiles, &files);

      /* TODO: sort files in reverse order by size */

      /* iterate over and rename each file */
      for (i=0; i < numfiles; i++) {
        /* get the current file name */
        char* file = files[i];

        /* lookup meta data for this file */
        scr_meta* meta = scr_meta_new();
        scr_filemap_get_meta(map, id, send_rank, file, meta);

        /* get the path for this file based on its type
         * and dataset id */
        char* dir = NULL;
        if (scr_meta_check_filetype(meta, SCR_META_FILE_USER) == SCR_SUCCESS) {
          dir = scr_cache_dir_get(red, id);
        } else {
          dir = scr_cache_dir_hidden_get(red, id);
        }

        /* build the new file name */
        scr_path* path_newfile = scr_path_from_str(file);
        scr_path_basename(path_newfile);
        scr_path_prepend_str(path_newfile, dir);
        char* newfile = scr_path_strdup(path_newfile);

        /* if the new file name is different from the old name, rename it */
        if (strcmp(file, newfile) != 0) {
          /* record the new filename to our map and write it to disk */
          scr_filemap_add_file(map, id, send_rank, newfile);
          scr_filemap_set_meta(map, id, send_rank, newfile, meta);
          scr_filemap_write(scr_map_file, map);

          /* rename the file */
          scr_dbg(2, "Round %d: rename(%s, %s)", round, file, newfile);
          tmp_rc = rename(file, newfile);
          if (tmp_rc != 0) {
            /* TODO: to cross mount points, if tmp_rc == EXDEV,
             * open new file, copy, and delete orig */
            scr_err("Moving checkpoint file: rename(%s, %s) %s errno=%d @ %s:%d",
              file, newfile, strerror(errno), errno, __FILE__, __LINE__
            );
            rc = SCR_FAILURE;
          }

          /* remove the old name from the filemap and write it to disk */
          scr_filemap_remove_file(map, id, send_rank, file);
          scr_filemap_write(scr_map_file, map);
        }

        /* free the path and string */
        scr_free(&newfile);
        scr_path_delete(&path_newfile);

        /* free directory string */
        scr_free(&dir);

        /* free meta data */
        scr_meta_delete(&meta);
      }

      /* free the list of filename pointers */
      scr_free(&files);
    } else {
      /* if we have files for this round, but the correspdonding
       * rank doesn't need them, delete the files */
      if (round < nranks && send_rank == MPI_PROC_NULL) {
        int dst_rank = have_rank_by_round[round];
        scr_unlink_rank(map, id, dst_rank);
      }

      /* sending to and/or recieving from another node */
      if (send_rank != MPI_PROC_NULL || recv_rank != MPI_PROC_NULL) {
        /* have someone to send to or receive from */
        int have_outgoing = 0;
        int have_incoming = 0;
        if (send_rank != MPI_PROC_NULL) {
          have_outgoing = 1;
        }
        if (recv_rank != MPI_PROC_NULL) {
          have_incoming = 1;
        }

        /* first, determine how many files I will be receiving and
         * tell how many I will be sending */
        MPI_Request request[2];
        MPI_Status  status[2];
        int num_req = 0;
        if (have_incoming) {
          MPI_Irecv(
            &recv_num, 1, MPI_INT, recv_rank, 0,
            scr_comm_world, &request[num_req]
          );
          num_req++;
        }
        if (have_outgoing) {
          MPI_Isend(
            &send_num, 1, MPI_INT, send_rank, 0,
            scr_comm_world, &request[num_req]
          );
          num_req++;
        }
        if (num_req > 0) {
          MPI_Waitall(num_req, request, status);
        }

        /* record how many files I will receive (need to distinguish
         * between 0 files and not knowing) */
        if (have_incoming) {
          scr_filemap_set_expected_files(map, id, scr_my_rank_world, recv_num);
        }

        /* turn off send or receive flags if the file count is 0,
         * nothing else to do */
        if (send_num == 0) {
          have_outgoing = 0;
          send_rank = MPI_PROC_NULL;
        }
        if (recv_num == 0) {
          have_incoming = 0;
          recv_rank = MPI_PROC_NULL;
        }

        /* TODO: since we overwrite files in place in order to avoid
         * running out of storage space, we should sort files in order
         * of descending size for the next step */

        /* get our file list for the destination */
        int numfiles = 0;
        char** files = NULL;
        if (have_outgoing) {
          scr_filemap_list_files(map, id, send_rank, &numfiles, &files);
        }

        /* while we have a file to send or receive ... */
        while (have_incoming || have_outgoing) {
          /* get the filename */
          char* file = NULL;
          scr_meta* send_meta = NULL;
          if (have_outgoing) {
            file = files[numfiles - send_num];
            send_meta = scr_meta_new();
            scr_filemap_get_meta(map, id, send_rank, file, send_meta);
          }

          /* exchange meta data so we can determine type of incoming file */
          scr_meta* recv_meta = scr_meta_new();
          scr_hash_sendrecv(send_meta, send_rank, recv_meta, recv_rank, scr_comm_world);

          /* get the path for this file based on its type and dataset id */
          char* dir = NULL;
          if (have_incoming) {
            if (scr_meta_check_filetype(recv_meta, SCR_META_FILE_USER) == SCR_SUCCESS) {
              dir = scr_cache_dir_get(red, id);
            } else {
              dir = scr_cache_dir_hidden_get(red, id);
            }
          }

          /* exhange file names with partners,
           * building full path of incoming file */
          char file_partner[SCR_MAX_FILENAME];
          scr_swap_file_names(
            file, send_rank, file_partner, sizeof(file_partner), recv_rank,
            dir, scr_comm_world
          );

          /* free directory string */
          scr_free(&dir);

          /* free incoming meta data (we'll get this again later) */
          scr_meta_delete(&recv_meta);

          /* if we'll receive a file, record the name of our file
           * in the filemap and write it to disk */
          recv_meta = NULL;
          if (recv_rank != MPI_PROC_NULL) {
            recv_meta = scr_meta_new();
            scr_filemap_add_file(map, id, scr_my_rank_world, file_partner);
            scr_filemap_write(scr_map_file, map);
          }

          /* either sending or receiving a file this round, since we move files,
           * it will be deleted or overwritten */
          if (scr_swap_files(MOVE_FILES, file, send_meta, send_rank,
              file_partner, recv_meta, recv_rank, scr_comm_world) != SCR_SUCCESS)
          {
            scr_err("Swapping files: %s to %d, %s from %d @ %s:%d",
                    file, send_rank, file_partner, recv_rank, __FILE__, __LINE__
            );
            rc = SCR_FAILURE;
          }

          /* if we received a file, record its meta data and decrement
           * our receive count */
          if (have_incoming) {
            /* record meta data for the file we received */
            scr_filemap_set_meta(map, id, scr_my_rank_world, file_partner, recv_meta);
            scr_meta_delete(&recv_meta);

            /* decrement receive count */
            recv_num--;
            if (recv_num == 0) {
              have_incoming = 0;
              recv_rank = MPI_PROC_NULL;
            }
          }

          /* if we sent a file, remove it from the filemap and decrement
           * our send count */
          if (have_outgoing) {
            /* remove file from the filemap */
            scr_filemap_remove_file(map, id, send_rank, file);
            scr_meta_delete(&send_meta);

            /* decrement our send count */
            send_num--;
            if (send_num == 0) {
              have_outgoing = 0;
              send_rank = MPI_PROC_NULL;
            }
          }

          /* update filemap on disk */
          scr_filemap_write(scr_map_file, map);
        }

        /* free our file list */
        scr_free(&files);
      }
    }
  }

  /* if we have more rounds than max rounds, delete the remainder of our files */
  for (round = max_rounds+1; round < nranks; round++) {
    /* have someone's files for this round, so delete them */
    int dst_rank = have_rank_by_round[round];
    scr_unlink_rank(map, id, dst_rank);
  }

  scr_free(&send_flag_by_round);
  scr_free(&have_rank_by_round);

  /* write out new filemap and free the memory resources */
  scr_filemap_write(scr_map_file, map);

  /* clean out any incomplete files */
  scr_cache_clean(map);

  /* TODO: if the exchange or redundancy rebuild failed,
   * we should also delete any *good* files we received */

  /* return whether distribute succeeded, it does not ensure we have
   * all of our files, only that the transfer completed without failure */
  return rc;
}

/* distribute and rebuild files in cache */
int scr_cache_rebuild(scr_filemap* map)
{
  int rc = SCR_FAILURE;

  double time_start, time_end, time_diff;

  /* start timer */
  time_t time_t_start;
  if (scr_my_rank_world == 0) {
    time_t_start = scr_log_seconds();
    time_start = MPI_Wtime();
  }

  /* we set this variable to 1 if we actually try to distribute
   * files for a restart */
  int distribute_attempted = 0;

  /* clean any incomplete files from our cache */
  scr_cache_clean(map);

  /* get ordered list of datasets we have in our cache */
  int ndsets;
  int* dsets;
  scr_filemap_list_datasets(map, &ndsets, &dsets);

  /* TODO: put dataset selection logic into a function */

  /* TODO: also attempt to recover datasets which we were in the
   * middle of flushing */
  int current_id;
  int dset_index = 0;
  int output_failed_rebuild = 0;
  do {
    /* get the smallest index across all processes (returned in current_id),
     * this also updates our dset_index value if appropriate */
    scr_next_dataset(ndsets, dsets, &dset_index, &current_id);

    /* if we found a dataset, try to distribute and rebuild it */
    if (current_id != -1) {
      /* remember that we made an attempt to distribute at least one dataset */
      distribute_attempted = 1;
      
      /* log the attempt */
      if (scr_my_rank_world == 0) {
        scr_dbg(1, "Attempting to distribute and rebuild dataset %d", current_id);
        if (scr_log_enable) {
          time_t now = scr_log_seconds();
          scr_log_event("REBUILD STARTED", NULL, &current_id, &now, NULL);
        }
      }

      /* assume we'll fail to rebuild */
      int rebuild_succeeded = 0;

      /* distribute dataset descriptor for this dataset */
      if (scr_distribute_datasets(map, current_id) == SCR_SUCCESS) {
        /* get dataset for this id */
        scr_dataset* dataset = scr_dataset_new();
        scr_filemap_get_dataset(map, current_id, scr_my_rank_world, dataset);

        /* determine whether this is an output dataset */
        int is_output = scr_dataset_is_output(dataset);

        /* distribute redundancy descriptor for this dataset */
        scr_reddesc reddesc;
        if (scr_distribute_reddescs(map, current_id, &reddesc) == SCR_SUCCESS) {
          /* create a directory for this dataset */
          scr_cache_dir_create(&reddesc, current_id);

          /* distribute the files for this dataset */
          scr_distribute_files(map, &reddesc, current_id);

          /* rebuild files for this dataset */
          int tmp_rc = scr_reddesc_recover(map, &reddesc, current_id);
          if (tmp_rc == SCR_SUCCESS) {
            /* rebuild succeeded */
            rebuild_succeeded = 1;

            /* if we have a checkpoint, update dataset and checkpoint counters,
             * however skip this if we failed to rebuild an output set, in this
             * case we'll restart from the checkpoint before the lost output set */
            int is_ckpt = scr_dataset_is_ckpt(dataset);
            if (is_ckpt && !output_failed_rebuild) {
              /* if we rebuild any checkpoint, return success */
              rc = SCR_SUCCESS;

              /* update scr_dataset_id */
              if (current_id > scr_dataset_id) {
                scr_dataset_id = current_id;
              }

              /* get checkpoint id for dataset */
              int ckpt_id;
              scr_dataset_get_ckpt(dataset, &ckpt_id);

              /* update scr_checkpoint_id and scr_ckpt_dset_id if needed */
              if (ckpt_id > scr_checkpoint_id) {
                /* got a more recent checkpoint, update our checkpoint info */
                scr_checkpoint_id = ckpt_id;
                scr_ckpt_dset_id = current_id;
              }
            }

            /* update our flush file to indicate this dataset is in cache */
            scr_flush_file_location_set(current_id, SCR_FLUSH_KEY_LOCATION_CACHE);

            /* TODO: if storing flush file in control directory on each node,
             * if we find any process that has marked the dataset as flushed,
             * marked it as flushed in every flush file */

            /* TODO: would like to restore flushing status to datasets that
             * were in the middle of a flush, but we need to better manage
             * the transfer file to do this, so for now just forget about
             * flushing this dataset */
            scr_flush_file_location_unset(current_id, SCR_FLUSH_KEY_LOCATION_FLUSHING);
          }

          /* free redundancy descriptor */
          scr_reddesc_free(&reddesc);
        }

        /* remember if we fail to rebuild an output set */
        if (!rebuild_succeeded && is_output) {
          output_failed_rebuild = 1;
        }

        /* free dataset */
        scr_dataset_delete(&dataset);
      } else {
        /* if we failed to distribute dataset info, then we can't know
         * whether this was output or not, so we have to assume it was */
        output_failed_rebuild = 1;
      }

      /* if the distribute or rebuild failed, delete the dataset */
      if (! rebuild_succeeded) {
        /* log that we failed */
        if (scr_my_rank_world == 0) {
          scr_dbg(1, "Failed to rebuild dataset %d", current_id);
          if (scr_log_enable) {
            time_t now = scr_log_seconds();
            scr_log_event("REBUILD FAILED", NULL, &current_id, &now, NULL);
          }
        }

        /* TODO: there is a bug here, since scr_cache_delete needs to read
         * the redundancy descriptor from the filemap in order to delete the
         * cache directory, but we may have failed to distribute the reddescs
         * above so not every task has one */

        /* rebuild failed, delete this dataset from cache */
        scr_cache_delete(map, current_id);
      } else {
        /* rebuid worked, log success */
        if (scr_my_rank_world == 0) {
          scr_dbg(1, "Rebuilt dataset %d", current_id);
          if (scr_log_enable) {
            time_t now = scr_log_seconds();
            scr_log_event("REBUILD SUCCEEDED", NULL, &current_id, &now, NULL);
          }
        }
      }
    }
  } while (current_id != -1);

  /* free our list of dataset ids */
  scr_free(&dsets);

  /* get an updated list of datasets since we may have rebuilt/deleted some */
  scr_filemap_list_datasets(map, &ndsets, &dsets);

  /* delete all datasets following the most recent checkpoint */
  dset_index = 0;
  do {
    /* get the smallest index across all processes (returned in current_id),
     * this also updates our dset_index value if appropriate */
    scr_next_dataset(ndsets, dsets, &dset_index, &current_id);

    /* if we found a dataset, try to distribute and rebuild it */
    if (current_id != -1 && current_id > scr_ckpt_dset_id) {
      /* rebuild failed, delete this dataset from cache */
      scr_cache_delete(map, current_id);
    }
  } while (current_id != -1);

  /* free our list of dataset ids */
  scr_free(&dsets);

  /* stop timer and report performance */
  if (scr_my_rank_world == 0) {
    time_end = MPI_Wtime();
    time_diff = time_end - time_start;

    if (distribute_attempted) {
      if (rc == SCR_SUCCESS) {
        scr_dbg(1, "Scalable restart succeeded for checkpoint %d, took %f secs",
          scr_checkpoint_id, time_diff
        );
        if (scr_log_enable) {
          scr_log_event("RESTART SUCCEEDED", NULL, &scr_checkpoint_id, &time_t_start, &time_diff);
        }
      } else {
        /* scr_checkpoint_id is not defined */
        scr_dbg(1, "Scalable restart failed, took %f secs", time_diff);
        if (scr_log_enable) {
          scr_log_event("RESTART FAILED", NULL, NULL, &time_t_start, &time_diff);
        }
      }
    }
  }

  return rc;
}

/* remove any dataset ids from flush file which are not in cache,
 * and add any datasets in cache that are not in the flush file */
int scr_flush_file_rebuild(const scr_filemap* map)
{
  if (scr_my_rank_world == 0) {
    /* read the flush file */
    scr_hash* hash = scr_hash_new();
    scr_hash_read_path(scr_flush_file, hash);

    /* get ordered list of dataset ids in flush file */
    int flush_ndsets;
    int* flush_dsets;
    scr_hash* flush_dsets_hash = scr_hash_get(hash, SCR_FLUSH_KEY_DATASET);
    scr_hash_list_int(flush_dsets_hash, &flush_ndsets, &flush_dsets);

    /* get ordered list of dataset ids in cache */
    int cache_ndsets;
    int* cache_dsets;
    scr_filemap_list_datasets(map, &cache_ndsets, &cache_dsets);

    int flush_index = 0;
    int cache_index = 0;
    while (flush_index < flush_ndsets && cache_index < cache_ndsets) {
      /* get next smallest index from flush file and cache */
      int flush_dset = flush_dsets[flush_index];
      int cache_dset = cache_dsets[cache_index];

      if (flush_dset < cache_dset) {
        /* dataset exists in flush file but not in cache,
         * delete it from the flush file */
        scr_hash_unset_kv_int(hash, SCR_FLUSH_KEY_DATASET, flush_dset);
        flush_index++;
      } else if (cache_dset < flush_dset) {
        /* dataset exists in cache but not flush file,
         * add it to the flush file */
        scr_hash* dset_hash = scr_hash_set_kv_int(hash, SCR_FLUSH_KEY_DATASET, cache_dset);
        scr_hash_set_kv(dset_hash, SCR_FLUSH_KEY_LOCATION, SCR_FLUSH_KEY_LOCATION_CACHE);
        cache_index++;
      } else {
        /* dataset exists in cache and the flush file,
         * ensure that it is listed as being in the cache */
        scr_hash* dset_hash = scr_hash_set_kv_int(hash, SCR_FLUSH_KEY_DATASET, cache_dset);
        scr_hash_unset_kv(dset_hash, SCR_FLUSH_KEY_LOCATION, SCR_FLUSH_KEY_LOCATION_CACHE);
        scr_hash_set_kv(dset_hash, SCR_FLUSH_KEY_LOCATION, SCR_FLUSH_KEY_LOCATION_CACHE);
        flush_index++;
        cache_index++;
      }
    }
    while (flush_index < flush_ndsets) {
      /* dataset exists in flush file but not in cache,
       * delete it from the flush file */
      int flush_dset = flush_dsets[flush_index];
      scr_hash_unset_kv_int(hash, SCR_FLUSH_KEY_DATASET, flush_dset);
      flush_index++;
    }
    while (cache_index < cache_ndsets) {
      /* dataset exists in cache but not flush file,
       * add it to the flush file */
      int cache_dset = cache_dsets[cache_index];
      scr_hash* dset_hash = scr_hash_set_kv_int(hash, SCR_FLUSH_KEY_DATASET, cache_dset);
      scr_hash_set_kv(dset_hash, SCR_FLUSH_KEY_LOCATION, SCR_FLUSH_KEY_LOCATION_CACHE);
      cache_index++;
    }

    /* free our list of cache dataset ids */
    scr_free(&cache_dsets);

    /* free our list of flush file dataset ids */
    scr_free(&flush_dsets);

    /* write the hash back to the flush file */
    scr_hash_write_path(scr_flush_file, hash);

    /* delete the hash */
    scr_hash_delete(&hash);
  }
  return SCR_SUCCESS;
}
