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

#include "scr_globals.h"

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
    scr_err("Unknown file transfer type: %d @ %s:%d", swap_type, __FILE__, __LINE__);
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

/* copy files to a partner node */
static int scr_reddesc_apply_partner(scr_filemap* map, const scr_reddesc* c, int id)
{
  int rc = SCR_SUCCESS;

  /* get pointer to partner state structure */
  scr_reddesc_partner* state = (scr_reddesc_partner*) c->copy_state;

  /* get a list of our files */
  int numfiles = 0;
  char** files = NULL;
  scr_filemap_list_files(map, id, scr_my_rank_world, &numfiles, &files);

  /* first, determine how many files we'll be sending and receiving with our partners */
  MPI_Status status;
  int send_num = numfiles;
  int recv_num = 0;
  MPI_Sendrecv(
    &send_num, 1, MPI_INT, state->rhs_rank, 0,
    &recv_num, 1, MPI_INT, state->lhs_rank, 0,
    c->comm, &status
  );

  /* record how many files our partner will send */
  scr_filemap_set_expected_files(map, id, state->lhs_rank_world, recv_num);

  /* remember which node our partner is on (needed for scavenge) */
  scr_hash* flushdesc = scr_hash_new();
  scr_filemap_get_flushdesc(map, id, state->lhs_rank_world, flushdesc);
  scr_hash_util_set_int(flushdesc, SCR_SCAVENGE_KEY_PRESERVE,  scr_preserve_directories);
  scr_hash_util_set_int(flushdesc, SCR_SCAVENGE_KEY_CONTAINER, scr_use_containers);
  scr_hash_util_set_str(flushdesc, SCR_SCAVENGE_KEY_PARTNER,   state->lhs_hostname);
  scr_filemap_set_flushdesc(map, id, state->lhs_rank_world, flushdesc);
  scr_hash_delete(&flushdesc);

  /* record partner's redundancy descriptor hash */
  scr_hash* lhs_desc_hash = scr_hash_new();
  scr_hash* my_desc_hash  = scr_hash_new();
  scr_reddesc_store_to_hash(c, my_desc_hash);
  scr_hash_sendrecv(my_desc_hash, state->rhs_rank, lhs_desc_hash, state->lhs_rank, c->comm);
  scr_filemap_set_desc(map, id, state->lhs_rank_world, lhs_desc_hash);
  scr_hash_delete(&my_desc_hash);
  scr_hash_delete(&lhs_desc_hash);

  /* store this info in our filemap before we receive any files */
  scr_filemap_write(scr_map_file, map);

  /* define directory to receive partner file in */
  char dir[SCR_MAX_FILENAME];
  scr_cache_dir_get(c, id, dir);

  /* for each potential file, step through a call to swap */
  while (send_num > 0 || recv_num > 0) {
    /* assume we won't send or receive in this step */
    int send_rank = MPI_PROC_NULL;
    int recv_rank = MPI_PROC_NULL;

    /* if we have a file left to send, get the filename and destination rank */
    char* file = NULL;
    if (send_num > 0) {
      int i = numfiles - send_num;
      file = files[i];
      send_rank = state->rhs_rank;
      send_num--;
    }

    /* if we have a file left to receive, get the rank */
    if (recv_num > 0) {
      recv_rank = state->lhs_rank;
      recv_num--;
    }

    /* exhange file names with partners */
    char file_partner[SCR_MAX_FILENAME];
    scr_swap_file_names(file, send_rank, file_partner, sizeof(file_partner), recv_rank, dir, c->comm);

    /* if we'll receive a file, record the name of our partner's file in the filemap */
    if (recv_rank != MPI_PROC_NULL) {
      scr_filemap_add_file(map, id, state->lhs_rank_world, file_partner);
      scr_filemap_write(scr_map_file, map);
    }

    /* get meta data of file we're sending */
    scr_meta* send_meta = scr_meta_new();
    scr_filemap_get_meta(map, id, scr_my_rank_world, file, send_meta);

    /* exhange files with partners */
    scr_meta* recv_meta = scr_meta_new();
    if (scr_swap_files(COPY_FILES, file, send_meta, send_rank, file_partner, recv_meta, recv_rank, c->comm) != SCR_SUCCESS) {
      rc = SCR_FAILURE;
    }
    scr_filemap_set_meta(map, id, state->lhs_rank_world, file_partner, recv_meta);

    /* free meta data for these files */
    scr_meta_delete(&recv_meta);
    scr_meta_delete(&send_meta);
  }

  /* write out the updated filemap */
  scr_filemap_write(scr_map_file, map);

  /* free our list of files */
  scr_free(&files);

  return rc;
}

/* apply XOR redundancy scheme to dataset files */
static int scr_reddesc_apply_xor(scr_filemap* map, const scr_reddesc* c, int id)
{
  int rc = SCR_SUCCESS;
  int i;

  /* get pointer to XOR state structure */
  scr_reddesc_xor* state = (scr_reddesc_xor*) c->copy_state;

  /* allocate buffer to read a piece of my file */
  char* send_buf = (char*) scr_align_malloc(scr_mpi_buf_size, scr_page_size);
  if (send_buf == NULL) {
    scr_abort(-1, "Allocating memory for send buffer: malloc(%d) errno=%d %s @ %s:%d",
            scr_mpi_buf_size, errno, strerror(errno), __FILE__, __LINE__
    );
  }

  /* allocate buffer to read a piece of the recevied chunk file */
  char* recv_buf = (char*) scr_align_malloc(scr_mpi_buf_size, scr_page_size);
  if (recv_buf == NULL) {
    scr_abort(-1, "Allocating memory for recv buffer: malloc(%d) errno=%d %s @ %s:%d",
            scr_mpi_buf_size, errno, strerror(errno), __FILE__, __LINE__
    );
  }

  /* count the number of files I have and allocate space in structures for each of them */
  int num_files = scr_filemap_num_files(map, id, scr_my_rank_world);
  int* fds = NULL;
  char** filenames = NULL;
  unsigned long* filesizes = NULL;
  if (num_files > 0) {
    fds       = (int*)           malloc(num_files * sizeof(int));
    filenames = (char**)         malloc(num_files * sizeof(char*));
    filesizes = (unsigned long*) malloc(num_files * sizeof(unsigned long));
    if (fds == NULL || filenames == NULL || filesizes == NULL) {
      scr_abort(-1, "Failed to allocate file arrays @ %s:%d",
                __FILE__, __LINE__
      );
    }
  }

  /* record partner's redundancy descriptor hash in our filemap */
  scr_hash* lhs_desc_hash = scr_hash_new();
  scr_hash* my_desc_hash  = scr_hash_new();
  scr_reddesc_store_to_hash(c, my_desc_hash);
  scr_hash_sendrecv(my_desc_hash, state->rhs_rank, lhs_desc_hash, state->lhs_rank, c->comm);
  scr_filemap_set_desc(map, id, state->lhs_rank_world, lhs_desc_hash);
  scr_hash_delete(&my_desc_hash);
  scr_hash_delete(&lhs_desc_hash);

  /* allocate a new xor file header hash */
  scr_hash* header = scr_hash_new();

  /* record the global ranks of the processes in our xor group */
  scr_hash_merge(header, state->group_map);

  /* record dataset in header */
  scr_hash* dataset = scr_hash_new();
  scr_filemap_get_dataset(map, id, scr_my_rank_world, dataset);
  scr_hash_set(header, SCR_KEY_COPY_XOR_DATASET, dataset);

  /* open each file, get the filesize of each, and read the meta data of each */
  scr_hash* current_files = scr_hash_new();
  int file_count = 0;
  unsigned long my_bytes = 0;
  scr_hash_elem* file_elem;
  for (file_elem = scr_filemap_first_file(map, id, scr_my_rank_world);
       file_elem != NULL;
       file_elem = scr_hash_elem_next(file_elem))
  {
    /* get the filename */
    filenames[file_count] = scr_hash_elem_key(file_elem);

    /* get the filesize of this file and add the byte count to the total */
    filesizes[file_count] = scr_file_size(filenames[file_count]);
    my_bytes += filesizes[file_count];

    /* read the meta data for this file and insert it into the current_files hash */
    scr_meta* file_hash = scr_meta_new();
    scr_filemap_get_meta(map, id, scr_my_rank_world, filenames[file_count], file_hash);
    scr_hash_setf(current_files, file_hash, "%d", file_count);

    /* open the file */
    fds[file_count]  = scr_open(filenames[file_count], O_RDONLY);
    if (fds[file_count] < 0) {
      /* TODO: try again? */
      scr_abort(-1, "Opening checkpoint file for copying: scr_open(%s, O_RDONLY) errno=%d %s @ %s:%d",
                filenames[file_count], errno, strerror(errno), __FILE__, __LINE__
      );
    }

    file_count++;
  }

  /* set total number of files we have, plus our rank */
  scr_hash* current_hash = scr_hash_new();
  scr_hash_set_kv_int(current_hash, SCR_KEY_COPY_XOR_RANK,  scr_my_rank_world);
  scr_hash_set_kv_int(current_hash, SCR_KEY_COPY_XOR_FILES, file_count);
  scr_hash_set(current_hash, SCR_KEY_COPY_XOR_FILE, current_files);

  /* exchange file info with partners and add data to our header */
  scr_hash* partner_hash = scr_hash_new();
  scr_hash_sendrecv(current_hash, state->rhs_rank, partner_hash, state->lhs_rank, c->comm);
  scr_hash_set(header, SCR_KEY_COPY_XOR_CURRENT, current_hash);
  scr_hash_set(header, SCR_KEY_COPY_XOR_PARTNER, partner_hash);

  /* allreduce to get maximum filesize */
  unsigned long max_bytes;
  MPI_Allreduce(&my_bytes, &max_bytes, 1, MPI_UNSIGNED_LONG, MPI_MAX, c->comm);

  /* TODO: use unsigned long integer arithmetic (with proper byte padding) instead of char to speed things up */

  /* compute chunk size according to maximum file length and number of ranks in xor set */
  /* if filesize doesn't divide evenly, then add one byte to chunk_size */
  /* TODO: check that ranks > 1 for this divide to be safe (or at partner selection time) */
  size_t chunk_size = max_bytes / (unsigned long) (c->ranks - 1);
  if ((c->ranks - 1) * chunk_size < max_bytes) {
    chunk_size++;
  }

  /* TODO: need something like this to handle 0-byte files? */
  if (chunk_size == 0) {
    chunk_size++;
  }

  /* record the dataset id and the chunk size in the xor chunk header */
  scr_hash_util_set_bytecount(header, SCR_KEY_COPY_XOR_CHUNK, chunk_size);

  /* set chunk filenames of form:  <xor_rank+1>_of_<xor_ranks>_in_<group_id>.xor */
  char my_chunk_file[SCR_MAX_FILENAME];
  char dir[SCR_MAX_FILENAME];
  scr_cache_dir_get(c, id, dir);
  sprintf(my_chunk_file,  "%s/%d_of_%d_in_%d.xor", dir, c->my_rank+1, c->ranks, c->group_id);

  /* record chunk file in filemap before creating it */
  scr_filemap_add_file(map, id, scr_my_rank_world, my_chunk_file);
  scr_filemap_write(scr_map_file, map);

  /* open my chunk file */
  mode_t mode_file = scr_getmode(1, 1, 0);
  int fd_chunk = scr_open(my_chunk_file, O_WRONLY | O_CREAT | O_TRUNC, mode_file);
  if (fd_chunk < 0) {
    /* TODO: try again? */
    scr_abort(-1, "Opening XOR chunk file for writing: scr_open(%s) errno=%d %s @ %s:%d",
            my_chunk_file, errno, strerror(errno), __FILE__, __LINE__
    );
  }

  /* write out the xor chunk header */
  scr_hash_write_fd(my_chunk_file, fd_chunk, header);
  scr_hash_delete(&header);

  MPI_Request request[2];
  MPI_Status  status[2];

  /* XOR Reduce_scatter */
  size_t nread = 0;
  while (nread < chunk_size) {
    size_t count = chunk_size - nread;
    if (count > scr_mpi_buf_size) {
      count = scr_mpi_buf_size;
    }

    int chunk_id;
    for(chunk_id = c->ranks-1; chunk_id >= 0; chunk_id--) {
      /* read the next set of bytes for this chunk from my file into send_buf */
      if (chunk_id > 0) {
        int chunk_id_rel = (c->my_rank + c->ranks + chunk_id) % c->ranks;
        if (chunk_id_rel > c->my_rank) {
          chunk_id_rel--;
        }
        unsigned long offset = chunk_size * (unsigned long) chunk_id_rel + nread;
        if (scr_read_pad_n(num_files, filenames, fds,
                           send_buf, count, offset, filesizes) != SCR_SUCCESS)
        {
          rc = SCR_FAILURE;
        }
      } else {
        memset(send_buf, 0, count);
      }

      /* TODO: XORing with unsigned long would be faster here (if chunk size is multiple of this size) */
      /* merge the blocks via xor operation */
      if (chunk_id < c->ranks-1) {
        for (i = 0; i < count; i++) {
          send_buf[i] ^= recv_buf[i];
        }
      }

      if (chunk_id > 0) {
        /* not our chunk to write, forward it on and get the next */
        MPI_Irecv(recv_buf, count, MPI_BYTE, state->lhs_rank, 0, c->comm, &request[0]);
        MPI_Isend(send_buf, count, MPI_BYTE, state->rhs_rank, 0, c->comm, &request[1]);
        MPI_Waitall(2, request, status);
      } else {
        /* write send block to send chunk file */
        if (scr_write_attempt(my_chunk_file, fd_chunk, send_buf, count) != count) {
          rc = SCR_FAILURE;
        }
      }
    }

    nread += count;
  }

  /* close my chunkfile, with fsync */
  if (scr_close(my_chunk_file, fd_chunk) != SCR_SUCCESS) {
    rc = SCR_FAILURE;
  }

  /* close my checkpoint files */
  for (i=0; i < num_files; i++) {
    scr_close(filenames[i], fds[i]);
  }

  /* free the buffers */
  scr_free(&filesizes);
  /* in this case, we don't free each name, since we copied the pointer to the string in the filemap */
  scr_free(&filenames);
  scr_free(&fds);
  scr_align_free(&send_buf);
  scr_align_free(&recv_buf);

  /* TODO: need to check for errors */
  /* write meta file for xor chunk */
  unsigned long my_chunk_file_size = scr_file_size(my_chunk_file);
  scr_meta* meta = scr_meta_new();
  scr_meta_set_filename(meta, my_chunk_file);
  scr_meta_set_filetype(meta, SCR_META_FILE_XOR);
  scr_meta_set_filesize(meta, my_chunk_file_size);
  scr_meta_set_complete(meta, 1);
  /* TODODSET: move the ranks field elsewhere, for now it's needed by scr_index.c */
  scr_meta_set_ranks(meta, scr_ranks_world);
  scr_filemap_set_meta(map, id, scr_my_rank_world, my_chunk_file, meta);
  scr_filemap_write(scr_map_file, map);
  scr_meta_delete(&meta);

  /* if crc_on_copy is set, compute and store CRC32 value for chunk file */
  if (scr_crc_on_copy) {
    scr_compute_crc(map, id, scr_my_rank_world, my_chunk_file);
    /* TODO: would be nice to save this CRC in our partner's XOR file so we can check correctness on a rebuild */
  }

  return rc;
}

/* apply redundancy scheme to file and return number of bytes copied in bytes parameter */
int scr_reddesc_apply(scr_filemap* map, const scr_reddesc* c, int id, double* bytes)
{
  /* initialize to 0 */
  *bytes = 0.0;

  /* step through each of my files for the specified dataset to scan for any incomplete files */
  int valid = 1;
  double my_bytes = 0.0;
  scr_hash_elem* file_elem;
  for (file_elem = scr_filemap_first_file(map, id, scr_my_rank_world);
       file_elem != NULL;
       file_elem = scr_hash_elem_next(file_elem))
  {
    /* get the filename */
    char* file = scr_hash_elem_key(file_elem);

    /* check the file */
    if (!scr_bool_have_file(map, id, scr_my_rank_world, file, scr_ranks_world)) {
      scr_dbg(2, "File determined to be invalid: %s", file);
      valid = 0;
    }

    /* add up the number of bytes on our way through */
    my_bytes += (double) scr_file_size(file);

    /* if crc_on_copy is set, compute crc and update meta file (PARTNER does this during the copy) */
    if (scr_crc_on_copy && c->copy_type != SCR_COPY_PARTNER) {
      scr_compute_crc(map, id, scr_my_rank_world, file);
    }
  }

  /* determine whether everyone's files are good */
  int all_valid = scr_alltrue(valid);
  if (! all_valid) {
    if (scr_my_rank_world == 0) {
      scr_dbg(1, "Exiting copy since one or more checkpoint files is invalid");
    }
    return SCR_FAILURE;
  }

  /* start timer */
  time_t timestamp_start;
  double time_start;
  if (scr_my_rank_world == 0) {
    timestamp_start = scr_log_seconds();
    time_start = MPI_Wtime();
  }

  /* apply the redundancy scheme */
  int rc = SCR_FAILURE;
  switch (c->copy_type) {
  case SCR_COPY_SINGLE:
    rc = SCR_SUCCESS;
    break;
  case SCR_COPY_PARTNER:
    rc = scr_reddesc_apply_partner(map, c, id);
    break;
  case SCR_COPY_XOR:
    rc = scr_reddesc_apply_xor(map, c, id);
    break;
  }

  /* record the number of files this task wrote during this dataset 
   * (need to remember when a task writes 0 files) */
  int num_files = scr_filemap_num_files(map, id, scr_my_rank_world);
  scr_filemap_set_expected_files(map, id, scr_my_rank_world, num_files);
  scr_filemap_write(scr_map_file, map);

  /* determine whether everyone succeeded in their copy */
  int valid_copy = (rc == SCR_SUCCESS);
  if (! valid_copy) {
    scr_err("scr_copy_files failed with return code %d @ %s:%d", rc, __FILE__, __LINE__);
  }
  int all_valid_copy = scr_alltrue(valid_copy);
  rc = all_valid_copy ? SCR_SUCCESS : SCR_FAILURE;

  /* add up total number of bytes */
  MPI_Allreduce(&my_bytes, bytes, 1, MPI_DOUBLE, MPI_SUM, scr_comm_world);

  /* stop timer and report performance info */
  if (scr_my_rank_world == 0) {
    double time_end = MPI_Wtime();
    double time_diff = time_end - time_start;
    double bw = *bytes / (1024.0 * 1024.0 * time_diff);
    scr_dbg(1, "scr_copy_files: %f secs, %e bytes, %f MB/s, %f MB/s per proc",
            time_diff, *bytes, bw, bw/scr_ranks_world
    );

    /* log data on the copy in the database */
    if (scr_log_enable) {
      char dir[SCR_MAX_FILENAME];
      scr_cache_dir_get(c, id, dir);
      scr_log_transfer("COPY", c->base, dir, &id, &timestamp_start, &time_diff, bytes);
    }
  }

  return rc;
}
