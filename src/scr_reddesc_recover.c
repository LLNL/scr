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

/*
=========================================
Distribute and file rebuild functions
=========================================
*/

/* returns true if a an XOR file is found for this rank for the given checkpoint id,
 * sets xor_file to full filename */
static int scr_bool_have_xor_file(scr_filemap* map, int checkpoint_id, char* xor_file)
{
  int rc = 0;

  /* find the name of my xor chunk file: read filemap and check filetype of each file */
  scr_hash_elem* file_elem = NULL;
  for (file_elem = scr_filemap_first_file(map, checkpoint_id, scr_my_rank_world);
       file_elem != NULL;
       file_elem = scr_hash_elem_next(file_elem))
  {
    /* get the filename */
    char* file = scr_hash_elem_key(file_elem);

    /* read the meta for this file */
    scr_meta* meta = scr_meta_new();
    scr_filemap_get_meta(map, checkpoint_id, scr_my_rank_world, file, meta);

    /* if the filetype of this file is an XOR fule, copy the filename and bail out */
    char* filetype = NULL;
    if (scr_meta_get_filetype(meta, &filetype) == SCR_SUCCESS) {
      if (strcmp(filetype, SCR_META_FILE_XOR) == 0) {
        strcpy(xor_file, file);
        rc = 1;
        scr_meta_delete(&meta);
        break;
      }
    }

    /* free the meta data for this file and go on to the next */
    scr_meta_delete(&meta);
  }

  return rc;
}

/* given a filemap, a redundancy descriptor, a dataset id, and a failed rank in my xor set,
 * rebuild files and add them to the filemap */
static int scr_reddesc_recover_xor(scr_filemap* map, const scr_reddesc* c, int id, int root)
{
  int rc = SCR_SUCCESS;
  int i;
  MPI_Status status[2];

  int fd_chunk = 0;
  char full_chunk_filename[SCR_MAX_FILENAME] = "";

  int* fds = NULL;
  char** filenames = NULL;
  unsigned long* filesizes = NULL;

  /* get pointer to XOR state structure */
  scr_reddesc_xor* state = (scr_reddesc_xor*) c->copy_state;

  /* allocate hash object to read in (or receive) the header of the XOR file */
  scr_hash* header = scr_hash_new();

  int num_files = -1;
  scr_hash* current_hash = NULL;
  if (root != c->my_rank) {
    /* lookup name of xor file */
    if (! scr_bool_have_xor_file(map, id, full_chunk_filename)) {
      scr_abort(-1, "Missing XOR file %s @ %s:%d",
        full_chunk_filename, __FILE__, __LINE__
      );
    }

    /* open our xor file for reading */
    fd_chunk = scr_open(full_chunk_filename, O_RDONLY);
    if (fd_chunk < 0) {
      scr_abort(-1, "Opening XOR file for reading in XOR rebuild: scr_open(%s, O_RDONLY) errno=%d %s @ %s:%d",
        full_chunk_filename, errno, strerror(errno), __FILE__, __LINE__
      );
    }

    /* read in the xor chunk header */
    scr_hash_read_fd(full_chunk_filename, fd_chunk, header);

    /* lookup number of files this process wrote */
    current_hash = scr_hash_get(header, SCR_KEY_COPY_XOR_CURRENT);
    if (scr_hash_util_get_int(current_hash, SCR_KEY_COPY_XOR_FILES, &num_files) != SCR_SUCCESS) {
      scr_abort(-1, "Failed to read number of files from XOR file header: %s @ %s:%d",
        full_chunk_filename, __FILE__, __LINE__
      );
    }

    /* allocate arrays to hold file descriptors, filenames, and filesizes for each of our files */
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

    /* get path from chunk file */
    scr_path* path_chunk = scr_path_from_str(full_chunk_filename);
    scr_path_dirname(path_chunk);

    /* open each of our files */
    for (i=0; i < num_files; i++) {
      /* lookup meta data for this file from header hash */
      scr_hash* meta_tmp = scr_hash_get_kv_int(current_hash, SCR_KEY_COPY_XOR_FILE, i);
      if (meta_tmp == NULL) {
        scr_abort(-1, "Failed to find file %d in XOR file header %s @ %s:%d",
          i, full_chunk_filename, __FILE__, __LINE__
        );
      }

      /* get pointer to filename for this file */
      char* filename = NULL;
      if (scr_meta_get_filename(meta_tmp, &filename) != SCR_SUCCESS) {
        scr_abort(-1, "Failed to read filename for file %d in XOR file header %s @ %s:%d",
          i, full_chunk_filename, __FILE__, __LINE__
        );
      }

      /* create full path to the file */
      scr_path* path_full_file = scr_path_dup(path_chunk);
      scr_path_append_str(path_full_file, filename);
      char* full_file = scr_path_strdup(path_full_file);

      /* copy the full filename */
      filenames[i] = strdup(full_file);
      if (filenames[i] == NULL) {
        scr_abort(-1, "Failed to copy filename during rebuild @ %s:%d",
          full_file, __FILE__, __LINE__
        );
      }

      /* lookup the filesize */
      if (scr_meta_get_filesize(meta_tmp, &filesizes[i]) != SCR_SUCCESS) {
        scr_abort(-1, "Failed to read file size for file %s in XOR file header during rebuild @ %s:%d",
          full_file, __FILE__, __LINE__
        );
      }

      /* open the file for reading */
      fds[i] = scr_open(full_file, O_RDONLY);
      if (fds[i] < 0) {
        /* TODO: try again? */
        scr_abort(-1, "Opening checkpoint file for reading in XOR rebuild: scr_open(%s, O_RDONLY) errno=%d %s @ %s:%d",
          full_file, errno, strerror(errno), __FILE__, __LINE__
        );
      }

      /* free the path and string for the file name */
      scr_free(&full_file);
      scr_path_delete(&path_full_file);
    }

    /* free the chunk path */
    scr_path_delete(&path_chunk);

    /* if failed rank is to my left, i have the meta for his files, send him the header */
    if (root == state->lhs_rank) {
      scr_hash_send(header, state->lhs_rank, c->comm);
    }

    /* if failed rank is to my right, send him my file info so he can write his XOR header */
    if (root == state->rhs_rank) {
      scr_hash_send(current_hash, state->rhs_rank, c->comm);
    }
  } else {
    /* receive the header from right-side partner;
     * includes number of files and meta data for my files, as well as, 
     * the checkpoint id and the chunk size */
    scr_hash_recv(header, state->rhs_rank, c->comm);

    /* rename PARTNER to CURRENT in our header */
    current_hash = scr_hash_new();
    scr_hash* old_hash = scr_hash_get(header, SCR_KEY_COPY_XOR_PARTNER);
    scr_hash_merge(current_hash, old_hash);
    scr_hash_unset(header, SCR_KEY_COPY_XOR_CURRENT);
    scr_hash_unset(header, SCR_KEY_COPY_XOR_PARTNER);
    scr_hash_set(header, SCR_KEY_COPY_XOR_CURRENT, current_hash);

    /* receive number of files our left-side partner has and allocate an array of
     * meta structures to store info */
    scr_hash* partner_hash = scr_hash_new();
    scr_hash_recv(partner_hash, state->lhs_rank, c->comm);
    scr_hash_set(header, SCR_KEY_COPY_XOR_PARTNER, partner_hash);

    /* get the number of files */
    if (scr_hash_util_get_int(current_hash, SCR_KEY_COPY_XOR_FILES, &num_files) != SCR_SUCCESS) {
      scr_abort(-1, "Failed to read number of files from XOR file header during rebuild @ %s:%d",
        __FILE__, __LINE__
      );
    }
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

    /* get dataset directory */
    char dir[SCR_MAX_FILENAME];
    scr_cache_dir_get(c, id, dir);

    /* set chunk filename of form: <xor_rank+1>_of_<xor_groupsize>_in_<xor_groupid>.xor */
    scr_path* path_full_chunk_filename = scr_path_from_str(dir);
    scr_path_append_strf(path_full_chunk_filename, "%d_of_%d_in_%d.xor", dir, c->my_rank+1, c->ranks, c->group_id);
    scr_path_strcpy(full_chunk_filename, sizeof(full_chunk_filename), path_full_chunk_filename);
    scr_path_delete(&path_full_chunk_filename);

    /* split file into path and name */
    scr_path* path_chunk = scr_path_from_str(full_chunk_filename);
    scr_path_dirname(path_chunk);

    /* record our chunk file and each of our files in the filemap before creating */
    scr_filemap_add_file(map, id, scr_my_rank_world, full_chunk_filename);
    for (i=0; i < num_files; i++) {
      /* lookup meta data for this file from header hash */
      scr_hash* meta_tmp = scr_hash_get_kv_int(current_hash, SCR_KEY_COPY_XOR_FILE, i);
      if (meta_tmp == NULL) {
        scr_abort(-1, "Failed to find file %d in XOR file header %s @ %s:%d",
          i, full_chunk_filename, __FILE__, __LINE__
        );
      }

      /* get pointer to filename for this file */
      char* filename = NULL;
      if (scr_meta_get_filename(meta_tmp, &filename) != SCR_SUCCESS) {
        scr_abort(-1, "Failed to read filename for file %d in XOR file header %s @ %s:%d",
          i, full_chunk_filename, __FILE__, __LINE__
        );
      }

      /* get the filename */
      scr_path* path_full_file = scr_path_dup(path_chunk);
      scr_path_append_str(path_full_file, filename);
      char* full_file = scr_path_strdup(path_full_file);

      /* copy the filename */
      filenames[i] = strdup(full_file);
      if (filenames[i] == NULL) {
        scr_abort(-1, "Failed to copy filename during rebuild @ %s:%d",
          full_file, __FILE__, __LINE__
        );
      }

      /* get the filesize */
      if (scr_meta_get_filesize(meta_tmp, &filesizes[i]) != SCR_SUCCESS) {
        scr_abort(-1, "Failed to read file size for file %s in XOR file header during rebuild @ %s:%d",
          full_file, __FILE__, __LINE__
        );
      }

      /* add the file to our filemap */
      scr_filemap_add_file(map, id, scr_my_rank_world, full_file);

      /* free path and string for file name */
      scr_free(&full_file);
      scr_path_delete(&path_full_file);
    }
    scr_filemap_set_expected_files(map, id, scr_my_rank_world, num_files + 1);
    scr_filemap_write(scr_map_file, map);

    /* free the chunk path */
    scr_path_delete(&path_chunk);

    /* get permissions for file */
    mode_t mode_file = scr_getmode(1, 1, 0);

    /* open my chunk file for writing */
    fd_chunk = scr_open(full_chunk_filename, O_WRONLY | O_CREAT | O_TRUNC, mode_file);
    if (fd_chunk < 0) {
      /* TODO: try again? */
      scr_abort(-1, "Opening XOR chunk file for writing in XOR rebuild: scr_open(%s) errno=%d %s @ %s:%d",
        full_chunk_filename, errno, strerror(errno), __FILE__, __LINE__
      );
    }

    /* open each of my files for writing */
    for (i=0; i < num_files; i++) {
      /* open my file for writing */
      fds[i] = scr_open(filenames[i], O_WRONLY | O_CREAT | O_TRUNC, mode_file);
      if (fds[i] < 0) {
        /* TODO: try again? */
        scr_abort(-1, "Opening file for writing in XOR rebuild: scr_open(%s) errno=%d %s @ %s:%d",
          filenames[i], errno, strerror(errno), __FILE__, __LINE__
        );
      }
    }

    /* write XOR chunk file header */
    scr_hash_write_fd(full_chunk_filename, fd_chunk, header);
  }

  /* read the chunk size used to compute the xor data */
  unsigned long chunk_size;
  if (scr_hash_util_get_unsigned_long(header, SCR_KEY_COPY_XOR_CHUNK, &chunk_size) != SCR_SUCCESS) {
    scr_abort(-1, "Failed to read chunk size from XOR file header %s @ %s:%d",
      full_chunk_filename, __FILE__, __LINE__
    );
  }

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

  /* Pipelined XOR Reduce to root */
  unsigned long offset = 0;
  int chunk_id;
  for (chunk_id = 0; chunk_id < c->ranks; chunk_id++) {
    size_t nread = 0;
    while (nread < chunk_size) {
      size_t count = chunk_size - nread;
      if (count > scr_mpi_buf_size) {
        count = scr_mpi_buf_size;
      }

      if (root != c->my_rank) {
        /* read the next set of bytes for this chunk from my file into send_buf */
        if (chunk_id != c->my_rank) {
          /* for this chunk, read data from the logical file */
          if (scr_read_pad_n(num_files, filenames, fds,
                             send_buf, count, offset, filesizes) != SCR_SUCCESS)
          {
            /* read failed, make sure we fail this rebuild */
            rc = SCR_FAILURE;
          }
          offset += count;
        } else {
          /* for this chunk, read data from the XOR file */
          if (scr_read_attempt(full_chunk_filename, fd_chunk, send_buf, count) != count) {
            /* read failed, make sure we fail this rebuild */
            rc = SCR_FAILURE;
          }
        }

        /* if not start of pipeline, receive data from left and xor with my own */
        if (root != state->lhs_rank) {
          int i;
          MPI_Recv(recv_buf, count, MPI_BYTE, state->lhs_rank, 0, c->comm, &status[0]);
          for (i = 0; i < count; i++) {
            send_buf[i] ^= recv_buf[i];
          }
        }

        /* send data to right-side partner */
        MPI_Send(send_buf, count, MPI_BYTE, state->rhs_rank, 0, c->comm);
      } else {
        /* root of rebuild, just receive incoming chunks and write them out */
        MPI_Recv(recv_buf, count, MPI_BYTE, state->lhs_rank, 0, c->comm, &status[0]);

        /* if this is not my xor chunk, write data to normal file, otherwise write to my xor chunk */
        if (chunk_id != c->my_rank) {
          /* for this chunk, write data to the logical file */
          if (scr_write_pad_n(num_files, filenames, fds,
                              recv_buf, count, offset, filesizes) != SCR_SUCCESS)
          {
            /* write failed, make sure we fail this rebuild */
            rc = SCR_FAILURE;
          }
          offset += count;
        } else {
          /* for this chunk, write data from the XOR file */
          if (scr_write_attempt(full_chunk_filename, fd_chunk, recv_buf, count) != count) {
            /* write failed, make sure we fail this rebuild */
            rc = SCR_FAILURE;
          }
        }
      }

      nread += count;
    }
  }

  /* close my chunkfile */
  if (scr_close(full_chunk_filename, fd_chunk) != SCR_SUCCESS) {
    rc = SCR_FAILURE;
  }

  /* close my checkpoint files */
  for (i=0; i < num_files; i++) {
    if (scr_close(filenames[i], fds[i]) != SCR_SUCCESS) {
      rc = SCR_FAILURE;
    }
  }

  /* if i'm the rebuild rank, complete my file and xor chunk */
  if (root == c->my_rank) {
    /* complete each of our files and mark each as complete */
    for (i=0; i < num_files; i++) {
      /* TODO: need to check for errors, check that file is really valid */

      /* fill out meta info for our file and complete it */
      scr_hash* meta_tmp = scr_hash_get_kv_int(current_hash, SCR_KEY_COPY_XOR_FILE, i);
      scr_filemap_set_meta(map, id, scr_my_rank_world, filenames[i], meta_tmp);

      /* TODODSET:write out filemap here? */

      /* if crc_on_copy is set, compute and store CRC32 value for each file */
      if (scr_crc_on_copy) {
        /* check for mismatches here, in case we failed to rebuild the file correctly */
        if (scr_compute_crc(map, id, scr_my_rank_world, filenames[i]) != SCR_SUCCESS) {
          scr_err("Failed to verify CRC32 after rebuild on file %s @ %s:%d",
            filenames[i], __FILE__, __LINE__
          );

          /* make sure we fail this rebuild */
          rc = SCR_FAILURE;
        }
      }
    }

    /* create meta data for chunk and complete it */
    unsigned long full_chunk_filesize = scr_file_size(full_chunk_filename);
    scr_meta* meta_chunk = scr_meta_new();
    scr_meta_set_filename(meta_chunk, full_chunk_filename);
    scr_meta_set_filetype(meta_chunk, SCR_META_FILE_XOR);
    scr_meta_set_filesize(meta_chunk, full_chunk_filesize);
    scr_meta_set_complete(meta_chunk, 1);
    /* TODODSET: move the ranks field elsewhere, for now it's needed by scr_index.c */
    scr_meta_set_ranks(meta_chunk, scr_ranks_world);
    scr_filemap_set_meta(map, id, scr_my_rank_world, full_chunk_filename, meta_chunk);
    scr_filemap_write(scr_map_file, map);
    scr_meta_delete(&meta_chunk);

    /* if crc_on_copy is set, compute and store CRC32 value for chunk file */
    if (scr_crc_on_copy) {
      /* TODO: would be nice to check for mismatches here, but we did not save this value in the partner XOR file */
      scr_compute_crc(map, id, scr_my_rank_world, full_chunk_filename);
    }
  }

  /* free the buffers */
  scr_align_free(&recv_buf);
  scr_align_free(&send_buf);
  scr_free(&filesizes);
  if (filenames != NULL) {
    /* free each of the filenames we strdup'd */
    for (i=0; i < num_files; i++) {
      scr_free(&filenames[i]);
    }
    scr_free(&filenames);
  }
  scr_free(&fds);
  scr_hash_delete(&header);

  return rc;
}

/* given a dataset id, check whether files can be rebuilt via xor and execute the rebuild if needed */
static int scr_reddesc_recover_xor_attempt(scr_filemap* map, const scr_reddesc* c, int id)
{
  /* check whether we have our files */
  int have_my_files = scr_bool_have_files(map, id, scr_my_rank_world);

  /* check whether we have our XOR file */
  char xor_file[SCR_MAX_FILENAME];
  if (! scr_bool_have_xor_file(map, id, xor_file)) {
    have_my_files = 0;
  }

  /* TODO: check whether each of the files listed in our xor file exists? */

  /* check whether I have my full checkpoint file, assume I don't */
  int need_rebuild = 1;
  if (have_my_files) {
    need_rebuild = 0;
  }

  /* count how many in my xor set need to rebuild */
  int total_rebuild;
  MPI_Allreduce(&need_rebuild, &total_rebuild, 1, MPI_INT, MPI_SUM, c->comm); 

  /* check whether all sets can rebuild, if not, bail out */
  int set_can_rebuild = (total_rebuild <= 1);
  if (! scr_alltrue(set_can_rebuild)) {
    if (scr_my_rank_world == 0) {
      scr_err("Cannot rebuild missing files @ %s:%d", __FILE__, __LINE__);
    }
    return SCR_FAILURE;
  }

  /* it's possible to rebuild; rebuild if we need to */
  int rc = SCR_SUCCESS;
  if (total_rebuild > 0) {
    /* someone in my set needs to rebuild, determine who */
    int tmp_rank = need_rebuild ? c->my_rank : -1;
    int rebuild_rank;
    MPI_Allreduce(&tmp_rank, &rebuild_rank, 1, MPI_INT, MPI_MAX, c->comm);

    /* rebuild */
    if (need_rebuild) {
      scr_dbg(1, "Rebuilding file from XOR segments");
    }
    rc = scr_reddesc_recover_xor(map, c, id, rebuild_rank);
  }

  /* check whether all sets rebuilt ok */
  if (! scr_alltrue(rc == SCR_SUCCESS)) {
    if (scr_my_rank_world == 0) {
      scr_dbg(1, "One or more processes failed to rebuild its files @ %s:%d", __FILE__, __LINE__);
    }
    return SCR_FAILURE;
  }

  return SCR_SUCCESS;
}

/* rebuilds files for specified dataset id using specified redundancy descriptor,
 * adds them to filemap, and returns SCR_SUCCESS if all processes succeeded */
int scr_reddesc_recover(scr_filemap* map, const scr_reddesc* c, int id)
{
  int rc = SCR_SUCCESS;

  /* for xor, need to call rebuild_xor here */
  if (c->copy_type == SCR_COPY_XOR) {
    rc = scr_reddesc_recover_xor_attempt(map, c, id);
  }

  /* check that rebuild worked */
  if (rc != SCR_SUCCESS) {
    if (scr_my_rank_world == 0) {
      scr_dbg(1, "Missing files @ %s:%d",
        __FILE__, __LINE__
      );
    }
    return SCR_FAILURE;
  }

  /* at this point, we should have all of our files, check that they're all here */

  /* check whether everyone has their files */
  int have_my_files = scr_bool_have_files(map, id, scr_my_rank_world);
  if (! scr_alltrue(have_my_files)) {
    if (scr_my_rank_world == 0) {
      scr_dbg(1, "Missing files @ %s:%d",
        __FILE__, __LINE__
      );
    }
    return SCR_FAILURE;
  }

  /* for SINGLE and PARTNER, we need to apply the copy to complete the rebuild,
   * with XOR the copy is done as part of the rebuild process */
  if (c->copy_type == SCR_COPY_SINGLE || c->copy_type == SCR_COPY_PARTNER) {
    double bytes_copied = 0.0;
    rc = scr_reddesc_apply(map, c, id, &bytes_copied);
  }

  return rc;
}
