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
    int* ranks = (int*) malloc(scr_storedesc_cntl->ranks * sizeof(int));
    if (ranks == NULL) {
      scr_abort(-1,"Failed to allocate memory to record local rank list @ %s:%d",
         __FILE__, __LINE__
      );
    }
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

/* this moves all files in the cache to make them accessible
 * to new rank mapping */
static int scr_distribute_files(scr_filemap* map, const scr_reddesc* red, int id)
{
  int i, round;
  int rc = SCR_SUCCESS;

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
  int* have_rank_by_round = (int*) malloc(sizeof(int) * nranks);
  int* send_flag_by_round = (int*) malloc(sizeof(int) * nranks);
  if (have_rank_by_round == NULL || send_flag_by_round == NULL) {
    scr_abort(-1,"Failed to allocate memory to record rank id by round @ %s:%d",
      __FILE__, __LINE__
    );
  }

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

  /* get the path for this dataset */
  char dir[SCR_MAX_FILENAME];
  scr_cache_dir_get(red, id, dir);

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

      /* iterate over and rename each file */
      for (i=0; i < numfiles; i++) {
        /* get the current file name */
        char* file = files[i];

        /* build the new file name */
        scr_path* path_newfile = scr_path_from_str(file);
        scr_path_basename(path_newfile);
        scr_path_prepend_str(path_newfile, dir);
        char* newfile = scr_path_strdup(path_newfile);

        /* if the new file name is different from the old name, rename it */
        if (strcmp(file, newfile) != 0) {
          /* record the new filename to our map and write it to disk */
          scr_filemap_add_file(map, id, send_rank, newfile);
          scr_meta* oldmeta = scr_meta_new();
          scr_filemap_get_meta(map, id, send_rank, file, oldmeta);
          scr_filemap_set_meta(map, id, send_rank, newfile, oldmeta);
          scr_filemap_write(scr_map_file, map);
          scr_meta_delete(&oldmeta);

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

          /* exhange file names with partners */
          char file_partner[SCR_MAX_FILENAME];
          scr_swap_file_names(
            file, send_rank, file_partner, sizeof(file_partner), recv_rank,
            dir, scr_comm_world
          );

          /* if we'll receive a file, record the name of our file
           * in the filemap and write it to disk */
          scr_meta* recv_meta = NULL;
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

  /* get the list of datasets we have in our cache */
  int ndsets;
  int* dsets;
  scr_filemap_list_datasets(map, &ndsets, &dsets);

  /* TODO: put dataset selection logic into a function */

  /* TODO: also attempt to recover datasets which we were in the
   * middle of flushing */
  int current_id;
  int dset_index = 0;
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

      /* distribute dataset descriptor for this dataset */
      int rebuild_succeeded = 0;
      if (scr_distribute_datasets(map, current_id) == SCR_SUCCESS) {
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

            /* if we rebuild any checkpoint, return success */
            rc = SCR_SUCCESS;

            /* update scr_dataset_id */
            if (current_id > scr_dataset_id) {
              scr_dataset_id = current_id;
            }

            /* TODO: dataset may not be a checkpoint */
            /* update scr_checkpoint_id */
            if (current_id > scr_checkpoint_id) {
              scr_checkpoint_id = current_id;
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
      }

      /* if the distribute or rebuild failed, delete the dataset */
      if (! rebuild_succeeded) {
        /* log that we failed */
        if (scr_my_rank_world == 0) {
          scr_dbg(1, "Failed to distribute and rebuild dataset %d", current_id);
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

  /* free our list of dataset ids */
  scr_free(&dsets);

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

    /* get list of dataset ids in flush file */
    int flush_ndsets;
    int* flush_dsets;
    scr_hash* flush_dsets_hash = scr_hash_get(hash, SCR_FLUSH_KEY_DATASET);
    scr_hash_list_int(flush_dsets_hash, &flush_ndsets, &flush_dsets);

    /* get list of dataset ids in cache */
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
