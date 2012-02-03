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

#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "mpi.h"

#include "scr_globals.h"

#ifdef HAVE_LIBGCS
#include "gcs.h"
#endif /* HAVE_LIBGCS */

/*
=========================================
Group descriptor functions
=========================================
*/

/* initialize the specified group descriptor */
int scr_groupdesc_init(scr_groupdesc* d)
{
  /* check that we got a valid group descriptor */
  if (d == NULL) {
    scr_err("No group descriptor @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* initialize the descriptor */
  d->enabled      =  0;
  d->index        = -1;
  d->name         = NULL;
  d->comm         = MPI_COMM_NULL;
  d->rank         = MPI_PROC_NULL;
  d->ranks        = 0;
  d->comm_across  = MPI_COMM_NULL;
  d->rank_across  = MPI_PROC_NULL;
  d->ranks_across = 0;

  return SCR_SUCCESS;
}

/* free any memory associated with the specified group descriptor */
int scr_groupdesc_free(scr_groupdesc* d)
{
  /* free the strings we strdup'd */
  scr_free(&d->name);

  /* free the communicator we created */
  if (d->comm != MPI_COMM_NULL) {
    MPI_Comm_free(&d->comm);
  }

  /* free the communicator we created */
  if (d->comm_across != MPI_COMM_NULL) {
    MPI_Comm_free(&d->comm_across);
  }

  return SCR_SUCCESS;
}

/* split processes into groups who have matching strings */
static int scr_split_by_string(const char* str, MPI_Comm* comm)
{
  /* determine the length of the maximum string (including terminating NULL character) */
  int str_len = strlen(str) + 1;
  int max_len;
  MPI_Allreduce(&str_len, &max_len, 1, MPI_INT, MPI_MAX, scr_comm_world);

  /* allocate a buffer and copy our string to it */
  char* tmp_str = NULL;
  if (max_len > 0) {
    tmp_str = (char*) malloc(max_len);
    strcpy(tmp_str, str);
  }

#ifdef HAVE_LIBGCS
  /* split ranks based on string */
  GCS_Comm_splitv(
    scr_comm_world,
    tmp_str, max_len, GCS_CMP_STR,
    NULL,    0,       GCS_CMP_IGNORE,
    comm
  );
#else /* HAVE_LIBGCS */
  /* allocate buffer to receive string from each process */
  char* buf = NULL;
  if (max_len > 0) {
    buf = (char*) malloc(max_len * scr_ranks_world);
  }

  /* receive all strings */
  MPI_Allgather(tmp_str, max_len, MPI_CHAR, buf, max_len, MPI_CHAR, scr_comm_world);

  /* search through strings until we find one that matches our own */
  int index = 0;
  char* current = buf;
  while (index < scr_ranks_world) {
    /* compare string from rank index to our own, break if we find a match */
    if (strcmp(current, tmp_str) == 0) {
      break;
    }

    /* advance to string from next process */
    current += max_len;
    index++;
  }

  /* split comm world into subcommunicators based on the index of the first match */
  MPI_Comm_split(scr_comm_world, index, 0, comm);

  /* free our temporary buffer of all strings */
  scr_free(&buf);
#endif

  /* free temporary copy of string */
  scr_free(&tmp_str);

  return SCR_SUCCESS;
}

/* build a group descriptor of all procs on the same node */
int scr_groupdesc_create_by_str(scr_groupdesc* d, int index, const char* key, const char* value)
{
  /* initialize the descriptor */
  scr_groupdesc_init(d);

  /* enable descriptor, record its index, and copy its name */
  d->enabled = 1;
  d->index   = index;
  d->name    = strdup(key);

  /* get communicator of all tasks with same value */
  scr_split_by_string(value, &d->comm);

  /* find our position in the group communicator */
  MPI_Comm_rank(d->comm, &d->rank);
  MPI_Comm_size(d->comm, &d->ranks);

  /* Based on my group rank, create communicators consisting of all tasks at same group rank level */
  MPI_Comm_split(scr_comm_world, d->rank, scr_my_rank_world, &d->comm_across);

  /* find our position in the communicator */
  MPI_Comm_rank(d->comm_across, &d->rank_across);
  MPI_Comm_size(d->comm_across, &d->ranks_across);

  return SCR_SUCCESS;
}

#if 0
  /* TODO: maybe a better way to identify processes on the same node?
   * TODO: could improve scalability here using a parallel sort and prefix scan
   * TODO: need something to work on systems with IPv6
   * Assumes: same int(IP) ==> same node 
   *   1. Get IP address as integer data type
   *   2. Allgather IP addresses from all processes
   *   3. Set color id to process with highest rank having the same IP */

  /* get IP address as integer data type */
  struct hostent *hostent;
  hostent = gethostbyname(scr_my_hostname);
  if (hostent == NULL) {
    scr_err("Fetching host information: gethostbyname(%s) @ %s:%d",
            scr_my_hostname, __FILE__, __LINE__
    );
    MPI_Abort(scr_comm_world, 0);
  }
  int host_id = (int) ((struct in_addr *) hostent->h_addr_list[0])->s_addr;

  /* gather all host_id values */
  int* host_ids = (int*) malloc(scr_ranks_world * sizeof(int));
  if (host_ids == NULL) {
    scr_err("Can't allocate memory to determine which processes are on the same node @ %s:%d",
            __FILE__, __LINE__
    );
    MPI_Abort(scr_comm_world, 0);
  }
  MPI_Allgather(&host_id, 1, MPI_INT, host_ids, 1, MPI_INT, scr_comm_world);

  /* set host_index to the highest rank having the same host_id as we do */
  int host_index = 0;
  for (i=0; i < scr_ranks_world; i++) {
    if (host_ids[i] == host_id) {
      host_index = i;
    }
  }
  scr_free(&host_ids);

  /* finally create the communicator holding all ranks on the same node */
  MPI_Comm_split(scr_comm_world, host_index, scr_my_rank_world, &d->comm);
#endif

/*
=========================================
Routines that operate on scr_groupdescs array
=========================================
*/

int scr_groupdescs_index_from_name(const char* name)
{
  int index = -1;

  if (name == NULL) {
    return index;
  }

  int i;
  for (i = 0; i < scr_ngroupdescs; i++) {
    if (strcmp(name, scr_groupdescs[i].name) == 0) {
      index = i;
      break;
    }
  }

  return index;
}

scr_groupdesc* scr_groupdescs_from_name(const char* name)
{
  int index = scr_groupdescs_index_from_name(name);
  if (index < 0) {
    return NULL;
  }
  return &scr_groupdescs[index];
}

int scr_groupdescs_create()
{
  int i;
  int all_valid = 1;

  /* get groups defined for our hostname */
  scr_hash* groups = scr_hash_get_kv(scr_groupdesc_hash, SCR_CONFIG_KEY_GROUPDESC, scr_my_hostname);

  /* set the number of group descriptors,
   * we define one for all procs on the same node */
  int num_groups = scr_hash_size(groups);
  int count = num_groups + 1;

  /* set our count to maximum count across all procs */
  MPI_Allreduce(&count, &scr_ngroupdescs, 1, MPI_INT, MPI_MAX, scr_comm_world);

  /* allocate our group descriptors */
  if (scr_ngroupdescs > 0) {
    scr_groupdescs = (scr_groupdesc*) malloc(scr_ngroupdescs * sizeof(scr_groupdesc));
    /* TODO: check for errors */
  }

  /* disable all group descriptors until we build each one */
  for (i = 0; i < scr_ngroupdescs; i++) {
    scr_groupdesc_init(&scr_groupdescs[i]);
  }

  /* create group descriptor for all procs on the same node */
  int index = 0;
  scr_groupdesc_create_by_str(&scr_groupdescs[index], index, SCR_GROUP_NODE, scr_my_hostname);
  index++;

  /* in order to form groups in the same order on all procs,
   * we have rank 0 decide the order */

  /* determine number of entries on rank 0 */
  MPI_Bcast(&num_groups, 1, MPI_INT, 0, scr_comm_world);

  /* iterate over each of our hash entries filling in each corresponding descriptor */
  int key_len;
  char key_name[SCR_MAX_FILENAME];
  char* value;
  int have_match;
  if (scr_my_rank_world == 0) {
    scr_hash_elem* elem;
    for (elem = scr_hash_elem_first(groups);
         elem != NULL;
         elem = scr_hash_elem_next(elem))
    {
      /* get key for this group */
      char* key = scr_hash_elem_key(elem);

      /* bcast length of key */
      key_len = strlen(key) + 1;
      MPI_Bcast(&key_len, 1, MPI_INT, 0, scr_comm_world);

      /* bcast key name */
      MPI_Bcast(key, key_len, MPI_CHAR, 0, scr_comm_world);

      /* determine whether all procs have corresponding entry */
      have_match = 1;
      if (scr_hash_util_get_str(groups, key, &value) != SCR_SUCCESS) {
        have_match = 0;
      }
      if (scr_alltrue(have_match)) {
        /* create group */
        scr_groupdesc_create_by_str(&scr_groupdescs[index], index, key, value);
        index++;
      } else {
        /* TODO: print error */
      }
    }
  } else {
    for (i = 0; i < num_groups; i++) {
      /* receive length of key */
      MPI_Bcast(&key_len, 1, MPI_INT, 0, scr_comm_world);

      /* receive key */
      MPI_Bcast(key_name, key_len, MPI_CHAR, 0, scr_comm_world);

      /* determine whether all procs have corresponding entry */
      have_match = 1;
      if (scr_hash_util_get_str(groups, key_name, &value) != SCR_SUCCESS) {
        have_match = 0;
      }
      if (scr_alltrue(have_match)) {
        /* create group */
        scr_groupdesc_create_by_str(&scr_groupdescs[index], index, key_name, value);
        index++;
      }
    }
  }

  /* determine whether everyone found a valid group descriptor */
  if (!all_valid) {
    return SCR_FAILURE;
  }
  return SCR_SUCCESS;
}

int scr_groupdescs_free()
{
  /* iterate over and free each of our group descriptors */
  if (scr_ngroupdescs > 0 && scr_groupdescs != NULL) {
    int i;
    for (i=0; i < scr_ngroupdescs; i++) {
      scr_groupdesc_free(&scr_groupdescs[i]);
    }
  }

  /* set the count back to zero */
  scr_ngroupdescs = 0;

  /* and free off the memory allocated */
  scr_free(&scr_groupdescs);

  return SCR_SUCCESS;
}
