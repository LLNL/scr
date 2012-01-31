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

/* build a group descriptor of all procs on the same node */
int scr_groupdesc_create_node(scr_groupdesc* d, int index)
{
  d->enabled = 1;
  d->index   = index;
  d->name    = strdup(SCR_GROUP_NODE);

#ifdef HAVE_LIBGCS
  /* determine the length of the maximum hostname (including terminating NULL character),
   * and check that our own buffer is at least as big */
  int my_hostname_len = strlen(scr_my_hostname) + 1;
  int max_hostname_len = 0;
  MPI_Allreduce(&my_hostname_len, &max_hostname_len, 1, MPI_INT, MPI_MAX, scr_comm_world);
  if (max_hostname_len > sizeof(scr_my_hostname)) {
    scr_err("Hostname is too long on some process @ %s:%d",
            __FILE__, __LINE__
    );
    MPI_Abort(scr_comm_world, 0);
  }

  /* split ranks based on hostname */
  GCS_Comm_splitv(
      scr_comm_world,
      scr_my_hostname, max_hostname_len, GCS_CMP_STR,
      NULL,            0,                GCS_CMP_IGNORE,
      &d->comm
  );
#else /* HAVE_LIBGCS */
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
#endif /* HAVE_LIBGCS */

  /* find our position in the local communicator */
  MPI_Comm_rank(d->comm, &d->rank);
  MPI_Comm_size(d->comm, &d->ranks);

  /* Based on my group rank, create communicators consisting of all tasks at same group rank level */
  MPI_Comm_split(scr_comm_world, d->rank, scr_my_rank_world, &d->comm_across);

  /* find our position in the communicator */
  MPI_Comm_rank(d->comm_across, &d->rank_across);
  MPI_Comm_size(d->comm_across, &d->ranks_across);

  return SCR_SUCCESS;
}

/* build a group descriptor corresponding to the specified hash */
int scr_groupdesc_create_from_hash(scr_groupdesc* d, int index, const scr_hash* hash)
{
  int rc = SCR_SUCCESS;

  /* check that we got a valid descriptor */
  if (d == NULL) {
    scr_err("No group descriptor to fill from hash @ %s:%d",
            __FILE__, __LINE__
    );
    rc = SCR_FAILURE;
  }

  /* check that we got a valid pointer to a hash */
  if (hash == NULL) {
    scr_err("No hash specified to build group descriptor from @ %s:%d",
            __FILE__, __LINE__
    );
    rc = SCR_FAILURE;
  }

  /* check that everyone made it this far */
  if (! scr_alltrue(rc == SCR_SUCCESS)) {
    if (d != NULL) {
      d->enabled = 0;
    }
    return SCR_FAILURE;
  }

  /* initialize the descriptor */
  scr_groupdesc_init(d);

  char* value = NULL;

  /* enable / disable the descriptor */
  d->enabled = 1;
  value = scr_hash_elem_get_first_val(hash, SCR_CONFIG_KEY_ENABLED);
  if (value != NULL) {
    d->enabled = atoi(value);
  }

  /* index of the descriptor */
  d->index = index;

  /* set the name */
  value = scr_hash_elem_get_first_val(hash, SCR_CONFIG_KEY_NAME);
  if (value != NULL) {
    d->name = strdup(value);
  }

  /* TODO: execute generalized comm split based on group name */
  /* get communicator of ranks that can access this storage device */
  MPI_Comm_dup(scr_comm_world, &d->comm);

  /* get our rank and the number of ranks in this communicator */
  MPI_Comm_rank(d->comm, &d->rank);
  MPI_Comm_size(d->comm, &d->ranks);

  /* if anyone has disabled this descriptor, everyone needs to */
  if (! scr_alltrue(d->enabled)) {
    d->enabled = 0;
  }

  return SCR_SUCCESS;
}

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
  /* set the number of group descriptors */
  scr_ngroupdescs = 1;
  scr_hash* tmp = scr_hash_get(scr_groupdesc_hash, SCR_CONFIG_KEY_GROUPDESC);
  if (tmp != NULL) {
    /* include anything the user defines plus node */
    scr_ngroupdescs += scr_hash_size(tmp);
  }

  /* allocate our group descriptors */
  if (scr_ngroupdescs > 0) {
    scr_groupdescs = (scr_groupdesc*) malloc(scr_ngroupdescs * sizeof(scr_groupdesc));
    /* TODO: check for errors */
  }

  int all_valid = 1;

  /* create group descriptor for all procs on the same node */
  scr_groupdesc_create_node(&scr_groupdescs[0], 0);

  /* iterate over each of our hash entries filling in each corresponding descriptor */
  int i;
  for (i=1; i < scr_ngroupdescs; i++) {
    /* get the info hash for this descriptor */
    scr_hash* hash = scr_hash_get_kv_int(scr_groupdesc_hash, SCR_CONFIG_KEY_GROUPDESC, i);
    if (scr_groupdesc_create_from_hash(&scr_groupdescs[i], i, hash) != SCR_SUCCESS) {
      all_valid = 0;
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
