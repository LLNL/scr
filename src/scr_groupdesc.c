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

#include "kvtree.h"
#include "kvtree_util.h"

#include "rankstr_mpi.h"

#include "scr_globals.h"

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

  return SCR_SUCCESS;
}

/* build a group descriptor of all procs having the same value */
static int scr_groupdesc_create_by_str(
  scr_groupdesc* d, int index, const char* key, const char* value, MPI_Comm comm)
{
  /* initialize the descriptor */
  scr_groupdesc_init(d);

  /* enable descriptor, record its index, and copy its name */
  d->enabled = 1;
  d->index   = index;
  d->name    = strdup(key);

  /* get communicator of all tasks with same value */
  rankstr_mpi_comm_split(comm, value, 0, 0, 1, &d->comm);

  /* find our position in the group communicator */
  MPI_Comm_rank(d->comm, &d->rank);
  MPI_Comm_size(d->comm, &d->ranks);

  return SCR_SUCCESS;
}

/*
=========================================
Routines that operate on scr_groupdescs array
=========================================
*/

/* given a group name, return its index within scr_groupdescs array,
 * returns -1 if not found */
int scr_groupdescs_index_from_name(const char* name)
{
  /* assume we won't find the target name */
  int index = -1;

  /* check that we got a name to lookup */
  if (name == NULL) {
    return index;
  }

  /* iterate through our group descriptors until we find a match */
  int i;
  for (i = 0; i < scr_ngroupdescs; i++) {
    if (scr_groupdescs[i].enabled &&
        strcmp(name, scr_groupdescs[i].name) == 0)
    {
      /* found a match, record its index and break */
      index = i;
      break;
    }
  }

  return index;
}

/* given a group name, return pointer to groupdesc struct within
 * scr_groupdescs array, returns NULL if not found */
scr_groupdesc* scr_groupdescs_from_name(const char* name)
{
  int index = scr_groupdescs_index_from_name(name);
  if (index < 0) {
    return NULL;
  }
  return &scr_groupdescs[index];
}

/* fill in scr_groupdescs array from scr_groupdescs_hash */
int scr_groupdescs_create(MPI_Comm comm)
{
  int i;
  int all_valid = 1;

  /* get our rank in comm */
  int rank;
  MPI_Comm_rank(comm, &rank);

  /* get groups defined for our hostname */
  kvtree* groups = kvtree_get_kv(
    scr_groupdesc_hash, SCR_CONFIG_KEY_GROUPDESC, scr_my_hostname
  );

  /* set the number of group descriptors,
   * we define one for all procs on the same node
   * and another for the world */
  int num_groups = kvtree_size(groups);
  int count = num_groups + 2;

  /* set our count to maximum count across all procs */
  MPI_Allreduce(
    &count, &scr_ngroupdescs, 1, MPI_INT, MPI_MAX, comm
  );

  /* allocate our group descriptors */
  scr_groupdescs = (scr_groupdesc*) SCR_MALLOC(scr_ngroupdescs * sizeof(scr_groupdesc));

  /* disable all group descriptors until we build each one */
  for (i = 0; i < scr_ngroupdescs; i++) {
    scr_groupdesc_init(&scr_groupdescs[i]);
  }

  /* create group descriptor for all procs on the same node */
  int index = 0;
  scr_groupdesc_create_by_str(
    &scr_groupdescs[index], index, SCR_GROUP_NODE, scr_my_hostname, comm
  );
  index++;

  /* create group descriptor for all procs in job */
  scr_groupdesc_create_by_str(
    &scr_groupdescs[index], index, SCR_GROUP_WORLD, "ALL", comm
  );
  index++;

  /* in order to form groups in the same order on all procs,
   * we have rank 0 decide the order */

  /* determine number of entries on rank 0 */
  MPI_Bcast(&num_groups, 1, MPI_INT, 0, comm);

  /* iterate over each of our hash entries filling in each
   * corresponding descriptor */
  char* value;
  int have_match;
  if (rank == 0) {
    kvtree_elem* elem;
    for (elem = kvtree_elem_first(groups);
         elem != NULL;
         elem = kvtree_elem_next(elem))
    {
      /* get key for this group */
      char* key = kvtree_elem_key(elem);

      /* bcast key name */
      scr_str_bcast(&key, 0, comm);

      /* determine whether all procs have corresponding entry */
      have_match = 1;
      if (kvtree_util_get_str(groups, key, &value) != KVTREE_SUCCESS) {
        have_match = 0;
      }
      if (scr_alltrue(have_match, comm)) {
        /* create group */
        scr_groupdesc_create_by_str(
          &scr_groupdescs[index], index, key, value, comm
        );
        index++;
      } else {
        /* print warning that group is not defined */
        scr_warn("Not all ranks have group %s defined @ %s:%d",
          key, __FILE__, __LINE__
        );
      }
    }
  } else {
    for (i = 0; i < num_groups; i++) {
      /* bcast key name */
      char* key;
      scr_str_bcast(&key, 0, comm);

      /* determine whether all procs have corresponding entry */
      have_match = 1;
      if (kvtree_util_get_str(groups, key, &value) != KVTREE_SUCCESS) {
        have_match = 0;
      }
      if (scr_alltrue(have_match, comm)) {
        /* create group */
        scr_groupdesc_create_by_str(
          &scr_groupdescs[index], index, key, value, comm
        );
        index++;
      }

      /* free the key name */
      scr_free(&key);
    }
  }

  /* determine whether everyone found a valid group descriptor */
  if (! all_valid) {
    return SCR_FAILURE;
  }
  return SCR_SUCCESS;
}

/* free scr_groupdescs array */
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
