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

/*
=========================================
Store descriptor functions
=========================================
*/

/* initialize the specified store descriptor */
int scr_storedesc_init(scr_storedesc* s)
{
  /* check that we got a valid store descriptor */
  if (s == NULL) {
    scr_err("No store descriptor @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* initialize the descriptor */
  s->enabled   =  0;
  s->index     = -1;
  s->name      = NULL;
  s->max_count = 0;
  s->can_mkdir = 0;
  s->comm      = MPI_COMM_NULL;
  s->rank      = MPI_PROC_NULL;
  s->ranks     = 0;

  return SCR_SUCCESS;
}

/* free any memory associated with the specified store descriptor */
int scr_storedesc_free(scr_storedesc* s)
{
  /* free the strings we strdup'd */
  scr_free(&s->name);

  /* free the communicator we created */
  if (s->comm != MPI_COMM_NULL) {
    MPI_Comm_free(&s->comm);
  }

  return SCR_SUCCESS;
}

/* make full copy of a store descriptor */
static int scr_storedesc_copy(scr_storedesc* out, const scr_storedesc* in)
{
  /* check that we got valid store descriptors */
  if (out == NULL || in == NULL) {
    scr_err("Invalid store descriptors @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* duplicate values from input descriptor */
  out->enabled   = in->enabled;
  out->index     = in->index;
  out->name      = strdup(in->name);
  out->max_count = in->max_count;
  out->can_mkdir = in->can_mkdir;
  MPI_Comm_dup(in->comm, &out->comm);
  out->rank      = in->rank;
  out->ranks     = in->ranks;

  return SCR_SUCCESS;
}

/* build a store descriptor corresponding to the specified hash,
 * this function is collective, because it issues MPI calls */
int scr_storedesc_create_from_hash(scr_storedesc* s, int index, const scr_hash* hash)
{
  int rc = SCR_SUCCESS;

  /* check that we got a valid descriptor */
  if (s == NULL) {
    scr_err("No store descriptor to fill from hash @ %s:%d",
      __FILE__, __LINE__
    );
    rc = SCR_FAILURE;
  }

  /* check that we got a valid pointer to a hash */
  if (hash == NULL) {
    scr_err("No hash specified to build store descriptor from @ %s:%d",
      __FILE__, __LINE__
    );
    rc = SCR_FAILURE;
  }

  /* check that everyone made it this far */
  if (! scr_alltrue(rc == SCR_SUCCESS)) {
    if (s != NULL) {
      s->enabled = 0;
    }
    return SCR_FAILURE;
  }

  /* initialize the descriptor */
  scr_storedesc_init(s);

  /* enable / disable the descriptor */
  s->enabled = 1;
  scr_hash_util_get_int(hash, SCR_CONFIG_KEY_ENABLED, &(s->enabled));

  /* index of the descriptor */
  s->index = index;

  /* set the base directory */
  char* base;
  if (scr_hash_util_get_str(hash, SCR_CONFIG_KEY_BASE, &base) == SCR_SUCCESS) {
    s->name = strdup(base);
  }

  /* set the max count, default to scr_cache_size unless specified otherwise */
  s->max_count = scr_cache_size;
  scr_hash_util_get_int(hash, SCR_CONFIG_KEY_COUNT, &(s->max_count));

  /* assume we can call mkdir/rmdir on this store unless told otherwise */
  s->can_mkdir = 1;
  scr_hash_util_get_int(hash, SCR_CONFIG_KEY_MKDIR, &(s->can_mkdir));

  /* TODO: use FGFS eventually, for now we assume node-local storage */
  /* get communicator of ranks that can access this storage device */
  char* group;
  const scr_groupdesc* groupdesc;
  if (scr_hash_util_get_str(hash, SCR_CONFIG_KEY_GROUP, &group) == SCR_SUCCESS) {
      /* lookup group descriptor for specified name */
      groupdesc = scr_groupdescs_from_name(group);
  } else {
      /* if group is not set, assume it's node-local storage */
      groupdesc = scr_groupdescs_from_name(SCR_GROUP_NODE);
  }
  if (groupdesc != NULL) {
    MPI_Comm_dup(groupdesc->comm, &s->comm);

    /* get our rank and the number of ranks in this communicator */
    MPI_Comm_rank(s->comm, &s->rank);
    MPI_Comm_size(s->comm, &s->ranks);
  } else {
    s->enabled = 0;
  }

  /* if anyone has disabled this descriptor, everyone needs to */
  if (! scr_alltrue(s->enabled)) {
    s->enabled = 0;
  }

  return SCR_SUCCESS;
}

/* create specified directory on store */
int scr_storedesc_dir_create(const scr_storedesc* store, const char* dir)
{
  /* verify that we have a valid store descriptor and directory name */
  if (store == NULL || dir == NULL) {
    return SCR_FAILURE;
  }

  /* return with failure if this store is disabled */
  if (! store->enabled) {
    return SCR_FAILURE;
  }

  /* rank 0 creates the directory */
  int rc = SCR_SUCCESS;
  if (store->rank == 0 && store->can_mkdir) {
    scr_dbg(2, "Creating directory: %s", dir);
    rc = scr_mkdir(dir, S_IRWXU | S_IRWXG);
  }

  /* broadcast return code from rank zero to other ranks */
  MPI_Bcast(&rc, 1, MPI_INT, 0, store->comm);

  return rc;
}

/* delete specified directory from store */
int scr_storedesc_dir_delete(const scr_storedesc* store, const char* dir)
{
  /* verify that we have a valid store descriptor and directory name */
  if (store == NULL || dir == NULL) {
    return SCR_FAILURE;
  }

  /* return with failure if this store is disabled */
  if (! store->enabled) {
    return SCR_FAILURE;
  }

  /* barrier to ensure all procs are ready before we delete */
  MPI_Barrier(store->comm);

  /* rank 0 deletes the directory */
  int rc = SCR_SUCCESS;
  if (store->rank == 0 && store->can_mkdir) {
    /* delete directory */
    if (scr_rmdir(dir) != SCR_SUCCESS) {
      /* whoops, something failed when we tried to delete our directory */
      rc = SCR_FAILURE;
      scr_err("Error deleting directory: %s @ %s:%d",
        dir, __FILE__, __LINE__
      );
    }
  }

  /* broadcast return code from rank zero to other ranks */
  MPI_Bcast(&rc, 1, MPI_INT, 0, store->comm);

  return rc;
}

/*
=========================================
Routines that operate on scr_storedescs array
=========================================
*/

/* lookup index in scr_storedescs given a target name,
 * returns -1 if not found */
int scr_storedescs_index_from_name(const char* name)
{
  /* assume we won't find a match */
  int index = -1;

  /* check that we're given a name */
  if (name == NULL) {
    return index;
  }

  /* search through the scr_storedescs looking for a match */
  int i;
  for (i = 0; i < scr_nstoredescs; i++) {
    if (strcmp(name, scr_storedescs[i].name) == 0) {
      /* found a match, record its index, and break */
      index = i;
      break;
    }
  }

  return index;
}

/* fill in scr_storedescs array from scr_storedescs_hash */
int scr_storedescs_create()
{
  /* set the number of store descriptors */
  scr_nstoredescs = 0;
  scr_hash* tmp = scr_hash_get(scr_storedesc_hash, SCR_CONFIG_KEY_STOREDESC);
  if (tmp != NULL) {
    scr_nstoredescs = scr_hash_size(tmp);
  }

  /* allocate our store descriptors */
  if (scr_nstoredescs > 0) {
    scr_storedescs = (scr_storedesc*) malloc(scr_nstoredescs * sizeof(scr_storedesc));
    /* TODO: check for errors */
  }

  int all_valid = 1;

  /* iterate over each of our hash entries filling in each
   * corresponding descriptor */
  int i;
  for (i=0; i < scr_nstoredescs; i++) {
    /* get the info hash for this descriptor */
    scr_hash* hash = scr_hash_get_kv_int(scr_storedesc_hash, SCR_CONFIG_KEY_STOREDESC, i);
    if (scr_storedesc_create_from_hash(&scr_storedescs[i], i, hash) != SCR_SUCCESS) {
      all_valid = 0;
    }
  }

  /* create store descriptor for control directory */
  scr_storedesc_cntl = (scr_storedesc*) malloc(sizeof(scr_storedesc));
  int index = scr_storedescs_index_from_name(scr_cntl_base);
  if (scr_storedesc_cntl != NULL && index >= 0) {
    scr_storedesc_copy(scr_storedesc_cntl, &scr_storedescs[index]);
  } else {
    scr_abort(-1, "Failed to create store descriptor for control directory @ %s:%d",
      __FILE__, __LINE__
    );
  }
  
  /* determine whether everyone found a valid store descriptor */
  if (!all_valid) {
    return SCR_FAILURE;
  }
  return SCR_SUCCESS;
}

/* free scr_storedescs array */
int scr_storedescs_free()
{
  /* iterate over and free each of our store descriptors */
  if (scr_nstoredescs > 0 && scr_storedescs != NULL) {
    int i;
    for (i=0; i < scr_nstoredescs; i++) {
      scr_storedesc_free(&scr_storedescs[i]);
    }
  }

  /* free descriptor for control directory */
  scr_storedesc_free(scr_storedesc_cntl);

  /* set the count back to zero */
  scr_nstoredescs = 0;

  /* and free off the memory allocated */
  scr_free(&scr_storedescs);

  return SCR_SUCCESS;
}
