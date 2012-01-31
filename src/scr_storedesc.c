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
  s->max_count = 0;
  s->base      = NULL;
  s->comm      = MPI_COMM_NULL;
  s->rank      = MPI_PROC_NULL;
  s->ranks     = 0;

  return SCR_SUCCESS;
}

/* free any memory associated with the specified store descriptor */
int scr_storedesc_free(scr_storedesc* s)
{
  /* free the strings we strdup'd */
  scr_free(&s->base);

  /* free the communicator we created */
  if (s->comm != MPI_COMM_NULL) {
    MPI_Comm_free(&s->comm);
  }

  return SCR_SUCCESS;
}

int scr_storedesc_copy(scr_storedesc* out, const scr_storedesc* in)
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
  out->max_count = in->max_count;
  out->base      = strdup(in->base);
  MPI_Comm_dup(in->comm, &out->comm);
  out->rank      = in->rank;
  out->ranks     = in->ranks;

  return SCR_SUCCESS;
}

/* build a redundancy descriptor corresponding to the specified hash,
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

  char* value = NULL;

  /* enable / disable the descriptor */
  s->enabled = 1;
  value = scr_hash_elem_get_first_val(hash, SCR_CONFIG_KEY_ENABLED);
  if (value != NULL) {
    s->enabled = atoi(value);
  }

  /* index of the descriptor */
  s->index = index;
  value = scr_hash_elem_get_first_val(hash, SCR_CONFIG_KEY_INDEX);
  if (value != NULL) {
    s->index = atoi(value);
  }

  /* set the max count, default to scr_cache_size unless specified otherwise */
  s->max_count = scr_cache_size;
  value = scr_hash_elem_get_first_val(hash, SCR_CONFIG_KEY_COUNT);
  if (value != NULL) {
    s->max_count = atoi(value);
  }

  /* set the base directory */
  value = scr_hash_elem_get_first_val(hash, SCR_CONFIG_KEY_BASE);
  if (value != NULL) {
    s->base = strdup(value);
  }

  /* TODO: use FGFS eventually, for now we assume node-local storage */
  /* get communicator of ranks that can access this storage device */
  const scr_groupdesc* groupdesc = scr_groupdescs_from_name(SCR_GROUP_NODE);
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

/*
=========================================
Routines that operate on scr_storedescs array
=========================================
*/

int scr_storedescs_index_from_base(const char* base)
{
  int index = -1;

  if (base == NULL) {
    return index;
  }

  int i;
  for (i = 0; i < scr_nstoredescs; i++) {
    if (strcmp(base, scr_storedescs[i].base) == 0) {
      index = i;
      break;
    }
  }

  return index;
}

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

  /* iterate over each of our hash entries filling in each corresponding descriptor */
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
  int index = scr_storedescs_index_from_base(scr_cntl_base);
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

int scr_storedescs_free()
{
  /* iterate over and free each of our store descriptors */
  if (scr_nstoredescs > 0 && scr_storedescs != NULL) {
    int i;
    for (i=0; i < scr_nstoredescs; i++) {
      scr_storedesc_free(&scr_storedescs[i]);
    }
  }
  scr_storedesc_free(scr_storedesc_cntl);

  /* set the count back to zero */
  scr_nstoredescs = 0;

  /* and free off the memory allocated */
  scr_free(&scr_storedescs);

  return SCR_SUCCESS;
}
