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
#include "spath.h"

#include "scr_globals.h"

/*
=========================================
Store descriptor functions
=========================================
*/

/* initialize the specified store descriptor */
static int scr_storedesc_init(scr_storedesc* s)
{
  /* check that we got a valid store descriptor */
  if (s == NULL) {
    scr_err("No store descriptor @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* initialize the descriptor */
  s->enabled   = 0;
  s->index     = -1;
  s->name      = NULL;
  s->max_count = 0;
  s->can_mkdir = 0;
  s->xfer      = NULL;
  s->view      = NULL;
  s->comm      = MPI_COMM_NULL;
  s->rank      = MPI_PROC_NULL;
  s->ranks     = 0;

  return SCR_SUCCESS;
}

/* free any memory associated with the specified store descriptor */
static int scr_storedesc_free(scr_storedesc* s)
{
  if (s != NULL) {
    /* free the strings we strdup'd */
    scr_free(&s->name);
    scr_free(&s->xfer);
    scr_free(&s->view);

    /* free the communicator we created */
    if (s->comm != MPI_COMM_NULL) {
      MPI_Comm_free(&s->comm);
    }

    /* reinitialize fields */
    scr_storedesc_init(s);
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
  out->xfer      = strdup(in->xfer);
  out->view      = strdup(in->view);
  MPI_Comm_dup(in->comm, &out->comm);
  out->rank      = in->rank;
  out->ranks     = in->ranks;

  return SCR_SUCCESS;
}

/* build a store descriptor corresponding to the specified hash,
 * this function is collective, because it issues MPI calls */
static int scr_storedesc_create_from_hash(
  scr_storedesc* s,
  const char* name,
  int index,
  const kvtree* hash,
  MPI_Comm comm)
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
  if (! scr_alltrue(rc == SCR_SUCCESS, comm)) {
    if (s != NULL) {
      s->enabled = 0;
    }
    return SCR_FAILURE;
  }

  /* initialize the descriptor */
  scr_storedesc_init(s);

  /* enable / disable the descriptor */
  s->enabled = 1;
  kvtree_util_get_int(hash, SCR_CONFIG_KEY_ENABLED, &(s->enabled));

  /* index of the descriptor */
  s->index = index;

  /* TODO: check that path is absolute */
  /* set the base directory, reduce path in the process */
  s->name = spath_strdup_reduce_str(name);

  /* set the max count, default to scr_cache_size unless specified otherwise */
  s->max_count = scr_cache_size;
  kvtree_util_get_int(hash, SCR_CONFIG_KEY_COUNT, &(s->max_count));

  /* assume we can call mkdir/rmdir on this store unless told otherwise */
  s->can_mkdir = 1;
  kvtree_util_get_int(hash, SCR_CONFIG_KEY_MKDIR, &(s->can_mkdir));

  /* set the type of the store which selects transfer mode */
  char* flush_type = scr_flush_type;
  kvtree_util_get_str(hash, SCR_CONFIG_KEY_FLUSH, &flush_type);
  s->xfer = strdup(flush_type);

  /* set the view of the store. Default to PRIVATE */
  /* strdup the view if one exists */
  char* tmp_view = NULL;
  kvtree_util_get_str(hash, SCR_CONFIG_KEY_VIEW, &tmp_view);
  if (tmp_view != NULL) {
    s->view = strdup(tmp_view);
  } else {
    s->view = strdup("PRIVATE");
  }

  /* get communicator of ranks that can access this storage device,
   * assume node-local storage unless told otherwise  */
  char* group = SCR_GROUP_NODE;
  kvtree_util_get_str(hash, SCR_CONFIG_KEY_GROUP, &group);

  /* lookup group descriptor for specified name */
  const scr_groupdesc* groupdesc = scr_groupdescs_from_name(group);
  if (groupdesc != NULL) {
    MPI_Comm_dup(groupdesc->comm, &s->comm);

    /* get our rank and the number of ranks in this communicator */
    MPI_Comm_rank(s->comm, &s->rank);
    MPI_Comm_size(s->comm, &s->ranks);
  } else {
    s->enabled = 0;
  }

  /* if anyone has disabled this descriptor, everyone needs to */
  if (! scr_alltrue(s->enabled, comm)) {
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
  if (!strcmp(store->view, "GLOBAL") && store->can_mkdir && scr_my_rank_host == 0) {
    scr_dbg(2, "Creating directory: %s", dir);
    rc = scr_mkdir(dir, S_IRWXU | S_IRWXG);
  } else if (store->rank == 0 && store->can_mkdir) {
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
  if ((store->rank == 0 || (scr_my_rank_host == 0 && !strcmp(store->view, "GLOBAL")))
      && store->can_mkdir)
  {
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
    if (scr_storedescs[i].enabled &&
        strcmp(name, scr_storedescs[i].name) == 0)
    {
      /* found a match, record its index, and break */
      index = i;
      break;
    }
  }

  return index;
}

/* lookup index in scr_storedescs given a child path
 * within the space of that descriptor,
 * returns -1 if not found */
int scr_storedescs_index_from_child_path(const char* path)
{
  /* assume we won't find a match */
  int index = -1;

  /* check that we're given a path */
  if (path == NULL) {
    return index;
  }

  /* search through the scr_storedescs looking for a match,
   * match on the longest path we can find, in case we have
   * a situation like "/dev/shm", and "/dev/shm/dir1" */
  int i;
  int maxlen = 0;
  for (i = 0; i < scr_nstoredescs; i++) {
    /* get length of path for this descriptor */
    int pathlen = strlen(scr_storedescs[i].name);

    /* see if prefix of path matches path for this descriptor */
    if (scr_storedescs[i].enabled &&
        strncmp(path, scr_storedescs[i].name, pathlen) == 0)
    {
      /* found a match, record its index,
       * keep searching in case we find a longer (better) match */
      if (maxlen < pathlen) {
        index = i;
        maxlen = pathlen;
      }
    }
  }

  return index;
}

/* fill in scr_storedescs array from scr_storedescs_hash */
int scr_storedescs_create(MPI_Comm comm)
{
  /* set the number of store descriptors */
  scr_nstoredescs = 0;
  kvtree* tmp = kvtree_get(scr_storedesc_hash, SCR_CONFIG_KEY_STOREDESC);
  if (tmp != NULL) {
    scr_nstoredescs = kvtree_size(tmp);
  }

  /* allocate our store descriptors */
  scr_storedescs = (scr_storedesc*) SCR_MALLOC(scr_nstoredescs * sizeof(scr_storedesc));

  /* flag to indicate whether we successfully build all store
   * descriptors */
  int all_valid = 1;

  /* sort the hash to ensure we step through all elements in the same
   * order on all procs */
  kvtree_sort(tmp, KVTREE_SORT_ASCENDING);

  /* iterate over each of our hash entries filling in each
   * corresponding descriptor */
  int index = 0;
  kvtree_elem* elem;
  for (elem = kvtree_elem_first(tmp);
       elem != NULL;
       elem = kvtree_elem_next(elem))
  {
    /* get name of store descriptor for this step */
    char* name = kvtree_elem_key(elem);

    /* get the hash for descriptor of specified name */
    kvtree* hash = kvtree_get(tmp, name);

    if (scr_storedesc_create_from_hash(&scr_storedescs[index], name, index, hash, comm) != SCR_SUCCESS) {
      all_valid = 0;
    }

    /* increment our index for the next descriptor */
    index++;
  }

  /* create store descriptor for control directory */
  scr_storedesc_cntl = (scr_storedesc*) SCR_MALLOC(sizeof(scr_storedesc));
  index = scr_storedescs_index_from_name(scr_cntl_base);
  if (scr_storedesc_cntl != NULL && index >= 0) {
    scr_storedesc_copy(scr_storedesc_cntl, &scr_storedescs[index]);
  } else {
    scr_abort(-1, "Failed to create store descriptor for control directory [%s] @ %s:%d",
      scr_cntl_base, __FILE__, __LINE__
    );
  }

  /* determine whether everyone found a valid store descriptor */
  if (! all_valid) {
    return SCR_FAILURE;
  }
  return SCR_SUCCESS;
}

/* free scr_storedescs array */
int scr_storedescs_free()
{
  /* free descriptor for control directory */
  scr_storedesc_free(scr_storedesc_cntl);
  scr_free(&scr_storedesc_cntl);

  /* iterate over and free each of our store descriptors */
  if (scr_nstoredescs > 0 && scr_storedescs != NULL) {
    int i;
    for (i = 0; i < scr_nstoredescs; i++) {
      scr_storedesc_free(&scr_storedescs[i]);
    }
  }

  /* set the count back to zero */
  scr_nstoredescs = 0;

  /* and free off the memory allocated */
  scr_free(&scr_storedescs);

  return SCR_SUCCESS;
}
