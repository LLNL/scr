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
Redundancy descriptor functions
=========================================
*/

/* initialize the specified redundancy descriptor */
int scr_reddesc_init(scr_reddesc* d)
{
  /* check that we got a valid redundancy descriptor */
  if (d == NULL) {
    scr_err("No redundancy descriptor to fill from hash @ %s:%d",
      __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* initialize the descriptor */
  d->enabled        =  0;
  d->index          = -1;
  d->interval       = -1;
  d->store_index    = -1;
  d->group_index    = -1;
  d->base           = NULL;
  d->directory      = NULL;
  d->copy_type      = SCR_COPY_NULL;
  d->copy_state     = NULL;
  d->comm           = MPI_COMM_NULL;
  d->groups         =  0;
  d->group_id       = -1;
  d->ranks          =  0;
  d->my_rank        = MPI_PROC_NULL;

  return SCR_SUCCESS;
}

/* given a redundancy descriptor with all top level fields filled in
 * allocate and fill in structure for partner specific fields in copy_state */
static int scr_reddesc_create_partner(scr_reddesc* d)
{
  int rc = SCR_SUCCESS;

  /* allocate a new structure to hold partner state */
  scr_reddesc_partner* state = (scr_reddesc_partner*) malloc(sizeof(scr_reddesc_partner));
  /* TODO: check for errors */

  /* attach structure to reddesc */
  d->copy_state = (void*) state;

  /* record group rank, world rank, and hostname of left and right partners */
  scr_set_partners(
    d->comm, 1,
    &state->lhs_rank, &state->lhs_rank_world, &state->lhs_hostname,
    &state->rhs_rank, &state->rhs_rank_world, &state->rhs_hostname
  );

  /* check that we got valid partners */
  if (state->lhs_hostname == NULL ||
      state->rhs_hostname == NULL ||
      strcmp(state->lhs_hostname, "") == 0 ||
      strcmp(state->rhs_hostname, "") == 0 ||
      strcmp(state->lhs_hostname, scr_my_hostname) == 0 ||
      strcmp(state->rhs_hostname, scr_my_hostname) == 0)
  {
    /* disable this descriptor */
    d->enabled = 0;
    scr_warn("Failed to find partner processes for redundancy descriptor %d, disabling checkpoint, too few nodes? @ %s:%d",
      d->index, __FILE__, __LINE__
    );
    rc = SCR_FAILURE;
  } else {
    scr_dbg(2, "LHS partner: %s (%d)  -->  My name: %s (%d)  -->  RHS partner: %s (%d)",
      state->lhs_hostname, state->lhs_rank_world,
      scr_my_hostname, scr_my_rank_world,
      state->rhs_hostname, state->rhs_rank_world
    );
  }

  return rc;
}

/* given a redundancy descriptor with all top level fields filled in
 * allocate and fill in structure for xor specific fields in copy_state */
static int scr_reddesc_create_xor(scr_reddesc* d)
{
  int rc = SCR_SUCCESS;

  /* allocate a new structure to hold XOR state */
  scr_reddesc_xor* state = (scr_reddesc_xor*) malloc(sizeof(scr_reddesc_xor));
  /* TODO: check for errors */

  /* attach structure to reddesc */
  d->copy_state = (void*) state;

  /* allocate a new hash to store group mapping info */
  scr_hash* header = scr_hash_new();

  /* record the total number of ranks in scr_comm_world */
  int ranks_world;
  MPI_Comm_size(scr_comm_world, &ranks_world);
  scr_hash_set_kv_int(header, SCR_KEY_COPY_XOR_RANKS, ranks_world);

  /* create a new empty hash to track group info for this xor set */
  scr_hash* hash = scr_hash_new();
  scr_hash_set(header, SCR_KEY_COPY_XOR_GROUP, hash);

  /* record the total number of ranks in the xor communicator */
  int ranks_comm;
  MPI_Comm_size(d->comm, &ranks_comm);
  scr_hash_set_kv_int(hash, SCR_KEY_COPY_XOR_GROUP_RANKS, ranks_comm);

  /* record mapping of rank in xor group to corresponding world rank */
  if (ranks_comm > 0) {
    /* allocate array to receive rank from each process */
    int* ranklist = (int*) malloc(ranks_comm * sizeof(int));
    /* TODO: check for errors */

    /* gather rank values */
    MPI_Allgather(&scr_my_rank_world, 1, MPI_INT, ranklist, 1, MPI_INT, d->comm);

    /* map ranks in comm to ranks in scr_comm_world */
    int i;
    for (i=0; i < ranks_comm; i++) {
      int rank = ranklist[i];
      scr_hash_setf(hash, NULL, "%s %d %d", SCR_KEY_COPY_XOR_GROUP_RANK, i, rank);
    }

    /* free the temporary array */
    scr_free(&ranklist);
  }

  /* record group mapping info in descriptor */
  state->group_map = header; 

  /* record group rank, world rank, and hostname of left and right partners */
  scr_set_partners(
    d->comm, 1,
    &state->lhs_rank, &state->lhs_rank_world, &state->lhs_hostname,
    &state->rhs_rank, &state->rhs_rank_world, &state->rhs_hostname
  );

  /* check that we got valid partners */
  if (state->lhs_hostname == NULL ||
      state->rhs_hostname == NULL ||
      strcmp(state->lhs_hostname, "") == 0 ||
      strcmp(state->rhs_hostname, "") == 0 ||
      strcmp(state->lhs_hostname, scr_my_hostname) == 0 ||
      strcmp(state->rhs_hostname, scr_my_hostname) == 0)
  {
    /* disable this descriptor */
    d->enabled = 0;
    scr_warn("Failed to find partner processes for redundancy descriptor %d, disabling checkpoint, too few nodes? @ %s:%d",
      d->index, __FILE__, __LINE__
    );
    rc = SCR_FAILURE;
  } else {
    scr_dbg(2, "LHS partner: %s (%d)  -->  My name: %s (%d)  -->  RHS partner: %s (%d)",
      state->lhs_hostname, state->lhs_rank_world,
      scr_my_hostname, scr_my_rank_world,
      state->rhs_hostname, state->rhs_rank_world
    );
  }

  return rc;
}

static int scr_reddesc_free_partner(scr_reddesc* d)
{
  scr_reddesc_partner* state = (scr_reddesc_partner*) d->copy_state;
  if (state != NULL) {
    /* free strings that we received */
    scr_free(&state->lhs_hostname);
    scr_free(&state->rhs_hostname);

    /* free the structure */
    scr_free(&d->copy_state);
  }
  return SCR_SUCCESS;
}

static int scr_reddesc_free_xor(scr_reddesc* d)
{
  scr_reddesc_xor* state = (scr_reddesc_xor*) d->copy_state;
  if (state != NULL) {
    /* free the hash mapping group ranks to world ranks */
    scr_hash_delete(&state->group_map);

    /* free strings that we received */
    scr_free(&state->lhs_hostname);
    scr_free(&state->rhs_hostname);

    /* free the structure */
    scr_free(&d->copy_state);
  }
  return SCR_SUCCESS;
}

/* free any memory associated with the specified redundancy
 * descriptor */
int scr_reddesc_free(scr_reddesc* d)
{
  /* free off copy type specific data */
  switch (d->copy_type) {
  case SCR_COPY_SINGLE:
    break;
  case SCR_COPY_PARTNER:
    scr_reddesc_free_partner(d);
    break;
  case SCR_COPY_XOR:
    scr_reddesc_free_xor(d);
    break;
  }

  /* free the strings we strdup'd */
  scr_free(&d->base);
  scr_free(&d->directory);

  /* free the communicator we created */
  if (d->comm != MPI_COMM_NULL) {
    MPI_Comm_free(&d->comm);
  }

  return SCR_SUCCESS;
}

/* given a checkpoint id and a list of redundancy descriptors,
 * select and return a pointer to a descriptor for the specified id */
scr_reddesc* scr_reddesc_for_checkpoint(
  int id,
  int ndescs,
  scr_reddesc* descs)
{
  scr_reddesc* d = NULL;

  /* pick the redundancy descriptor that is:
   *   1) enabled
   *   2) has the highest interval that evenly divides id */
  int i;
  int interval = 0;
  for (i=0; i < ndescs; i++) {
    if (descs[i].enabled &&
        interval < descs[i].interval &&
        id % descs[i].interval == 0)
    {
      d = &descs[i];
      interval = descs[i].interval;
    }
  }

  return d;
}

/* convert the specified redundancy descritpor into a corresponding
 * hash */
int scr_reddesc_store_to_hash(const scr_reddesc* d, scr_hash* hash)
{
  /* check that we got a valid pointer to a redundancy descriptor and
   * a hash */
  if (d == NULL || hash == NULL) {
    return SCR_FAILURE;
  }

  /* clear the hash */
  scr_hash_unset_all(hash);

  /* set the ENABLED key */
  scr_hash_set_kv_int(hash, SCR_CONFIG_KEY_ENABLED, d->enabled);

  /* we don't set the INDEX because this is dependent on runtime
   * environment */

  /* set the INTERVAL key */
  scr_hash_set_kv_int(hash, SCR_CONFIG_KEY_INTERVAL, d->interval);

  /* we don't set STORE_INDEX because this is dependent on runtime
   * environment */

  /* we don't set GROUP_INDEX because this is dependent on runtime
   * environment */

  /* set the BASE key */
  if (d->base != NULL) {
    scr_hash_set_kv(hash, SCR_CONFIG_KEY_BASE, d->base);
  }

  /* set the DIRECTORY key */
  if (d->directory != NULL) {
    scr_hash_set_kv(hash, SCR_CONFIG_KEY_DIRECTORY, d->directory);
  }

  /* set the TYPE key */
  switch (d->copy_type) {
  case SCR_COPY_SINGLE:
    scr_hash_set_kv(hash, SCR_CONFIG_KEY_TYPE, "SINGLE");
    break;
  case SCR_COPY_PARTNER:
    scr_hash_set_kv(hash, SCR_CONFIG_KEY_TYPE, "PARTNER");
    break;
  case SCR_COPY_XOR:
    scr_hash_set_kv(hash, SCR_CONFIG_KEY_TYPE, "XOR");
    break;
  }

  /* we don't set the LHS or RHS values because they are dependent on
   * runtime environment */

  /* we don't set the COMM because this is dependent on runtime
   * environment */

  /* set the GROUP_ID and GROUP_RANK keys, we use this info to rebuild
   * our communicator later */
  scr_hash_set_kv_int(hash, SCR_CONFIG_KEY_GROUPS,     d->groups);
  scr_hash_set_kv_int(hash, SCR_CONFIG_KEY_GROUP_ID,   d->group_id);
  scr_hash_set_kv_int(hash, SCR_CONFIG_KEY_GROUP_SIZE, d->ranks);
  scr_hash_set_kv_int(hash, SCR_CONFIG_KEY_GROUP_RANK, d->my_rank);

  return SCR_SUCCESS;
}

/* given our rank within a set of ranks and minimum group size,
 * divide the set as evenly as possible and return the
 * group id corresponding to our rank */
static int scr_reddesc_group_id(
  int rank,
  int ranks,
  int minsize,
  int* group_id)
{
  /* compute maximum number of full minsize groups we can fit within
   * ranks */
  int groups = ranks / minsize;

  /* compute number of ranks left over */
  int remainder_ranks = ranks - groups * minsize;

  /* determine base size for each group */
  int size = ranks;
  if (groups > 0) {
    /* evenly distribute remaining ranks over the groups that we have */
    int add_to_each_group = remainder_ranks / groups;
    size = minsize + add_to_each_group;
  }

  /* compute remaining ranks assuming we have groups of the new base
   * size */
  int remainder = ranks % size;

  /* for each remainder rank, we increase the lower groups by a size
   * of one, so that we end up with remainder groups of size+1 followed
   * by (groups - remainder) of size */

  /* cutoff is the first rank for which all groups are exactly size */
  int cutoff = remainder * (size + 1);

  if (rank < cutoff) {
    /* ranks below cutoff are grouped into sets of size+1 */
    *group_id = rank / (size + 1);
  } else {
    /* ranks at cutoff and higher are grouped into sets of size */
    *group_id = (rank - cutoff) / size + remainder;
  }

  return SCR_SUCCESS;
}

/* given a parent communicator and a communicator representing our group
 * within the parent, split parent into other communicators consisting
 * of all procs with same rank within its group */
static int scr_reddesc_split_across(
  MPI_Comm comm_parent,
  MPI_Comm comm_group,
  MPI_Comm* comm_across)
{
  /* TODO: this works well if each comm has about the same number of
   * procs, but we need something better to handle unbalanced groups */

  /* get rank of this process within parent communicator */
  int rank_parent;
  MPI_Comm_rank(comm_parent, &rank_parent);

  /* get rank of this process within group communicator */
  int rank_group;
  MPI_Comm_rank(comm_group, &rank_group);

  /* Split procs in comm into groups containing all procs with same
   * rank within group */
  MPI_Comm_split(comm_parent, rank_group, rank_parent, comm_across);
  
  return SCR_SUCCESS;
}

/* convert copy type string to integer value */
static int scr_reddesc_type_int_from_str(const char* value, int* type)
{
  int rc = SCR_SUCCESS;

  int copy_type;
  if (strcasecmp(value, "SINGLE") == 0) {
    copy_type = SCR_COPY_SINGLE;
  } else if (strcasecmp(value, "PARTNER") == 0) {
    copy_type = SCR_COPY_PARTNER;
  } else if (strcasecmp(value, "XOR") == 0) {
    copy_type = SCR_COPY_XOR;
  } else {
    if (scr_my_rank_world == 0) {
      scr_warn("Unknown copy type %s @ %s:%d",
        value, __FILE__, __LINE__
      );
    }
    rc = SCR_FAILURE;
  }

  *type = copy_type;
  return rc;
}

/* build a redundancy descriptor corresponding to the specified hash,
 * this function is collective */
int scr_reddesc_create_from_hash(
  scr_reddesc* d,
  int index,
  const scr_hash* hash)
{
  int rc = SCR_SUCCESS;

  /* check that we got a valid redundancy descriptor */
  if (d == NULL) {
    scr_err("No redundancy descriptor to fill from hash @ %s:%d",
      __FILE__, __LINE__
    );
    rc = SCR_FAILURE;
  }

  /* check that we got a valid pointer to a hash */
  if (hash == NULL) {
    scr_err("No hash specified to build redundancy descriptor from @ %s:%d",
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
  scr_reddesc_init(d);

  /* enable / disable the descriptor */
  d->enabled = 1;
  scr_hash_util_get_int(hash, SCR_CONFIG_KEY_ENABLED, &(d->enabled));

  /* index of the descriptor */
  d->index = index;

  /* set the interval, default to 1 unless specified otherwise */
  d->interval = 1;
  scr_hash_util_get_int(hash, SCR_CONFIG_KEY_INTERVAL, &(d->interval));

  /* set the base directory */
  char* base;
  if (scr_hash_util_get_str(hash, SCR_CONFIG_KEY_BASE, &base) == SCR_SUCCESS) {
    d->base = strdup(base);

    /* set the index to the store descriptor for this base directory */
    int store_index = scr_storedescs_index_from_name(d->base);
    if (store_index >= 0) {
      d->store_index = store_index;
    } else {
      /* couldn't find requested store, disable this descriptor and
       * warn user */
      d->enabled = 0;
      scr_warn("Failed to find store descriptor named %s @ %s:%d",
        d->base, __FILE__, __LINE__
      );
    }

//    d->group_index = scr_groupdescs_index_from_base(d->group);
  }

  /* build the directory name */
  scr_path* dir = scr_path_from_str(d->base);
  scr_path_append_str(dir, scr_username);
  scr_path_append_strf(dir, "scr.%s", scr_jobid);
  scr_path_append_strf(dir, "index.%d", d->index);
  scr_path_reduce(dir);
  d->directory = scr_path_strdup(dir);
  scr_path_delete(&dir);
    
  /* set the xor set size */
  int set_size = scr_set_size;
  scr_hash_util_get_int(hash, SCR_CONFIG_KEY_SET_SIZE, &set_size);

  /* read the checkpoint type from the hash,
   * and build our checkpoint communicator */
  char* type;
  if (scr_hash_util_get_str(hash, SCR_CONFIG_KEY_TYPE, &type) == SCR_SUCCESS) {
    if (scr_reddesc_type_int_from_str(type, &d->copy_type) != SCR_SUCCESS)
    {
      /* don't recognize copy type, disable this descriptor */
      d->enabled = 0;
      if (scr_my_rank_world == 0) {
        scr_warn("Unknown copy type %s in redundancy descriptor %d, disabling checkpoint @ %s:%d",
          type, d->index, __FILE__, __LINE__
        );
      }
    }

    const scr_groupdesc* groupdesc;

    /* CONVENIENCE: if all ranks are on the same node, change checkpoint
     * type to SINGLE, we do this so single-node jobs can run without
     * requiring the user to change the copy type */
    groupdesc = scr_groupdescs_from_name(SCR_GROUP_NODE);
    if (groupdesc != NULL && groupdesc->ranks == scr_ranks_world) {
      if (scr_my_rank_world == 0) {
        if (d->copy_type != SCR_COPY_SINGLE) {
          /* print a warning if we changed things on the user */
          scr_warn("Forcing copy type to SINGLE in redundancy descriptor %d @ %s:%d",
            d->index, __FILE__, __LINE__
          );
        }
      }
      d->copy_type = SCR_COPY_SINGLE;
    }

    /* build the communicator based on the copy type
     * and other parameters */
    int rank_across, ranks_across, split_id;
    switch (d->copy_type) {
    case SCR_COPY_SINGLE:
      /* not going to communicate with anyone, so just dup COMM_SELF */
      MPI_Comm_dup(MPI_COMM_SELF, &d->comm);
      break;
    case SCR_COPY_PARTNER:
      /* dup the communicator across nodes */
      groupdesc = scr_groupdescs_from_name(SCR_GROUP_NODE);
      if (groupdesc != NULL) {
        scr_reddesc_split_across(
          scr_comm_world, groupdesc->comm, &d->comm
        );
      } else {
        /* TODO: we could fall back to SINGLE here instead */
        scr_abort(-1, "Failed to get communicator across failure groups @ %s:%d",
          __FILE__, __LINE__
        );
      }
      break;
    case SCR_COPY_XOR:
      /* split the communicator across nodes based on xor set size
       * to create our xor communicator */
      groupdesc = scr_groupdescs_from_name(SCR_GROUP_NODE);
      if (groupdesc != NULL) {
        /* split comm world across groups */
        MPI_Comm comm_across;
        scr_reddesc_split_across(
          scr_comm_world, groupdesc->comm, &comm_across
        );

        /* get our rank and the number of ranks in this communicator */
        MPI_Comm_rank(comm_across, &rank_across);
        MPI_Comm_size(comm_across, &ranks_across);

        /* identify which group we're in */
        scr_reddesc_group_id(
          rank_across, ranks_across, set_size, &split_id
        );

        /* split communicator into groups */
        MPI_Comm_split(
          comm_across, split_id, scr_my_rank_world, &d->comm
        );

        /* free the temporary communicator */
        MPI_Comm_free(&comm_across);
      } else {
        /* TODO: we could fall back to SINGLE here instead */
        scr_abort(-1, "Failed to get communicator across failure groups @ %s:%d",
          __FILE__, __LINE__
        );
      }
      break;
    }

    /* find our position in the checkpoint communicator */
    MPI_Comm_rank(d->comm, &d->my_rank);
    MPI_Comm_size(d->comm, &d->ranks);

    /* for our group id, use the global rank of the rank 0 task
     * in our checkpoint comm */
    d->group_id = scr_my_rank_world;
    MPI_Bcast(&d->group_id, 1, MPI_INT, 0, d->comm);

    /* count the number of groups */
    int group_master = (d->my_rank == 0) ? 1 : 0;
    MPI_Allreduce(
      &group_master, &d->groups, 1, MPI_INT, MPI_SUM, scr_comm_world
    );

    /* fill in state struct depending on copy type */
    switch (d->copy_type) {
    case SCR_COPY_SINGLE:
      break;
    case SCR_COPY_PARTNER:
      scr_reddesc_create_partner(d);
      break;
    case SCR_COPY_XOR:
      scr_reddesc_create_xor(d);
      break;
    }

    /* if anyone has disabled this checkpoint, everyone needs to */
    if (! scr_alltrue(d->enabled)) {
      d->enabled = 0;
    }
  }

  return SCR_SUCCESS;
}

/* build a redundancy descriptor corresponding to the specified hash,
 * this function is collective, it differs from create_from_hash in
 * that it uses group id and group rank values to restore a descriptor
 * that was previously created */
int scr_reddesc_restore_from_hash(
  scr_reddesc* d,
  const scr_hash* hash)
{
  int rc = SCR_SUCCESS;

  /* check that we got a valid redundancy descriptor */
  if (d == NULL) {
    scr_err("No redundancy descriptor to fill from hash @ %s:%d",
      __FILE__, __LINE__
    );
    rc = SCR_FAILURE;
  }

  /* check that we got a valid pointer to a hash */
  if (hash == NULL) {
    scr_err("No hash specified to build redundancy descriptor from @ %s:%d",
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
  scr_reddesc_init(d);

  char* value = NULL;

  /* enable / disable the descriptor */
  d->enabled = 1;
  value = scr_hash_elem_get_first_val(hash, SCR_CONFIG_KEY_ENABLED);
  if (value != NULL) {
    d->enabled = atoi(value);
  }

  /* set the interval, default to 1 unless specified otherwise */
  d->interval = 1;
  value = scr_hash_elem_get_first_val(hash, SCR_CONFIG_KEY_INTERVAL);
  if (value != NULL) {
    d->interval = atoi(value);
  }

  /* set the base directory */
  value = scr_hash_elem_get_first_val(hash, SCR_CONFIG_KEY_BASE);
  if (value != NULL) {
    d->base = strdup(value);

    /* set the index to the store descriptor for this base directory */
    int store_index = scr_storedescs_index_from_name(d->base);
    if (store_index >= 0) {
      d->store_index = store_index;
    } else {
      /* couldn't find requested store, disable this descriptor and
       * warn user */
      d->enabled = 0;
      scr_warn("Failed to find store descriptor named %s @ %s:%d",
        d->base, __FILE__, __LINE__
      );
    }

//    d->group_index = scr_groupdescs_index_from_base(d->group);
  }

  /* build the directory name */
  value = scr_hash_elem_get_first_val(hash, SCR_CONFIG_KEY_DIRECTORY);
  if (value != NULL) {
    /* directory name already set, just copy it */
    d->directory = strdup(value);
  } else {
    /* if it's not set, we have no idea what it should be since we
     * we don't know the index which is included in the directory */
    scr_abort(-1, "Missing directory in descriptor hash @ %s:%d",
      __FILE__, __LINE__
    );
  }
    
  /* read the checkpoint type from the hash,
   * and build our checkpoint communicator */
  value = scr_hash_elem_get_first_val(hash, SCR_CONFIG_KEY_TYPE);
  if (value != NULL) {
    if (scr_reddesc_type_int_from_str(value, &d->copy_type) != SCR_SUCCESS) {
      d->enabled = 0;
      if (scr_my_rank_world == 0) {
        scr_warn("Unknown copy type %s in redundancy descriptor hash, disabling checkpoint @ %s:%d",
          value, __FILE__, __LINE__
        );
      }
    }
  }

  /* build the checkpoint communicator */
  char* group_id_str   = scr_hash_elem_get_first_val(hash, SCR_CONFIG_KEY_GROUP_ID);
  char* group_rank_str = scr_hash_elem_get_first_val(hash, SCR_CONFIG_KEY_GROUP_RANK);
  if (group_id_str != NULL && group_rank_str != NULL) {
    /* we already have a group id and rank,
     * use that to rebuild the communicator */
    int group_id   = atoi(group_id_str);
    int group_rank = atoi(group_rank_str);
    MPI_Comm_split(scr_comm_world, group_id, group_rank, &d->comm);
  } else {
    scr_abort(-1, "Failed to restore redundancy communicator @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* find our position in the checkpoint communicator */
  MPI_Comm_rank(d->comm, &d->my_rank);
  MPI_Comm_size(d->comm, &d->ranks);

  /* for our group id, use the global rank of the rank 0 task
   * in our checkpoint comm */
  d->group_id = scr_my_rank_world;
  MPI_Bcast(&d->group_id, 1, MPI_INT, 0, d->comm);

  /* count the number of groups */
  int group_master = (d->my_rank == 0) ? 1 : 0;
  MPI_Allreduce(
    &group_master, &d->groups, 1, MPI_INT, MPI_SUM, scr_comm_world
  );

  /* fill in state struct depending on copy type */
  switch (d->copy_type) {
  case SCR_COPY_SINGLE:
    break;
  case SCR_COPY_PARTNER:
    scr_reddesc_create_partner(d);
    break;
  case SCR_COPY_XOR:
    scr_reddesc_create_xor(d);
    break;
  }

  /* if anyone has disabled this checkpoint, everyone needs to */
  if (! scr_alltrue(d->enabled)) {
    d->enabled = 0;
  }

  return SCR_SUCCESS;
}

/* many times we just need a string value from the descriptor stored in
 * the filemap, it's overkill to create the whole descriptor each time,
 * returns a newly allocated string */
char* scr_reddesc_val_from_filemap(
  scr_filemap* map,
  int ckpt,
  int rank,
  char* name)
{
  /* check that we have a pointer to a map and a character buffer */
  if (map == NULL || name == NULL) {
    return NULL;
  }

  /* create an empty hash to store the redundancy descriptor
   * hash from the filemap */
  scr_hash* desc = scr_hash_new();
  if (desc == NULL) {
    return NULL;
  }

  /* get the redundancy descriptor hash from the filemap */
  if (scr_filemap_get_desc(map, ckpt, rank, desc) != SCR_SUCCESS) {
    scr_hash_delete(&desc);
    return NULL;
  }

  /* copy the directory from the redundancy descriptor hash, if it's
   * set */
  char* dup = NULL;
  char* val;
  if (scr_hash_util_get_str(desc, name, &val) == SCR_SUCCESS) {
    dup = strdup(val);
  }

  /* delete the hash object */
  scr_hash_delete(&desc);

  return dup;
}

/* read base directory from descriptor stored in filemap,
 * returns a newly allocated string */
char* scr_reddesc_base_from_filemap(scr_filemap* map, int ckpt, int rank)
{
  char* value = scr_reddesc_val_from_filemap(
    map, ckpt, rank, SCR_CONFIG_KEY_BASE
  );
  return value;
}

/* read directory from descriptor stored in filemap,
 * returns a newly allocated string */
char* scr_reddesc_dir_from_filemap(scr_filemap* map, int ckpt, int rank)
{
  char* value = scr_reddesc_val_from_filemap(
    map, ckpt, rank, SCR_CONFIG_KEY_DIRECTORY
  );
  return value;
}

/* build a redundancy descriptor from its corresponding hash
 * stored in the filemap, this function is collective */
int scr_reddesc_create_from_filemap(
  scr_filemap* map,
  int id,
  int rank,
  scr_reddesc* d)
{
  /* check that we have a pointer to a map and a redundancy
   * descriptor */
  if (map == NULL || d == NULL) {
    return SCR_FAILURE;
  }

  /* create an empty hash to store the redundancy descriptor hash
   * from the filemap */
  scr_hash* desc = scr_hash_new();
  if (desc == NULL) {
    return SCR_FAILURE;
  }

  /* get the redundancy descriptor hash from the filemap */
  if (scr_filemap_get_desc(map, id, rank, desc) != SCR_SUCCESS) {
    scr_hash_delete(&desc);
    return SCR_FAILURE;
  }

  /* fill in our redundancy descriptor */
  if (scr_reddesc_restore_from_hash(d, desc) != SCR_SUCCESS) {
    scr_hash_delete(&desc);
    return SCR_FAILURE;
  }

  /* delete the hash object */
  scr_hash_delete(&desc);

  return SCR_SUCCESS;
}

/* return pointer to store descriptor associated with redundancy
 * descriptor, returns NULL if reddesc or storedesc is not enabled */
scr_storedesc* scr_reddesc_get_store(const scr_reddesc* desc)
{
  /* verify that our redundancy descriptor is valid */
  if (desc == NULL) {
    return NULL;
  }

  /* check that redudancy descriptor is enabled */
  if (! desc->enabled) {
    return NULL;
  }

  /* check that its store index is within range */
  int index = desc->store_index;
  if (index < 0 || index >= scr_nstoredescs) {
    return NULL;
  }

  /* check that the store descriptor is enabled */
  scr_storedesc* store = &scr_storedescs[index];
  if (! store->enabled) {
    return NULL;
  }

  /* finally, all is good, return the address of the store descriptor */
  return store;
}

/*
=========================================
Routines that operate on scr_reddescs array
=========================================
*/

/* create scr_reddescs array from scr_reddescs_hash */
int scr_reddescs_create()
{
  /* set the number of redundancy descriptors */
  scr_nreddescs = 0;
  scr_hash* descs = scr_hash_get(scr_reddesc_hash, SCR_CONFIG_KEY_CKPTDESC);
  if (descs != NULL) {
    scr_nreddescs = scr_hash_size(descs);
  }

  /* allocate our redundancy descriptors */
  if (scr_nreddescs > 0) {
    scr_reddescs = (scr_reddesc*) malloc(scr_nreddescs * sizeof(scr_reddesc));
    /* TODO: check for errors */
  }

  /* flag to indicate whether we successfully build all redundancy
   * descriptors */
  int all_valid = 1;

  /* iterate over each of our hash entries filling in each
   * corresponding descriptor */
  int index = 0;
  if (scr_my_rank_world == 0) {
    /* have rank 0 determine the order in which we'll create the
     * descriptors */
    scr_hash_elem* elem;
    for (elem = scr_hash_elem_first(descs);
         elem != NULL;
         elem = scr_hash_elem_next(elem))
    {
      /* get key for this group */
      char* key = scr_hash_elem_key(elem);

      /* bcast key name */
      scr_str_bcast(&key, 0, scr_comm_world);

      /* get the info hash for this descriptor */
      scr_hash* hash = scr_hash_get(descs, key);

      /* create descriptor */
      if (scr_reddesc_create_from_hash(&scr_reddescs[index], index, hash)
          != SCR_SUCCESS)
      {
        scr_err("Failed to set up %s=%s @ %s:%f",
          SCR_CONFIG_KEY_CKPTDESC, key, __FILE__, __LINE__
        );
        all_valid = 0;
      }

      /* advance to our next descriptor */
      index++;
    }
  } else {
    int i;
    for (i = 0; i < scr_nreddescs; i++) {
      /* receive key */
      char* key;
      scr_str_bcast(&key, 0, scr_comm_world);

      /* get the info hash for this descriptor */
      scr_hash* hash = scr_hash_get(descs, key);

      /* create descriptor */
      if (scr_reddesc_create_from_hash(&scr_reddescs[index], index, hash)
          != SCR_SUCCESS)
      {
        all_valid = 0;
      }

      /* free key name */
      scr_free(&key);

      /* advance to our next descriptor */
      index++;
    }
  }

  /* determine whether everyone found a valid redundancy descriptor */
  if (! all_valid) {
    return SCR_FAILURE;
  }
  return SCR_SUCCESS;
}

/* free scr_reddescs array */
int scr_reddescs_free()
{
  /* iterate over and free each of our redundancy descriptors */
  if (scr_nreddescs > 0 && scr_reddescs != NULL) {
    int i;
    for (i=0; i < scr_nreddescs; i++) {
      scr_reddesc_free(&scr_reddescs[i]);
    }
  }

  /* set the count back to zero */
  scr_nreddescs = 0;

  /* and free off the memory allocated */
  scr_free(&scr_reddescs);

  return SCR_SUCCESS;
}
