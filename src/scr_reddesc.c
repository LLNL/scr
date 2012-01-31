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
  d->base           = NULL;
  d->directory      = NULL;
  d->copy_type      = SCR_COPY_NULL;
  d->comm           = MPI_COMM_NULL;
  d->groups         =  0;
  d->group_id       = -1;
  d->ranks          =  0;
  d->my_rank        = MPI_PROC_NULL;
  d->lhs_rank       = MPI_PROC_NULL;
  d->lhs_rank_world = MPI_PROC_NULL;
  strcpy(d->lhs_hostname, "");
  d->rhs_rank       = MPI_PROC_NULL;
  d->rhs_rank_world = MPI_PROC_NULL;
  strcpy(d->rhs_hostname, "");

  return SCR_SUCCESS;
}

/* free any memory associated with the specified redundancy descriptor */
int scr_reddesc_free(scr_reddesc* d)
{
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
 * select and return a pointer to a descriptor for the specified checkpoint id */
scr_reddesc* scr_reddesc_for_checkpoint(int id, int ndescs, scr_reddesc* descs)
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

/* convert the specified redundancy descritpor into a corresponding hash */
int scr_reddesc_store_to_hash(const scr_reddesc* d, scr_hash* hash)
{
  /* check that we got a valid pointer to a redundancy descriptor and a hash */
  if (d == NULL || hash == NULL) {
    return SCR_FAILURE;
  }

  /* clear the hash */
  scr_hash_unset_all(hash);

  /* set the ENABLED key */
  scr_hash_set_kv_int(hash, SCR_CONFIG_KEY_ENABLED, d->enabled);

  /* set the INDEX key */
  scr_hash_set_kv_int(hash, SCR_CONFIG_KEY_INDEX, d->index);

  /* set the INTERVAL key */
  scr_hash_set_kv_int(hash, SCR_CONFIG_KEY_INTERVAL, d->interval);

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

  /* set the GROUP_ID and GROUP_RANK keys */
  scr_hash_set_kv_int(hash, SCR_CONFIG_KEY_GROUPS,     d->groups);
  scr_hash_set_kv_int(hash, SCR_CONFIG_KEY_GROUP_ID,   d->group_id);
  scr_hash_set_kv_int(hash, SCR_CONFIG_KEY_GROUP_SIZE, d->ranks);
  scr_hash_set_kv_int(hash, SCR_CONFIG_KEY_GROUP_RANK, d->my_rank);

  /* set the DISTANCE and SIZE */
  scr_hash_set_kv_int(hash, SCR_CONFIG_KEY_HOP_DISTANCE, d->hop_distance);
  scr_hash_set_kv_int(hash, SCR_CONFIG_KEY_SET_SIZE,     d->set_size);

  return SCR_SUCCESS;
}

static void scr_reddesc_group_id(int rank, int ranks, int minsize, int* group_id)
{
  /* compute maximum number of full minsize groups we can fit within ranks */
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

  /* compute remaining ranks assuming we have groups of the new base size */
  int remainder = ranks % size;

  /* for each remainder rank, we increase the lower groups by a size of one,
   * so that we end up with remainder groups of size+1 followed by (groups - remainder) of size */

  /* cutoff is the first rank for which all groups are exactly size */
  int cutoff = remainder * (size + 1);

  if (rank < cutoff) {
    /* ranks below cutoff are grouped into sets of size+1 */
    *group_id = rank / (size + 1);
  } else {
    /* ranks at cutoff and higher are grouped into sets of size */
    *group_id = (rank - cutoff) / size + remainder;
  }
}

/* build a redundancy descriptor corresponding to the specified hash,
 * this function is collective, because it issues MPI calls */
int scr_reddesc_create_from_hash(scr_reddesc* d, int index, const scr_hash* hash)
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

  /* enable / disable the checkpoint */
  d->enabled = 1;
  value = scr_hash_elem_get_first_val(hash, SCR_CONFIG_KEY_ENABLED);
  if (value != NULL) {
    d->enabled = atoi(value);
  }

  /* index of the checkpoint */
  d->index = index;
  value = scr_hash_elem_get_first_val(hash, SCR_CONFIG_KEY_INDEX);
  if (value != NULL) {
    d->index = atoi(value);
  }

  /* set the checkpoint interval, default to 1 unless specified otherwise */
  d->interval = 1;
  value = scr_hash_elem_get_first_val(hash, SCR_CONFIG_KEY_INTERVAL);
  if (value != NULL) {
    d->interval = atoi(value);
  }

  /* set the base checkpoint directory */
  value = scr_hash_elem_get_first_val(hash, SCR_CONFIG_KEY_BASE);
  if (value != NULL) {
    d->base = strdup(value);
    d->store_index = scr_storedescs_index_from_base(d->base);
  }

  /* build the checkpoint directory name */
  value = scr_hash_elem_get_first_val(hash, SCR_CONFIG_KEY_DIRECTORY);
  if (value != NULL) {
    /* directory name already set, just copy it */
    d->directory = strdup(value);
  } else if (d->base != NULL) {
    /* directory name was not already set, so we need to build it */
    char str[100];
    sprintf(str, "%d", d->index);
    int dirname_size = strlen(d->base) + 1 + strlen(scr_username) + strlen("/scr.") + strlen(scr_jobid) +
                       strlen("/index.") + strlen(str) + 1;
    d->directory = (char*) malloc(dirname_size);
    sprintf(d->directory, "%s/%s/scr.%s/index.%s", d->base, scr_username, scr_jobid, str);
  }
    
  /* set the partner hop distance */
  d->hop_distance = scr_hop_distance;
  value = scr_hash_elem_get_first_val(hash, SCR_CONFIG_KEY_HOP_DISTANCE);
  if (value != NULL) {
    d->hop_distance = atoi(value);
  }

  /* set the xor set size */
  d->set_size = scr_set_size;
  value = scr_hash_elem_get_first_val(hash, SCR_CONFIG_KEY_SET_SIZE);
  if (value != NULL) {
    d->set_size = atoi(value);
  }

  /* read the checkpoint type from the hash, and build our checkpoint communicator */
  value = scr_hash_elem_get_first_val(hash, SCR_CONFIG_KEY_TYPE);
  if (value != NULL) {
    if (strcasecmp(value, "SINGLE") == 0) {
      d->copy_type = SCR_COPY_SINGLE;
    } else if (strcasecmp(value, "PARTNER") == 0) {
      d->copy_type = SCR_COPY_PARTNER;
    } else if (strcasecmp(value, "XOR") == 0) {
      d->copy_type = SCR_COPY_XOR;
    } else {
      d->enabled = 0;
      if (scr_my_rank_world == 0) {
        scr_warn("Unknown copy type %s in redundancy descriptor %d, disabling checkpoint @ %s:%d",
                value, d->index, __FILE__, __LINE__
        );
      }
    }

    /* CONVENIENCE: if all ranks are on the same node, change checkpoint type to SINGLE,
     * we do this so single-node jobs can run without requiring the user to change the copy type */
    const scr_groupdesc* groupdesc = scr_groupdescs_from_name(SCR_GROUP_NODE);
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

    /* build the checkpoint communicator */
    char* group_id_str   = scr_hash_elem_get_first_val(hash, SCR_CONFIG_KEY_GROUP_ID);
    char* group_rank_str = scr_hash_elem_get_first_val(hash, SCR_CONFIG_KEY_GROUP_RANK);
    if (group_id_str != NULL && group_rank_str != NULL) {
      /* we already have a group id and rank, use that to rebuild the communicator */
      int group_id   = atoi(group_id_str);
      int group_rank = atoi(group_rank_str);
      MPI_Comm_split(scr_comm_world, group_id, group_rank, &d->comm);
    } else {
      /* otherwise, build the communicator based on the copy type and other parameters */
      int rel_rank, mod_rank, rel_ranks, mod_ranks, split_id;
      const scr_groupdesc* groupdesc;
      switch (d->copy_type) {
      case SCR_COPY_SINGLE:
        /* not going to communicate with anyone, so just dup COMM_SELF */
        MPI_Comm_dup(MPI_COMM_SELF, &d->comm);
        break;
      case SCR_COPY_PARTNER:
        /* dup the communicator across nodes */
        groupdesc = scr_groupdescs_from_name(SCR_GROUP_NODE);
        if (groupdesc != NULL) {
          MPI_Comm_dup(groupdesc->comm_across, &d->comm);
        } else {
          scr_abort(-1, "Failed to get communicator across failure groups @ %s:%d",
                  __FILE__, __LINE__
          );
        }
        break;
      case SCR_COPY_XOR:
        /* split the communicator across nodes based on xor set size to create our xor communicator */
        groupdesc = scr_groupdescs_from_name(SCR_GROUP_NODE);
        if (groupdesc != NULL) {
          rel_rank  = groupdesc->rank_across / d->hop_distance;
          mod_rank  = groupdesc->rank_across % d->hop_distance;

          /* compute number of ranks in our group */
          rel_ranks = groupdesc->ranks_across / d->hop_distance;
          mod_ranks = groupdesc->ranks_across % d->hop_distance;
          if (mod_ranks != 0 && mod_rank < mod_ranks) {
             rel_ranks++;
          }

          /* identify which group we're in */
          scr_reddesc_group_id(rel_rank, rel_ranks, d->set_size, &split_id);
          split_id = split_id * d->hop_distance + mod_rank;

          /* split communicator into groups */
          MPI_Comm_split(groupdesc->comm_across, split_id, scr_my_rank_world, &d->comm);
        } else {
          scr_abort(-1, "Failed to get communicator across failure groups @ %s:%d",
                  __FILE__, __LINE__
          );
        }
        break;
      }
    }

    /* find our position in the checkpoint communicator */
    MPI_Comm_rank(d->comm, &d->my_rank);
    MPI_Comm_size(d->comm, &d->ranks);

    /* for our group id, use the global rank of the rank 0 task in our checkpoint comm */
    d->group_id = scr_my_rank_world;
    MPI_Bcast(&d->group_id, 1, MPI_INT, 0, d->comm);

    /* count the number of groups */
    int group_master = (d->my_rank == 0) ? 1 : 0;
    MPI_Allreduce(&group_master, &d->groups, 1, MPI_INT, MPI_SUM, scr_comm_world);

    /* find left and right-hand-side partners (SINGLE needs no partner nodes) */
    if (d->copy_type == SCR_COPY_PARTNER) {
      scr_set_partners(d->comm, d->hop_distance,
          &d->lhs_rank, &d->lhs_rank_world, d->lhs_hostname,
          &d->rhs_rank, &d->rhs_rank_world, d->rhs_hostname
      );
    } else if (d->copy_type == SCR_COPY_XOR) {
      scr_set_partners(d->comm, 1,
          &d->lhs_rank, &d->lhs_rank_world, d->lhs_hostname,
          &d->rhs_rank, &d->rhs_rank_world, d->rhs_hostname
      );
    }

    /* check that we have a valid partner node (SINGLE needs no partner nodes) */
    if (d->copy_type == SCR_COPY_PARTNER || d->copy_type == SCR_COPY_XOR) {
      if (strcmp(d->lhs_hostname, "") == 0 ||
          strcmp(d->rhs_hostname, "") == 0 ||
          strcmp(d->lhs_hostname, scr_my_hostname) == 0 ||
          strcmp(d->rhs_hostname, scr_my_hostname) == 0)
      {
        d->enabled = 0;
        scr_warn("Failed to find partner processes for redundancy descriptor %d, disabling checkpoint, too few nodes? @ %s:%d",
                d->index, __FILE__, __LINE__
        );
      } else {
        scr_dbg(2, "LHS partner: %s (%d)  -->  My name: %s (%d)  -->  RHS partner: %s (%d)",
                d->lhs_hostname, d->lhs_rank_world, scr_my_hostname, scr_my_rank_world, d->rhs_hostname, d->rhs_rank_world
        );
      }
    }

    /* if anyone has disabled this checkpoint, everyone needs to */
    if (! scr_alltrue(d->enabled)) {
      d->enabled = 0;
    }
  }

  return SCR_SUCCESS;
}

/* many times we just need the directory for the checkpoint,
 * it's overkill to create the whole descriptor each time */
char* scr_reddesc_val_from_filemap(scr_filemap* map, int ckpt, int rank, char* name)
{
  /* check that we have a pointer to a map and a character buffer */
  if (map == NULL || name == NULL) {
    return NULL;
  }

  /* create an empty hash to store the redundancy descriptor hash from the filemap */
  scr_hash* desc = scr_hash_new();
  if (desc == NULL) {
    return NULL;
  }

  /* get the redundancy descriptor hash from the filemap */
  if (scr_filemap_get_desc(map, ckpt, rank, desc) != SCR_SUCCESS) {
    scr_hash_delete(desc);
    return NULL;
  }

  /* copy the directory from the redundancy descriptor hash, if it's set */
  char* dup = NULL;
  char* val;
  if (scr_hash_util_get_str(desc, name, &val) == SCR_SUCCESS) {
    dup = strdup(val);
  }

  /* delete the hash object */
  scr_hash_delete(desc);

  return dup;
}

/* many times we just need the directory for the checkpoint,
 * it's overkill to create the whole descripter each time */
char* scr_reddesc_base_from_filemap(scr_filemap* map, int ckpt, int rank)
{
  char* value = scr_reddesc_val_from_filemap(map, ckpt, rank, SCR_CONFIG_KEY_BASE);
  return value;
}

/* many times we just need the directory for the checkpoint,
 * it's overkill to create the whole descripter each time */
char* scr_reddesc_dir_from_filemap(scr_filemap* map, int ckpt, int rank)
{
  char* value = scr_reddesc_val_from_filemap(map, ckpt, rank, SCR_CONFIG_KEY_DIRECTORY);
  return value;
}

/* build a redundancy descriptor from its corresponding hash stored in the filemap,
 * this function is collective */
int scr_reddesc_create_from_filemap(scr_filemap* map, int id, int rank, scr_reddesc* d)
{
  /* check that we have a pointer to a map and a redundancy descriptor */
  if (map == NULL || d == NULL) {
    return SCR_FAILURE;
  }

  /* create an empty hash to store the redundancy descriptor hash from the filemap */
  scr_hash* desc = scr_hash_new();
  if (desc == NULL) {
    return SCR_FAILURE;
  }

  /* get the redundancy descriptor hash from the filemap */
  if (scr_filemap_get_desc(map, id, rank, desc) != SCR_SUCCESS) {
    scr_hash_delete(desc);
    return SCR_FAILURE;
  }

  /* fill in our redundancy descriptor */
  if (scr_reddesc_create_from_hash(d, -1, desc) != SCR_SUCCESS) {
    scr_hash_delete(desc);
    return SCR_FAILURE;
  }

  /* delete the hash object */
  scr_hash_delete(desc);

  return SCR_SUCCESS;
}

/*
=========================================
Routines that operate on scr_reddescs array
=========================================
*/

int scr_reddescs_create()
{
  /* set the number of redundancy descriptors */
  scr_nreddescs = 0;
  scr_hash* tmp = scr_hash_get(scr_reddesc_hash, SCR_CONFIG_KEY_CKPTDESC);
  if (tmp != NULL) {
    scr_nreddescs = scr_hash_size(tmp);
  }

  /* allocate our redundancy descriptors */
  if (scr_nreddescs > 0) {
    scr_reddescs = (scr_reddesc*) malloc(scr_nreddescs * sizeof(scr_reddesc));
    /* TODO: check for errors */
  }

  int all_valid = 1;

  /* iterate over each of our hash entries filling in each corresponding descriptor */
  int i;
  for (i=0; i < scr_nreddescs; i++) {
    /* get the info hash for this descriptor */
    scr_hash* hash = scr_hash_get_kv_int(scr_reddesc_hash, SCR_CONFIG_KEY_CKPTDESC, i);
    if (scr_reddesc_create_from_hash(&scr_reddescs[i], i, hash) != SCR_SUCCESS) {
      all_valid = 0;
    }
  }

  /* determine whether everyone found a valid redundancy descriptor */
  if (!all_valid) {
    return SCR_FAILURE;
  }
  return SCR_SUCCESS;
}

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
