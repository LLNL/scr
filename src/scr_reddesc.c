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

/* initialize the specified redundancy descriptor struct */
int scr_reddesc_init(struct scr_reddesc* c)
{
  /* check that we got a valid redundancy descriptor */
  if (c == NULL) {
    scr_err("No redundancy descriptor to fill from hash @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* initialize the descriptor */
  c->enabled        =  0;
  c->index          = -1;
  c->interval       = -1;
  c->base           = NULL;
  c->directory      = NULL;
  c->copy_type      = SCR_COPY_NULL;
  c->comm           = MPI_COMM_NULL;
  c->groups         =  0;
  c->group_id       = -1;
  c->ranks          =  0;
  c->my_rank        = MPI_PROC_NULL;
  c->lhs_rank       = MPI_PROC_NULL;
  c->lhs_rank_world = MPI_PROC_NULL;
  strcpy(c->lhs_hostname, "");
  c->rhs_rank       = MPI_PROC_NULL;
  c->rhs_rank_world = MPI_PROC_NULL;
  strcpy(c->rhs_hostname, "");

  return SCR_SUCCESS;
}

/* free any memory associated with the specified redundancy descriptor struct */
int scr_reddesc_free(struct scr_reddesc* c)
{
  /* free the strings we strdup'd */
  if (c->base != NULL) {
    free(c->base);
    c->base = NULL;
  }

  if (c->directory != NULL) {
    free(c->directory);
    c->directory = NULL;
  }

  /* free the communicator we created */
  if (c->comm != MPI_COMM_NULL) {
    MPI_Comm_free(&c->comm);
  }

  return SCR_SUCCESS;
}

/* given a checkpoint id and a list of redundancy descriptor structs,
 * select and return a pointer to a descriptor for the specified checkpoint id */
struct scr_reddesc* scr_reddesc_for_checkpoint(int id, int nckpts, struct scr_reddesc* ckpts)
{
  struct scr_reddesc* c = NULL;

  /* pick the redundancy descriptor that is:
   *   1) enabled
   *   2) has the highest interval that evenly divides id */
  int i;
  int interval = 0;
  for (i=0; i < nckpts; i++) {
    if (ckpts[i].enabled &&
        interval < ckpts[i].interval &&
        id % ckpts[i].interval == 0)
    {
      c = &ckpts[i];
      interval = ckpts[i].interval;
    }
  }

  return c;
}

/* convert the specified redundancy descritpor struct into a corresponding hash */
int scr_reddesc_store_to_hash(const struct scr_reddesc* c, scr_hash* hash)
{
  /* check that we got a valid pointer to a redundancy descriptor and a hash */
  if (c == NULL || hash == NULL) {
    return SCR_FAILURE;
  }

  /* clear the hash */
  scr_hash_unset_all(hash);

  /* set the ENABLED key */
  scr_hash_set_kv_int(hash, SCR_CONFIG_KEY_ENABLED, c->enabled);

  /* set the INDEX key */
  scr_hash_set_kv_int(hash, SCR_CONFIG_KEY_INDEX, c->index);

  /* set the INTERVAL key */
  scr_hash_set_kv_int(hash, SCR_CONFIG_KEY_INTERVAL, c->interval);

  /* set the BASE key */
  if (c->base != NULL) {
    scr_hash_set_kv(hash, SCR_CONFIG_KEY_BASE, c->base);
  }

  /* set the DIRECTORY key */
  if (c->directory != NULL) {
    scr_hash_set_kv(hash, SCR_CONFIG_KEY_DIRECTORY, c->directory);
  }

  /* set the TYPE key */
  switch (c->copy_type) {
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
  scr_hash_set_kv_int(hash, SCR_CONFIG_KEY_GROUPS,     c->groups);
  scr_hash_set_kv_int(hash, SCR_CONFIG_KEY_GROUP_ID,   c->group_id);
  scr_hash_set_kv_int(hash, SCR_CONFIG_KEY_GROUP_SIZE, c->ranks);
  scr_hash_set_kv_int(hash, SCR_CONFIG_KEY_GROUP_RANK, c->my_rank);

  /* set the DISTANCE and SIZE */
  scr_hash_set_kv_int(hash, SCR_CONFIG_KEY_HOP_DISTANCE, c->hop_distance);
  scr_hash_set_kv_int(hash, SCR_CONFIG_KEY_SET_SIZE,     c->set_size);

  return SCR_SUCCESS;
}

/* build a redundancy descriptor corresponding to the specified hash,
 * this function is collective, because it issues MPI calls */
int scr_reddesc_create_from_hash(struct scr_reddesc* c, int index, const scr_hash* hash)
{
  int rc = SCR_SUCCESS;

  /* check that we got a valid redundancy descriptor */
  if (c == NULL) {
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
    if (c != NULL) {
      c->enabled = 0;
    }
    return SCR_FAILURE;
  }

  /* initialize the descriptor */
  scr_reddesc_init(c);

  char* value = NULL;

  /* enable / disable the checkpoint */
  c->enabled = 1;
  value = scr_hash_elem_get_first_val(hash, SCR_CONFIG_KEY_ENABLED);
  if (value != NULL) {
    c->enabled = atoi(value);
  }

  /* index of the checkpoint */
  c->index = index;
  value = scr_hash_elem_get_first_val(hash, SCR_CONFIG_KEY_INDEX);
  if (value != NULL) {
    c->index = atoi(value);
  }

  /* set the checkpoint interval, default to 1 unless specified otherwise */
  c->interval = 1;
  value = scr_hash_elem_get_first_val(hash, SCR_CONFIG_KEY_INTERVAL);
  if (value != NULL) {
    c->interval = atoi(value);
  }

  /* set the base checkpoint directory */
  value = scr_hash_elem_get_first_val(hash, SCR_CONFIG_KEY_BASE);
  if (value != NULL) {
    c->base = strdup(value);
  }

  /* build the checkpoint directory name */
  value = scr_hash_elem_get_first_val(hash, SCR_CONFIG_KEY_DIRECTORY);
  if (value != NULL) {
    /* directory name already set, just copy it */
    c->directory = strdup(value);
  } else if (c->base != NULL) {
    /* directory name was not already set, so we need to build it */
    char str[100];
    sprintf(str, "%d", c->index);
    int dirname_size = strlen(c->base) + 1 + strlen(scr_username) + strlen("/scr.") + strlen(scr_jobid) +
                       strlen("/index.") + strlen(str) + 1;
    c->directory = (char*) malloc(dirname_size);
    sprintf(c->directory, "%s/%s/scr.%s/index.%s", c->base, scr_username, scr_jobid, str);
  }
    
  /* set the partner hop distance */
  c->hop_distance = scr_hop_distance;
  value = scr_hash_elem_get_first_val(hash, SCR_CONFIG_KEY_HOP_DISTANCE);
  if (value != NULL) {
    c->hop_distance = atoi(value);
  }

  /* set the xor set size */
  c->set_size = scr_set_size;
  value = scr_hash_elem_get_first_val(hash, SCR_CONFIG_KEY_SET_SIZE);
  if (value != NULL) {
    c->set_size = atoi(value);
  }

  /* read the checkpoint type from the hash, and build our checkpoint communicator */
  value = scr_hash_elem_get_first_val(hash, SCR_CONFIG_KEY_TYPE);
  if (value != NULL) {
    if (strcasecmp(value, "SINGLE") == 0) {
      c->copy_type = SCR_COPY_SINGLE;
    } else if (strcasecmp(value, "PARTNER") == 0) {
      c->copy_type = SCR_COPY_PARTNER;
    } else if (strcasecmp(value, "XOR") == 0) {
      c->copy_type = SCR_COPY_XOR;
    } else {
      c->enabled = 0;
      if (scr_my_rank_world == 0) {
        scr_warn("Unknown copy type %s in redundancy descriptor %d, disabling checkpoint @ %s:%d",
                value, c->index, __FILE__, __LINE__
        );
      }
    }

    /* CONVENIENCE: if all ranks are on the same node, change checkpoint type to SINGLE,
     * we do this so single-node jobs can run without requiring the user to change the copy type */
    if (scr_ranks_local == scr_ranks_world) {
      if (scr_my_rank_world == 0) {
        if (c->copy_type != SCR_COPY_SINGLE) {
          /* print a warning if we changed things on the user */
          scr_warn("Forcing copy type to SINGLE in redundancy descriptor %d @ %s:%d",
                  c->index, __FILE__, __LINE__
          );
        }
      }
      c->copy_type = SCR_COPY_SINGLE;
    }

    /* build the checkpoint communicator */
    char* group_id_str   = scr_hash_elem_get_first_val(hash, SCR_CONFIG_KEY_GROUP_ID);
    char* group_rank_str = scr_hash_elem_get_first_val(hash, SCR_CONFIG_KEY_GROUP_RANK);
    if (group_id_str != NULL && group_rank_str != NULL) {
      /* we already have a group id and rank, use that to rebuild the communicator */
      int group_id   = atoi(group_id_str);
      int group_rank = atoi(group_rank_str);
      MPI_Comm_split(scr_comm_world, group_id, group_rank, &c->comm);
    } else {
      /* otherwise, build the communicator based on the copy type and other parameters */
      int rel_rank, mod_rank, split_id;
      switch (c->copy_type) {
      case SCR_COPY_SINGLE:
        /* not going to communicate with anyone, so just dup COMM_SELF */
        MPI_Comm_dup(MPI_COMM_SELF, &c->comm);
        break;
      case SCR_COPY_PARTNER:
        /* dup the global level communicator */
        MPI_Comm_dup(scr_comm_level, &c->comm);
        break;
      case SCR_COPY_XOR:
        /* split the scr_comm_level communicator based on xor set size to create our xor communicator */
        rel_rank = scr_my_rank_level / c->hop_distance;
        mod_rank = scr_my_rank_level % c->hop_distance;
        split_id = (rel_rank / c->set_size) * c->hop_distance + mod_rank;
        MPI_Comm_split(scr_comm_level, split_id, scr_my_rank_world, &c->comm);
        break;
      }
    }

    /* find our position in the checkpoint communicator */
    MPI_Comm_rank(c->comm, &c->my_rank);
    MPI_Comm_size(c->comm, &c->ranks);

    /* for our group id, use the global rank of the rank 0 task in our checkpoint comm */
    c->group_id = scr_my_rank_world;
    MPI_Bcast(&c->group_id, 1, MPI_INT, 0, c->comm);

    /* count the number of groups */
    int group_master = (c->my_rank == 0) ? 1 : 0;
    MPI_Allreduce(&group_master, &c->groups, 1, MPI_INT, MPI_SUM, scr_comm_world);

    /* find left and right-hand-side partners (SINGLE needs no partner nodes) */
    if (c->copy_type == SCR_COPY_PARTNER) {
      scr_set_partners(c->comm, c->hop_distance,
          &c->lhs_rank, &c->lhs_rank_world, c->lhs_hostname,
          &c->rhs_rank, &c->rhs_rank_world, c->rhs_hostname
      );
    } else if (c->copy_type == SCR_COPY_XOR) {
      scr_set_partners(c->comm, 1,
          &c->lhs_rank, &c->lhs_rank_world, c->lhs_hostname,
          &c->rhs_rank, &c->rhs_rank_world, c->rhs_hostname
      );
    }

    /* check that we have a valid partner node (SINGLE needs no partner nodes) */
    if (c->copy_type == SCR_COPY_PARTNER || c->copy_type == SCR_COPY_XOR) {
      if (strcmp(c->lhs_hostname, "") == 0 ||
          strcmp(c->rhs_hostname, "") == 0 ||
          strcmp(c->lhs_hostname, scr_my_hostname) == 0 ||
          strcmp(c->rhs_hostname, scr_my_hostname) == 0)
      {
        c->enabled = 0;
        scr_warn("Failed to find partner processes for redundancy descriptor %d, disabling checkpoint, too few nodes? @ %s:%d",
                c->index, __FILE__, __LINE__
        );
      } else {
        scr_dbg(2, "LHS partner: %s (%d)  -->  My name: %s (%d)  -->  RHS partner: %s (%d)",
                c->lhs_hostname, c->lhs_rank_world, scr_my_hostname, scr_my_rank_world, c->rhs_hostname, c->rhs_rank_world
        );
      }
    }

    /* if anyone has disabled this checkpoint, everyone needs to */
    if (! scr_alltrue(c->enabled)) {
      c->enabled = 0;
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

/* build a redundancy descriptor struct from its corresponding hash stored in the filemap,
 * this function is collective */
int scr_reddesc_create_from_filemap(scr_filemap* map, int id, int rank, struct scr_reddesc* c)
{
  /* check that we have a pointer to a map and a redundancy descriptor */
  if (map == NULL || c == NULL) {
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
  if (scr_reddesc_create_from_hash(c, -1, desc) != SCR_SUCCESS) {
    scr_hash_delete(desc);
    return SCR_FAILURE;
  }

  /* delete the hash object */
  scr_hash_delete(desc);

  return SCR_SUCCESS;
}

int scr_reddesc_create_list()
{
  /* set the number of redundancy descriptors */
  scr_nreddescs = 0;
  scr_hash* tmp = scr_hash_get(scr_reddesc_hash, SCR_CONFIG_KEY_CKPTDESC);
  if (tmp != NULL) {
    scr_nreddescs = scr_hash_size(tmp);
  }

  /* allocate our redundancy descriptors */
  if (scr_nreddescs > 0) {
    scr_reddescs = (struct scr_reddesc*) malloc(scr_nreddescs * sizeof(struct scr_reddesc));
    /* TODO: check for errors */
  }

  int all_valid = 1;

  /* iterate over each of our checkpoints filling in each corresponding descriptor */
  int i;
  for (i=0; i < scr_nreddescs; i++) {
    /* get the info hash for this checkpoint */
    scr_hash* ckpt_hash = scr_hash_get_kv_int(scr_reddesc_hash, SCR_CONFIG_KEY_CKPTDESC, i);
    if (scr_reddesc_create_from_hash(&scr_reddescs[i], i, ckpt_hash) != SCR_SUCCESS) {
      all_valid = 0;
    }
  }

  /* determine whether everyone found a valid redundancy descriptor */
  if (!all_valid) {
    return SCR_FAILURE;
  }
  return SCR_SUCCESS;
}

int scr_reddesc_free_list()
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
  if (scr_reddescs != NULL) {
    free(scr_reddescs);
    scr_reddescs = NULL;
  }

  return SCR_SUCCESS;
}
