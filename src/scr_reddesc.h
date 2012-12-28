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

#ifndef SCR_REDDESC_H
#define SCR_REDDESC_H

#include "scr_globals.h"

/*
=========================================
Define redundancy descriptor structure
=========================================
*/

typedef struct {
  int      enabled;        /* flag indicating whether this descriptor is active */
  int      index;          /* each descriptor is indexed starting from 0 */
  int      interval;       /* how often to apply this descriptor, pick largest such
                            * that interval evenly divides checkpoint id */
  int      store_index;    /* index into scr_storedesc for storage descriptor */
  int      group_index;    /* index into scr_groupdesc for failure group */
  char*    base;           /* base cache directory to use */
  char*    directory;      /* full directory base/dataset.id */
  int      copy_type;      /* redundancy scheme to apply */
  void*    copy_state;     /* pointer to extra state depending on copy type */
  MPI_Comm comm;           /* communicator holding procs for this scheme */
  int      groups;         /* number of redundancy sets */
  int      group_id;       /* unique id assigned to this redundancy set */
  int      ranks;          /* number of ranks in this set */
  int      my_rank;        /* caller's rank within its set */
} scr_reddesc;

typedef struct {
  int       lhs_rank;       /* rank which is one less (with wrap to highest) within set */
  int       lhs_rank_world; /* rank of lhs process in comm world */
  char*     lhs_hostname;   /* hostname of lhs process */
  int       rhs_rank;       /* rank which is one more (with wrap to lowest) within set */
  int       rhs_rank_world; /* rank of rhs process in comm world */
  char*     rhs_hostname;   /* hostname of rhs process */
} scr_reddesc_partner;

typedef struct {
  scr_hash* group_map;      /* hash that maps group rank to world rank */
  int       lhs_rank;       /* rank which is one less (with wrap to highest) within set */
  int       lhs_rank_world; /* rank of lhs process in comm world */
  char*     lhs_hostname;   /* hostname of lhs process */
  int       rhs_rank;       /* rank which is one more (with wrap to lowest) within set */
  int       rhs_rank_world; /* rank of rhs process in comm world */
  char*     rhs_hostname;   /* hostname of rhs process */
} scr_reddesc_xor;

/*
=========================================
Redundancy descriptor functions
=========================================
*/

/* initialize the specified redundancy descriptor */
int scr_reddesc_init(
  scr_reddesc* c
);

/* free any memory associated with the specified redundancy
 * descriptor */
int scr_reddesc_free(
  scr_reddesc* c
);

/* given a checkpoint id and a list of redundancy descriptors,
 * select and return a pointer to a descriptor for the 
 * specified checkpoint id */
scr_reddesc* scr_reddesc_for_checkpoint(
  int id,
  int nckpts,
  scr_reddesc* ckpts
);

/* convert the specified redundancy descritpor into a corresponding
 * hash */
int scr_reddesc_store_to_hash(
  const scr_reddesc* c,
  scr_hash* hash
);

/* build a redundancy descriptor corresponding to the specified hash,
 * this function is collective */
int scr_reddesc_create_from_hash(
  scr_reddesc* c,
  int index,
  const scr_hash* hash
);

/* build a redundancy descriptor from its corresponding hash stored
 * in the filemap, this function is collective */
int scr_reddesc_create_from_filemap(
  scr_filemap* map, 
  int id,
  int rank,
  scr_reddesc* c
);

/* many times we just need a string value from the descriptor
 * stored in the filemap, it's overkill to create the whole
 * descriptor each time, returns a newly allocated string */
char* scr_reddesc_val_from_filemap(
  scr_filemap* map,
  int ckpt,
  int rank,
  char* name
);

/* read base directory from descriptor stored in filemap,
 * returns a newly allocated string */
char* scr_reddesc_base_from_filemap(
  scr_filemap* map,
  int ckpt,
  int rank
);

/* read directory from descriptor stored in filemap,
 * returns a newly allocated string */
char* scr_reddesc_dir_from_filemap(
  scr_filemap* map,
  int ckpt,
  int rank
);

/* return pointer to store descriptor associated with redundancy
 * descriptor, returns NULL if reddesc or storedesc is not enabled */
scr_storedesc* scr_reddesc_get_store(
  const scr_reddesc* desc
);

/*
=========================================
Routines that operate on scr_reddescs array
=========================================
*/

/* create scr_reddescs array from scr_reddescs_hash */
int scr_reddescs_create();

/* free scr_reddescs array */
int scr_reddescs_free();

#endif
