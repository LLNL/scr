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
#include "er.h"

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
  d->enabled     =  0;
  d->index       = -1;
  d->interval    = -1;
  d->output      = -1;
  d->bypass      = -1;
  d->store_index = -1;
  d->group_index = -1;
  d->base        = NULL;
  d->directory   = NULL;
  d->copy_type   = SCR_COPY_NULL;
  d->er_scheme   = -1;

  return SCR_SUCCESS;
}

/* free any memory associated with the specified redundancy
 * descriptor */
int scr_reddesc_free(scr_reddesc* d)
{
  /* free the strings we strdup'd */
  scr_free(&d->base);
  scr_free(&d->directory);

  /* free off ER scheme resources */
  if (d->er_scheme != -1) {
    ER_Free_Scheme(d->er_scheme);
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
int scr_reddesc_store_to_hash(const scr_reddesc* d, kvtree* hash)
{
  /* check that we got a valid pointer to a redundancy descriptor and
   * a hash */
  if (d == NULL || hash == NULL) {
    return SCR_FAILURE;
  }

  /* clear the hash */
  kvtree_unset_all(hash);

  /* set the ENABLED key */
  kvtree_set_kv_int(hash, SCR_CONFIG_KEY_ENABLED, d->enabled);

  /* we don't set the INDEX because this is dependent on runtime
   * environment */

  /* set the INTERVAL key */
  kvtree_set_kv_int(hash, SCR_CONFIG_KEY_INTERVAL, d->interval);

  /* set the OUTPUT key */
  kvtree_set_kv_int(hash, SCR_CONFIG_KEY_OUTPUT, d->output);

  /* set the BYPASS key */
  kvtree_set_kv_int(hash, SCR_CONFIG_KEY_BYPASS, d->bypass);

  /* we don't set STORE_INDEX because this is dependent on runtime
   * environment */

  /* we don't set GROUP_INDEX because this is dependent on runtime
   * environment */

  /* set the STORE key */
  if (d->base != NULL) {
    kvtree_set_kv(hash, SCR_CONFIG_KEY_STORE, d->base);
  }

  /* set the DIRECTORY key */
  if (d->directory != NULL) {
    kvtree_set_kv(hash, SCR_CONFIG_KEY_DIRECTORY, d->directory);
  }

  /* set the TYPE key */
  switch (d->copy_type) {
  case SCR_COPY_SINGLE:
    kvtree_set_kv(hash, SCR_CONFIG_KEY_TYPE, "SINGLE");
    break;
  case SCR_COPY_PARTNER:
    kvtree_set_kv(hash, SCR_CONFIG_KEY_TYPE, "PARTNER");
    break;
  case SCR_COPY_XOR:
    kvtree_set_kv(hash, SCR_CONFIG_KEY_TYPE, "XOR");
    break;
  case SCR_COPY_RS:
    kvtree_set_kv(hash, SCR_CONFIG_KEY_TYPE, "RS");
    break;
  }

  return SCR_SUCCESS;
}

/* convert copy type string to integer value */
static int scr_reddesc_type_int_from_str(const char* value, int* type)
{
  int rc = SCR_SUCCESS;

  int copy_type = SCR_COPY_NULL;
  if (strcasecmp(value, "SINGLE") == 0) {
    copy_type = SCR_COPY_SINGLE;
  } else if (strcasecmp(value, "PARTNER") == 0) {
    copy_type = SCR_COPY_PARTNER;
  } else if (strcasecmp(value, "XOR") == 0) {
    copy_type = SCR_COPY_XOR;
  } else if (strcasecmp(value, "RS") == 0) {
    copy_type = SCR_COPY_RS;
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
  const kvtree* hash)
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

  /* read the group name */
  char* groupname = scr_group;
  const scr_groupdesc* groupdesc;
  kvtree_util_get_str(hash, SCR_CONFIG_KEY_GROUP, &groupname);
  /* get group descriptor */
  if (!(groupdesc = scr_groupdescs_from_name(groupname))){
    scr_err("Could not get group '%s' listed in config", groupname);
    rc = SCR_FAILURE;
  }

  /* check that everyone made it this far */
  if (! scr_alltrue(rc == SCR_SUCCESS, scr_comm_world)) {
    if (d != NULL) {
      d->enabled = 0;
    }
    return SCR_FAILURE;
  }

  /* initialize the descriptor */
  scr_reddesc_init(d);

  /* enable / disable the descriptor */
  d->enabled = 1;
  kvtree_util_get_int(hash, SCR_CONFIG_KEY_ENABLED, &(d->enabled));

  /* index of the descriptor */
  d->index = index;

  /* set the interval, default to 1 unless specified otherwise */
  d->interval = 1;
  kvtree_util_get_int(hash, SCR_CONFIG_KEY_INTERVAL, &(d->interval));

  /* set output flag, assume this can't be used for output */
  d->output = 0;
  kvtree_util_get_int(hash, SCR_CONFIG_KEY_OUTPUT, &(d->output));

  /* set bypass flag */
  d->bypass = scr_cache_bypass;
  kvtree_util_get_int(hash, SCR_CONFIG_KEY_BYPASS, &(d->bypass));

  /* get the store name */
  char* base = scr_cache_base;
  kvtree_util_get_str(hash, SCR_CONFIG_KEY_STORE, &base);
  if (base != NULL) {
    /* strdup base after reducing it */
    d->base = spath_strdup_reduce_str(base);

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
  } else {
    /* couldn't find requested store, disable this descriptor and
     * warn user */
    d->enabled = 0;
    scr_warn("Failed to find store parameter for redundancy descriptor @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* build the directory name */
  spath* dir = spath_from_str(d->base);
  spath_append_str(dir, scr_username);
  spath_append_strf(dir, "scr.%s", scr_jobid);
//  spath_append_strf(dir, "index.%d", d->index);
  spath_reduce(dir);
  d->directory = spath_strdup(dir);
  spath_delete(&dir);
    
  /* set the redundancy set size */
  int set_size = scr_set_size;
  kvtree_util_get_int(hash, SCR_CONFIG_KEY_SET_SIZE, &set_size);

  /* set the number of failures to tolerate in each set */
  int set_failures = scr_set_failures;
  kvtree_util_get_int(hash, SCR_CONFIG_KEY_SET_FAILURES, &set_failures);
  if (set_failures < 1 || set_failures > set_size) {
    /* invalid value for number of failures within a set, disable this descriptor */
    d->enabled = 0;
    if (scr_my_rank_world == 0) {
      scr_warn("Number of redundancy encodings (%d) must be in the range [1,%d] in redundancy descriptor %d, disabling @ %s:%d",
        set_failures, set_size, d->index, __FILE__, __LINE__
      );
    }
  }

  /* read the redundancy scheme type from the hash */
  char* type;
  d->copy_type = scr_copy_type;
  if (kvtree_util_get_str(hash, SCR_CONFIG_KEY_TYPE, &type) == KVTREE_SUCCESS)
  {
    if (scr_reddesc_type_int_from_str(type, &d->copy_type) != SCR_SUCCESS) {
      /* don't recognize copy type, disable this descriptor */
      d->enabled = 0;
      if (scr_my_rank_world == 0) {
        scr_warn("Unknown copy type %s in redundancy descriptor %d, disabling @ %s:%d",
          type, d->index, __FILE__, __LINE__
        );
      }
    }
  }

  /* CONVENIENCE: if all ranks are on the same node, change
   * type to SINGLE, we do this so single-node jobs can run without
   * requiring the user to change the copy type */
  const scr_groupdesc* node_groupdesc = scr_groupdescs_from_name(SCR_GROUP_NODE);
  if (node_groupdesc != NULL && node_groupdesc->ranks == scr_ranks_world) {
    if (scr_my_rank_world == 0) {
      if (d->copy_type != SCR_COPY_SINGLE) {
        /* print a warning if we changed things on the user */
        scr_dbg(1, "Forcing copy type to SINGLE in redundancy descriptor %d @ %s:%d",
          d->index, __FILE__, __LINE__
        );
      }
    }
    d->copy_type = SCR_COPY_SINGLE;
  }

  // TODO: want to do this?
  /* CONVENIENCE: if writing to a cache location having WORLD access, change
   * type to SINGLE */
  const scr_storedesc* storedesc = scr_reddesc_get_store(d);
  if (storedesc != NULL && storedesc->ranks == scr_ranks_world) {
    if (scr_my_rank_world == 0) {
      if (d->copy_type != SCR_COPY_SINGLE) {
        /* print a warning if we changed things on the user */
        scr_dbg(1, "Forcing copy type to SINGLE in redundancy descriptor %d @ %s:%d",
          d->index, __FILE__, __LINE__
        );
      }
    }
    d->copy_type = SCR_COPY_SINGLE;
  }

  /* ER uses XOR internally when set_failures == 1, so warn user if they also selected RS */
  if (set_failures == 1 && d->copy_type == SCR_COPY_RS) {
    if (scr_my_rank_world == 0) {
      /* print a warning if we changed things on the user */
      scr_dbg(1, "Forcing copy type to XOR since SET_FAILURES=1 in redundancy descriptor %d @ %s:%d",
        d->index, __FILE__, __LINE__
      );
    }
    d->copy_type = SCR_COPY_XOR;
  }

  /* define a string for our failure group, use global rank
   * for leader of group communicator */
  char* failure_domain = NULL;
  if (groupdesc->rank == 0) {
    char rankstr[128];
    snprintf(rankstr, sizeof(rankstr), "%d", scr_my_rank_world);
    failure_domain = strdup(rankstr);
  }
  scr_str_bcast(&failure_domain, 0, groupdesc->comm);

  /* build the communicator based on the copy type
   * and other parameters */
  d->er_scheme = -1;
  switch (d->copy_type) {
  case SCR_COPY_SINGLE:
    d->er_scheme = ER_Create_Scheme(scr_comm_world, failure_domain, scr_ranks_world, 0);
    break;
  case SCR_COPY_PARTNER:
    d->er_scheme = ER_Create_Scheme(scr_comm_world, failure_domain, scr_ranks_world, scr_ranks_world);
    break;
  case SCR_COPY_XOR:
    d->er_scheme = ER_Create_Scheme(scr_comm_world, failure_domain, scr_ranks_world, 1);
    break;
  case SCR_COPY_RS:
    d->er_scheme = ER_Create_Scheme(scr_comm_world, failure_domain, scr_ranks_world, set_failures);
    break;
  }

  /* free failure domain string */
  scr_free(&failure_domain);

  /* disable descriptor if we failed to build a scheme */
  if (d->er_scheme == -1) {
    d->enabled = 0;
  }

  /* if anyone has disabled this, everyone needs to */
  if (! scr_alltrue(d->enabled, scr_comm_world)) {
    d->enabled = 0;
  }

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

/* define prefix to ER files for data files given the hidden dataset directory */
static char* scr_reddesc_prefix(const char* dir)
{
  spath* path = spath_from_str(dir);
  spath_append_str(path, "reddesc");
  char* prefix = spath_strdup(path);
  spath_delete(&path);
  return prefix;
}

/* define prefix to ER files for filemap given the hidden dataset directory */
static char* scr_reddesc_prefix_filemap(const char* dir)
{
  spath* path = spath_from_str(dir);
  spath_append_str(path, "reddescmap");
  char* prefix = spath_strdup(path);
  spath_delete(&path);
  return prefix;
}

static int scr_reddesc_apply_to_filemap(const scr_reddesc* desc, int id, const scr_storedesc* store)
{
  /* define path for hidden directory */
  const char* dir_hidden = scr_cache_dir_hidden_get(desc, id);

  /* define path to er files */
  char* reddesc_dir = scr_reddesc_prefix_filemap(dir_hidden);

  /* create ER set */
  int set_id = ER_Create(scr_comm_world, store->comm, reddesc_dir, ER_DIRECTION_ENCODE, desc->er_scheme);
  if (set_id < 0) {
    scr_err("Failed to create ER set @ %s:%d",
            __FILE__, __LINE__
    );
  }

  /* free directory path strings */
  scr_free(&reddesc_dir);
  scr_free(&dir_hidden);
 
  /* step through each of my files for the specified dataset
   * to scan for any incomplete files */
  int valid = 1;

  /* include filemap as protected file */
  const char* mapfile_str = scr_cache_get_map_file(scr_cindex, id);
  if (ER_Add(set_id, mapfile_str) != ER_SUCCESS) {
    scr_err("Failed to add map file to ER set: %s @ %s:%d", mapfile_str, __FILE__, __LINE__);
    valid = 0;
  }
  scr_free(&mapfile_str);

  /* determine whether everyone's files are good */
  int all_valid = scr_alltrue(valid, scr_comm_world);
  if (! all_valid) {
    if (scr_my_rank_world == 0) {
      scr_dbg(1, "Exiting copy since one or more checkpoint files is invalid");
    }
    ER_Free(set_id);
    return SCR_FAILURE;
  }

  /* apply the redundancy scheme */
  int rc = SCR_SUCCESS;
  if (ER_Dispatch(set_id) != ER_SUCCESS) {
    scr_err("ER_Dispatch failed @ %s:%d", __FILE__, __LINE__);
    rc = SCR_FAILURE;
  }
  if (ER_Wait(set_id) != ER_SUCCESS) {
    scr_err("ER_Wait failed @ %s:%d", __FILE__, __LINE__);
    rc = SCR_FAILURE;
  }
  if (ER_Free(set_id) != ER_SUCCESS) {
    scr_err("ER_Free failed @ %s:%d", __FILE__, __LINE__);
    rc = SCR_FAILURE;
  }

  /* determine whether everyone succeeded in their copy */
  int valid_copy = (rc == SCR_SUCCESS);
  if (! valid_copy) {
    scr_err("scr_copy_files failed with return code %d @ %s:%d",
            rc, __FILE__, __LINE__
    );
  }
  int all_valid_copy = scr_alltrue(valid_copy, scr_comm_world);
  rc = all_valid_copy ? SCR_SUCCESS : SCR_FAILURE;

  return rc;
}

/* apply redundancy scheme to files */
int scr_reddesc_apply(
  scr_filemap* map,
  const scr_reddesc* desc,
  int id)
{
  /* start timer */
  time_t timestamp_start;
  double time_start = 0.0;
  if (scr_my_rank_world == 0) {
    timestamp_start = scr_log_seconds();
    time_start = MPI_Wtime();
  }

  /* step through each of my files for the specified dataset
   * to scan for any incomplete files */
  int valid = 1;
  unsigned long my_counts[3] = {0};
  kvtree_elem* file_elem;
  for (file_elem = scr_filemap_first_file(map);
       file_elem != NULL;
       file_elem = kvtree_elem_next(file_elem))
  {
    /* get the filename */
    char* file = kvtree_elem_key(file_elem);

    /* check the file */
    if (! scr_bool_have_file(map, file)) {
      scr_dbg(2, "File determined to be invalid: %s", file);
      valid = 0;
    }

    /* add up the number of files and bytes on our way through */
    my_counts[0] += 1;
    my_counts[1] += scr_file_size(file);

    /* if crc_on_copy is set, compute crc and update meta file */
    if (scr_crc_on_copy) {
      scr_compute_crc(map, file);
    }
  }

  /* record valid flag, we'll sum these up to determine if all ranks are valid */
  my_counts[2] = valid;

  /* add up total number of files, bytes, and valid flags */
  unsigned long total_counts[3];
  MPI_Allreduce(&my_counts[0], &total_counts[0], 3, MPI_UNSIGNED_LONG, MPI_SUM, scr_comm_world);
  int files       = (int)    total_counts[0];
  double bytes    = (double) total_counts[1];
  int total_valid = (int)    total_counts[2];

  /* determine whether everyone's files are good */
  if (total_valid != scr_ranks_world) {
    if (scr_my_rank_world == 0) {
      scr_dbg(1, "Exiting copy since one or more checkpoint files is invalid");
    }
    return SCR_FAILURE;
  }

  /* get store descriptor for this redudancy scheme */
  scr_storedesc* store = scr_reddesc_get_store(desc);

  /* first encode filemap files, need to capture multi-level storage
   * info (path in cache and path in prefix) in case of a rebuild on scavenge */
  int filemap_rc = scr_reddesc_apply_to_filemap(desc, id, store);
  if (filemap_rc != SCR_SUCCESS) {
    if (scr_my_rank_world == 0) {
      scr_err("Failed to encode filemaps @ %s:%d",
              __FILE__, __LINE__
      );
    }
    return SCR_FAILURE;
  }

  /* assume we'll succeed from this point */
  int rc = SCR_SUCCESS;

  /* we only need to protect the filemap for bypass datasets, so we can skip out early */
  if (desc->bypass) {
    /* we jump to the end to print and log timing info */
    goto print_timing;
  }

  /* define path for hidden directory */
  const char* dir_hidden = scr_cache_dir_hidden_get(desc, id);

  /* define path to er files */
  char* reddesc_dir = scr_reddesc_prefix(dir_hidden);

  /* create ER set */
  int set_id = ER_Create(scr_comm_world, store->comm, reddesc_dir, ER_DIRECTION_ENCODE, desc->er_scheme);
  if (set_id < 0) {
    scr_err("Failed to create ER set @ %s:%d",
            __FILE__, __LINE__
    );
  }

  /* free directory path strings */
  scr_free(&reddesc_dir);
  scr_free(&dir_hidden);
 
  /* step through each of my files for the specified dataset
   * to scan for any incomplete files */
  for (file_elem = scr_filemap_first_file(map);
       file_elem != NULL;
       file_elem = kvtree_elem_next(file_elem))
  {
    /* get the filename */
    char* file = kvtree_elem_key(file_elem);

    /* add file to the set */
    if (ER_Add(set_id, file) != ER_SUCCESS) {
      scr_err("Failed to add file to ER set: %s @ %s:%d", file, __FILE__, __LINE__);
      valid = 0;
    }
  }

#if 0
  /* include filemap as protected file */
  const char* mapfile_str = scr_cache_get_map_file(scr_cindex, id);
  if (ER_Add(set_id, mapfile_str) != ER_SUCCESS) {
    scr_err("Failed to add map file to ER set: %s @ %s:%d", mapfile_str, __FILE__, __LINE__);
    valid = 0;
  }
  scr_free(&mapfile_str);
#endif

  /* determine whether everyone's files are good */
  int all_valid = scr_alltrue(valid, scr_comm_world);
  if (! all_valid) {
    if (scr_my_rank_world == 0) {
      scr_dbg(1, "Exiting copy since one or more checkpoint files is invalid");
    }
    ER_Free(set_id);
    return SCR_FAILURE;
  }

  /* apply the redundancy scheme */
  if (ER_Dispatch(set_id) != ER_SUCCESS) {
    scr_err("ER_Dispatch failed @ %s:%d", __FILE__, __LINE__);
    rc = SCR_FAILURE;
  }
  if (ER_Wait(set_id) != ER_SUCCESS) {
    scr_err("ER_Wait failed @ %s:%d", __FILE__, __LINE__);
    rc = SCR_FAILURE;
  }
  if (ER_Free(set_id) != ER_SUCCESS) {
    scr_err("ER_Free failed @ %s:%d", __FILE__, __LINE__);
    rc = SCR_FAILURE;
  }

  /* determine whether everyone succeeded in their copy */
  int valid_copy = (rc == SCR_SUCCESS);
  if (! valid_copy) {
    scr_err("scr_copy_files failed with return code %d @ %s:%d",
            rc, __FILE__, __LINE__
    );
  }
  int all_valid_copy = scr_alltrue(valid_copy, scr_comm_world);
  rc = all_valid_copy ? SCR_SUCCESS : SCR_FAILURE;

print_timing:
  /* stop timer and report performance info */
  if (scr_my_rank_world == 0) {
    double time_end = MPI_Wtime();
    double time_diff = time_end - time_start;
    double bw = 0.0;
    if (time_diff > 0.0) {
      bw = bytes / (1024.0 * 1024.0 * time_diff);
    }
    scr_dbg(1, "scr_reddesc_apply: %f secs, %d files, %e bytes, %f MB/s, %f MB/s per proc",
            time_diff, files, bytes, bw, bw/scr_ranks_world
    );

    /* log data on the copy in the database */
    if (scr_log_enable) {
      char* dir = scr_cache_dir_get(desc, id);
      scr_log_transfer("ENCODE", desc->base, dir, &id, NULL, &timestamp_start, &time_diff, &bytes, &files);
      scr_free(&dir);
    }
  }

  return rc;
}

static int scr_reddesc_er_recover(MPI_Comm comm, const char* name)
{
  int rc = SCR_SUCCESS;

  /* create ER set */
  int set_id = ER_Create(scr_comm_world, comm, name, ER_DIRECTION_REBUILD, 0);

  if (set_id >= 0) {
    if (ER_Dispatch(set_id) != ER_SUCCESS) {
      rc = SCR_FAILURE;
    }

    if (ER_Wait(set_id) != ER_SUCCESS) {
      rc = SCR_FAILURE;
    }

    ER_Free(set_id);
  } else {
    rc = SCR_FAILURE;
  }

  return rc;
}

/* rebuilds files for specified dataset id using specified redundancy descriptor,
 * adds them to filemap, and returns SCR_SUCCESS if all processes succeeded */
int scr_reddesc_recover(scr_cache_index* cindex, int id, const char* dir)
{
  int rc = SCR_SUCCESS;

  /* get store descriptor for this redudancy scheme */
  int store_index = scr_storedescs_index_from_child_path(dir);

  /* TODO: verify that everyone found a matching store descriptor */
  scr_storedesc* store = &scr_storedescs[store_index];

  /* recover filemap files */
  char* reddesc_filemap = scr_reddesc_prefix_filemap(dir);
  if (scr_reddesc_er_recover(store->comm, reddesc_filemap) != SCR_SUCCESS) {
    rc = SCR_FAILURE;
  }
  scr_free(&reddesc_filemap);

  /* if dataset was a cache bypass, we stop after recovering the filemap,
   * since no redundancy was applied to data files in prefix directory */
  int bypass;
  scr_cache_index_get_bypass(cindex, id, &bypass);
  if (bypass) {
    /* we don't have to rebuild files for bypass,
     * but we check that they exist and match meta data */
    if (rc == SCR_SUCCESS) {
      /* get the filemap for this dataset */
      scr_filemap* map = scr_filemap_new();
      scr_cache_get_map(cindex, id, map);

      /* step through each of my files for the specified dataset
       * to scan for any incomplete files */
      kvtree_elem* file_elem;
      for (file_elem = scr_filemap_first_file(map);
           file_elem != NULL;
           file_elem = kvtree_elem_next(file_elem))
      {
        /* get the filename */
        char* file = kvtree_elem_key(file_elem);

        /* check the file */
        if (! scr_bool_have_file(map, file)) {
          scr_dbg(2, "File determined to be invalid: %s", file);
          rc = SCR_FAILURE;
        }
      }

      /* free the map */
      scr_filemap_delete(&map);
    }

    /* check whether all ranks successfully found their files */
    if (! scr_alltrue(rc == SCR_SUCCESS, scr_comm_world)) {
      /* someone failed, so everyone fails */
      rc = SCR_FAILURE;
    }
    return rc;
  }

  /* recover data files */
  char* reddesc_data = scr_reddesc_prefix(dir);
  if (scr_reddesc_er_recover(store->comm, reddesc_data) != SCR_SUCCESS) {
    rc = SCR_FAILURE;
  }
  scr_free(&reddesc_data);

  return rc;
}

static int scr_reddesc_er_unapply(MPI_Comm comm, const char* name)
{
  int rc = SCR_SUCCESS;

  /* create ER set */
  int set_id = ER_Create(scr_comm_world, comm, name, ER_DIRECTION_REMOVE, 0);

  if (set_id >= 0) {
    if (ER_Dispatch(set_id) != ER_SUCCESS) {
      rc = SCR_FAILURE;
    }

    if (ER_Wait(set_id) != ER_SUCCESS) {
      rc = SCR_FAILURE;
    }

    ER_Free(set_id);
  } else {
    rc = SCR_FAILURE;
  }

  return rc;
}

/* remove redundancy files added during scr_reddesc_apply */
int scr_reddesc_unapply(const scr_cache_index* cindex, int id, const char* dir)
{
  int rc = SCR_SUCCESS;

  /* get store descriptor for this redudancy scheme */
  int store_index = scr_storedescs_index_from_child_path(dir);

  /* TODO: verify that everyone found a matching store descriptor */
  scr_storedesc* store = &scr_storedescs[store_index];

  /* delete redundancy data for filemap files */
  char* reddesc_filemap = scr_reddesc_prefix_filemap(dir);
  if (scr_reddesc_er_unapply(store->comm, reddesc_filemap) != SCR_SUCCESS) {
    rc = SCR_FAILURE;
  }
  scr_free(&reddesc_filemap);

  /* if dataset was a cache bypass, we stop after removing redudancy for the filemap,
   * since no redundancy was applied to data files in prefix directory */
  int bypass;
  scr_cache_index_get_bypass(cindex, id, &bypass);
  if (bypass) {
    return rc;
  }

  /* delete redundancy data for data files */
  char* reddesc_data = scr_reddesc_prefix(dir);
  if (scr_reddesc_er_unapply(store->comm, reddesc_data) != SCR_SUCCESS) {
    rc = SCR_FAILURE;
  }
  scr_free(&reddesc_data);

  return rc;
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
  kvtree* descs = kvtree_get(scr_reddesc_hash, SCR_CONFIG_KEY_CKPTDESC);
  if (descs != NULL) {
    scr_nreddescs = kvtree_size(descs);
  }

  /* allocate our redundancy descriptors */
  scr_reddescs = (scr_reddesc*) SCR_MALLOC(scr_nreddescs * sizeof(scr_reddesc));

  /* flag to indicate whether we successfully build all redundancy
   * descriptors */
  int all_valid = 1;

  /* sort the hash to ensure we step through all elements in the same
   * order on all procs */
  kvtree_sort(descs, KVTREE_SORT_ASCENDING);

  /* iterate over each of our hash entries filling in each
   * corresponding descriptor, have rank 0 determine the
   * order in which we'll create the descriptors */
  int index = 0;
  kvtree_elem* elem;
  for (elem = kvtree_elem_first(descs);
       elem != NULL;
       elem = kvtree_elem_next(elem))
  {
    /* select redundancy descriptor name on rank 0 */
    char* name = kvtree_elem_key(elem);

    /* get the info hash for this descriptor */
    kvtree* hash = kvtree_get(descs, name);

    /* create descriptor */
    if (scr_reddesc_create_from_hash(&scr_reddescs[index], index, hash)
        != SCR_SUCCESS)
    {
      if (scr_my_rank_world == 0) {
        scr_err("Failed to set up %s=%s @ %s:%f",
          SCR_CONFIG_KEY_CKPTDESC, name, __FILE__, __LINE__
        );
      }
      all_valid = 0;
    }

    /* advance to our next descriptor */
    index++;
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
