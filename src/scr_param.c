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

/* Reads parameters from environment and configuration files */

#include "scr_conf.h"
#include "scr.h"
#include "scr_util.h"
#include "scr_err.h"
#include "scr_io.h"
#include "scr_hash.h"
#include "scr_hash_util.h"
#include "scr_config.h"
#include "scr_param.h"

#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <strings.h>

/* variable length args */
#include <stdarg.h>
#include <errno.h>

/* since multiple objects may require parameters, and thus init and finalize,
 * we keep a reference so we don't clear the data structures until all modules
 * which currently are using the object have finished */
static int scr_param_ref_count = 0;

/* name of system configuration file we should read */
static char scr_config_file[SCR_MAX_FILENAME] = SCR_CONFIG_FILE;

/* this data structure holds variables names which we can't lookup in
 * environment */
static scr_hash* scr_no_user_hash = NULL;

/* this data structure will hold values read from the user config file */
static scr_hash* scr_user_hash = NULL;

/* this data structure will hold values read from the system config file */
static scr_hash* scr_system_hash = NULL;

/* caches lookups via getenv, need to do this on some systems because returning
 * pointers to getenv values back too many functions was segfaulting on some
 * systems */
static scr_hash* scr_env_hash = NULL;

/* searches for name and returns a character pointer to its value if set,
 * returns NULL if not found */
char* scr_param_get(char* name)
{
  char* value = NULL;

  /* see if this parameter is one which is restricted from user */
  scr_hash* no_user = scr_hash_get(scr_no_user_hash, name);

  /* if parameter is set in environment, return that value */
  if (no_user == NULL && getenv(name) != NULL) {
    /* we don't just return the getenv value directly because that causes
     * segfaults on some systems, so instead we add it to a hash and return
     * the pointer into the hash */

    /* try to lookup the value for this name in case we've already cached it */
    if (scr_hash_util_get_str(scr_env_hash, name, &value) != SCR_SUCCESS) {
      /* it's not in the hash yet, so add it */
      char* tmp_value = strdup(getenv(name));
      scr_hash_util_set_str(scr_env_hash, name, tmp_value);
      scr_free(&tmp_value);

      /* now issue our lookup again */
      if (scr_hash_util_get_str(scr_env_hash, name, &value) != SCR_SUCCESS) {
        /* it's an error if we don't find it this time */
        scr_abort(-1, "Failed to find value for %s in env hash @ %s:%d",
          name, __FILE__, __LINE__
        );
      }
    }
    
    return value;
  }

  /* otherwise, if parameter is set in user configuration file,
   * return that value */
  value = scr_hash_elem_get_first_val(scr_user_hash, name);
  if (no_user == NULL && value != NULL) {
    return value;
  }

  /* otherwise, if parameter is set in system configuration file,
   * return that value */
  value = scr_hash_elem_get_first_val(scr_system_hash, name);
  if (value != NULL) {
    return value;
  }

  /* parameter not found, return NULL */
  return NULL;
}

/* searchs for name and returns a newly allocated hash of its value if set,
 * returns NULL if not found */
scr_hash* scr_param_get_hash(char* name)
{
  scr_hash* hash = NULL;
  scr_hash* value_hash = NULL;

  /* see if this parameter is one which is restricted from user */
  scr_hash* no_user = scr_hash_get(scr_no_user_hash, name);

  /* if parameter is set in environment, return that value */
  if (no_user == NULL && getenv(name) != NULL) {
    /* TODO: need to strdup here to be safe? */
    hash = scr_hash_new();
    char* tmp_value = strdup(getenv(name));
    scr_hash_set(hash, tmp_value, scr_hash_new());
    scr_free(&tmp_value);
    return hash;
  }

  /* otherwise, if parameter is set in user configuration file,
   * return that value */
  value_hash = scr_hash_get(scr_user_hash, name);
  if (no_user == NULL && value_hash != NULL) {
    hash = scr_hash_new();
    scr_hash_merge(hash, value_hash);
    return hash;
  }

  /* otherwise, if parameter is set in system configuration file,
   * return that value */
  value_hash = scr_hash_get(scr_system_hash, name);
  if (value_hash != NULL) {
    hash = scr_hash_new();
    scr_hash_merge(hash, value_hash);
    return hash;
  }

  /* parameter not found, return NULL */
  return NULL;
}

/* read config files and store contents */
int scr_param_init()
{
  /* allocate storage and read in config files if we haven't already */
  if (scr_param_ref_count == 0) {
    /* allocate hash object to hold names we cannot read from the
     * environment */
    scr_no_user_hash = scr_hash_new();
    scr_hash_set(scr_no_user_hash, "SCR_CNTL_BASE", scr_hash_new());

    /* allocate hash object to store values from user config file,
     * if specified */
    char* user_file = getenv("SCR_CONF_FILE");
    if (user_file != NULL) {
      scr_user_hash = scr_hash_new();
      scr_config_read(user_file, scr_user_hash);
    }

    /* allocate hash object to store values from system config file */
    scr_system_hash = scr_hash_new();
    scr_config_read(scr_config_file, scr_system_hash);

    /* initialize our hash to cache lookups to getenv */
    scr_env_hash = scr_hash_new();

    /* warn user if he set any parameters in his environment or user
     * config file which aren't permitted */
    scr_hash_elem* elem;
    for (elem = scr_hash_elem_first(scr_no_user_hash);
         elem != NULL;
         elem = scr_hash_elem_next(elem))
    {
      /* get the parameter name */
      char* key = scr_hash_elem_key(elem);

      char* env_val = getenv(key);
      scr_hash* env_hash = scr_hash_get(scr_user_hash, key);

      /* check whether this is set in the environment */
      if (env_val != NULL || env_hash != NULL) {
        scr_err("%s cannot be set in the environment or user configuration file, ignoring setting",
          key
        );
      }
    }
  }

  /* increment our reference count */
  scr_param_ref_count++;

  return SCR_SUCCESS;
}

/* free contents from config files */
int scr_param_finalize()
{
  /* decrement our reference count */
  scr_param_ref_count--;

  /* if the reference count is zero, free the data structures */
  if (scr_param_ref_count == 0) {
    /* free our parameter hash */
    scr_hash_delete(&scr_user_hash);

    /* free our parameter hash */
    scr_hash_delete(&scr_system_hash);

    /* free our env hash */
    scr_hash_delete(&scr_env_hash);

    /* free the hash listing parameters user cannot set */
    scr_hash_delete(&scr_no_user_hash);
  }

  return SCR_SUCCESS;
}
