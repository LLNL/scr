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
#include "kvtree.h"
#include "kvtree_util.h"
#include "spath.h"
#include "scr_config.h"
#include "scr_param.h"

#include <stdlib.h>
#include <stdio.h>
#include <ctype.h>
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
static kvtree* scr_no_user_hash = NULL;

/* this data structure will hold values read from the user config file */
static kvtree* scr_user_hash = NULL;

/* this data structure will hold values read from the system config file */
static kvtree* scr_system_hash = NULL;

/* caches lookups via getenv, need to do this on some systems because returning
 * pointers to getenv values back too many functions was segfaulting on some
 * systems */
static kvtree* scr_env_hash = NULL;


/* expand environment variables in parameter value */
char* expand_env(const char* value)
{
  ssize_t len = (ssize_t)strlen(value) + 1;
  ssize_t rlen = len;
  char* retval = malloc(rlen);
  assert(retval); /* TODO: should I check for running out of memory? */
  for(int i = 0, j = 0 ; i < len ; /*nop*/) {
    if(value[i] == '$') {
      /* try and extract an environment variable name the '$' */
      int envbegin = -1, envend = -1;
      if(i+1 < len && value[i+1] == '{') {
        /* look for ${...} construct */
        envbegin = i+1;
        envend = envbegin + 1;
        /* extract variable name in {...} */
        while(envend < len && value[envend] != '}' &&
              (isalnum(value[envend]) || value[envend] == '_')) {
          envend += 1;
        }
      } else if(i+1 < len && (isalnum(value[i+1]) || value[i+1] == '_')) {
        /* look for $... construct */
        envbegin = i+1;
        envend = envbegin + 1;
        /* extract variable name in {...} */
        while(envend < len && (isalnum(value[envend]) || value[envend] == '_')) {
          envend += 1;
        }
      }

      if(envbegin >= 0 && envend >= 0 && envend < len &&
         (value[envbegin] == '{') == (value[envend] == '}')) {
        /* found a begin, and an end, end is not past the end of the string,
         * and '{' and '}' are balanced */

        /* extract name */
        const int envnamelen = envend - envbegin;
        char envname[envnamelen + 1];
        assert(envbegin + envnamelen <= len);
        memcpy(envname, &value[envbegin], envnamelen);
        envname[envnamelen] = '\0';

        /* get value, for ${...} we copied the initial '{' into the name, skip
         * it here */
        const char* env = getenv(envname + (value[envbegin]=='{'));
        if(env == NULL)
          env = "";

        /* resize retval to make room for expanded value */
        const int envlen = strlen(env);
        rlen += envlen - (envnamelen + 1);
        retval = realloc(retval, rlen);
        assert(retval); /* TODO: should I check for running out of memory? */

        /* finally copy value into retval */
        for(int envi = 0 ; envi < envlen ; /*nop*/) {
          assert(j < rlen);
          assert(envi < envlen);
          retval[j++] = env[envi++];
        }

        /* account for '}' at end */
        i += 1 + envnamelen + (value[envend]=='}');


        assert(i <= len);
      } else {
        /* invalid name of some sort, copy it to output string */
        assert(j < rlen);
        assert(i < len);
        retval[j++] = value[i++];
      }
    } else {
      /* regular character */
      assert(j < rlen);
      assert(i < len);
      retval[j++] = value[i++];
    }
  } /* for i */

  return retval;
}

/* searches for name and returns a character pointer to its value if set,
 * returns NULL if not found */
const char* scr_param_get(const char* name)
{
  char* value = NULL;

  /* see if this parameter is one which is restricted from user */
  kvtree* no_user = kvtree_get(scr_no_user_hash, name);

  /* if parameter is set in environment, return that value */
  if (no_user == NULL && getenv(name) != NULL) {
    /* we don't just return the getenv value directly because that causes
     * segfaults on some systems, so instead we add it to a hash and return
     * the pointer into the hash */

    /* try to lookup the value for this name in case we've already cached it */
    if (kvtree_util_get_str(scr_env_hash, name, &value) != KVTREE_SUCCESS) {
      /* it's not in the hash yet, so add it */
      char* tmp_value = expand_env(getenv(name));
      kvtree_util_set_str(scr_env_hash, name, tmp_value);
      scr_free(&tmp_value);

      /* now issue our lookup again */
      if (kvtree_util_get_str(scr_env_hash, name, &value) != KVTREE_SUCCESS) {
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
  value = kvtree_elem_get_first_val(scr_user_hash, name);
  if (no_user == NULL && value != NULL) {
    /* evaluate environment variables */
    if(strchr(value, '$')){
      value = expand_env(value);
    }
    return value;
  }

  /* otherwise, if parameter is set in system configuration file,
   * return that value */
  value = kvtree_elem_get_first_val(scr_system_hash, name);
  if (value != NULL) {
    /* evaluate environment variables */
    if(strchr(value, '$')){
      value = expand_env(value);
    }
    return value;
  }

  /* parameter not found, return NULL */
  return NULL;
}

/* searchs for name and returns a newly allocated hash of its value if set,
 * returns NULL if not found */
const kvtree* scr_param_get_hash(const char* name)
{
  kvtree* hash = NULL;
  kvtree* value_hash = NULL;

  /* see if this parameter is one which is restricted from user */
  kvtree* no_user = kvtree_get(scr_no_user_hash, name);

  /* if parameter is set in environment, return that value */
  if (no_user == NULL && getenv(name) != NULL) {
    /* TODO: need to strdup here to be safe? */
    hash = kvtree_new();
    char* tmp_value = expand_env(getenv(name));
    kvtree_set(hash, tmp_value, kvtree_new());
    scr_free(&tmp_value);
    return hash;
  }

  /* otherwise, if parameter is set in user configuration file,
   * return that value */
  value_hash = kvtree_get(scr_user_hash, name);
  if (no_user == NULL && value_hash != NULL) {
    /* walk value_hash and evaluate environment variables */
    kvtree_elem* elem;
    for (elem = kvtree_elem_first(value_hash);
	 elem != NULL;
	 elem = kvtree_elem_next(elem)){
      char* value = kvtree_elem_key(elem);
      if(strchr(value, '$')){
        value = expand_env(value);
	elem->key = value;
      }
    }

    /* return the resulting hash */
    hash = kvtree_new();
    kvtree_merge(hash, value_hash);
    return hash;
  }

  /* otherwise, if parameter is set in system configuration file,
   * return that value */
  value_hash = kvtree_get(scr_system_hash, name);
  if (value_hash != NULL) {
    /* walk value_hash and evaluate environment variables */
    kvtree_elem* elem;
    for (elem = kvtree_elem_first(value_hash);
	 elem != NULL;
	 elem = kvtree_elem_next(elem)){
      char* value = kvtree_elem_key(elem);
      if(strchr(value, '$')){
        value = expand_env(value);
	elem->key = value;
      }
    }

    /* return the resulting hash */
    hash = kvtree_new();
    kvtree_merge(hash, value_hash);
    return hash;
  }

  /* parameter not found, return NULL */
  return NULL;
}

/* allocates a new string (to be freed with scr_free)
 * that is path to user config file */
static char* user_config_path()
{
  char* file = NULL;

  /* first, use SCR_CONF_FILE if it's set */
  char* value = getenv("SCR_CONF_FILE");
  if (value != NULL) {
    file = strdup(value);
    return file;
  }

  /* otherwise, look in the prefix directory */
  char* prefix = NULL;
  value = getenv("SCR_PREFIX");
  if (value != NULL) {
    /* user set SCR_PREFIX, strdup that value */
    prefix = strdup(value);
  } else {
    /* if user didn't set with SCR_PREFIX,
     * pick up the current working directory as a default */
    char current_dir[SCR_MAX_FILENAME];
    if (scr_getcwd(current_dir, sizeof(current_dir)) != SCR_SUCCESS) {
      scr_abort(-1, "Problem reading current working directory @ %s:%d",
        __FILE__, __LINE__
      );
    }
    prefix = strdup(current_dir);
  }

  /* couldn't find a prefix directory, so bail */
  if (prefix == NULL) {
    return file;
  }

  /* tack file name on to directory */
  spath* prefix_path = spath_from_str(prefix);
  spath_append_str(prefix_path, SCR_CONFIG_FILE_USER);
  file = spath_strdup(prefix_path);
  spath_delete(&prefix_path);

  /* free the prefix dir which we strdup'd */
  scr_free(&prefix);

  return file;
}

/* read config files and store contents */
int scr_param_init()
{
  /* allocate storage and read in config files if we haven't already */
  if (scr_param_ref_count == 0) {
    /* allocate hash object to hold names we cannot read from the
     * environment */
    scr_no_user_hash = kvtree_new();
    kvtree_set(scr_no_user_hash, "SCR_CNTL_BASE", kvtree_new());

    /* allocate hash object to store values from user config file,
     * if specified */
    char* user_file = user_config_path();
    if (user_file != NULL) {
      scr_user_hash = kvtree_new();
      scr_config_read(user_file, scr_user_hash);
    }
    scr_free(&user_file);

    /* allocate hash object to store values from system config file */
    scr_system_hash = kvtree_new();
    scr_config_read(scr_config_file, scr_system_hash);

    /* initialize our hash to cache lookups to getenv */
    scr_env_hash = kvtree_new();

    /* warn user if they set any parameters in their environment or user
     * config file which aren't permitted */
    kvtree_elem* elem;
    for (elem = kvtree_elem_first(scr_no_user_hash);
         elem != NULL;
         elem = kvtree_elem_next(elem))
    {
      /* get the parameter name */
      char* key = kvtree_elem_key(elem);

      char* env_val = getenv(key);
      kvtree* env_hash = kvtree_get(scr_user_hash, key);

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
    kvtree_delete(&scr_user_hash);

    /* free our parameter hash */
    kvtree_delete(&scr_system_hash);

    /* free our env hash */
    kvtree_delete(&scr_env_hash);

    /* free the hash listing parameters user cannot set */
    kvtree_delete(&scr_no_user_hash);
  }

  return SCR_SUCCESS;
}
