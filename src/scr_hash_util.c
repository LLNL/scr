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

/* Implements some common tasks for SCR operations on hashes */

#include "scr.h"
#include "scr_hash.h"
#include "scr_hash_util.h"

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdint.h>

int scr_hash_util_set_bytecount(scr_hash* hash, const char* key, unsigned long count)
{
  /* first, unset any current setting */
  scr_hash_unset(hash, key);

  /* then set the new value */
  scr_hash* hash2 = scr_hash_setf(hash, NULL, "%s %llu", key, count);

  /* if there wasn't a hash, return failure */
  if (hash2 == NULL) {
    return SCR_FAILURE;
  }
  return SCR_SUCCESS;
}

int scr_hash_util_set_crc32(scr_hash* hash, const char* key, uLong crc)
{
  /* first, unset any current setting */
  scr_hash_unset(hash, key);

  /* then set the new value */
  scr_hash* hash2 = scr_hash_setf(hash, NULL, "%s %#x", key, (uint32_t) crc);

  /* if there wasn't a hash, return failure */
  if (hash2 == NULL) {
    return SCR_FAILURE;
  }
  return SCR_SUCCESS;
}

int scr_hash_util_get_bytecount(const scr_hash* hash, const char* key, unsigned long* val)
{
  int rc = SCR_FAILURE;

  /* check whether this key is even set */
  char* val_str = scr_hash_get_val(hash, key);
  if (val_str != NULL) {
    /* convert the key string */
    *val = strtoul(val_str, NULL, 0);
    rc = SCR_SUCCESS;
  }

  return rc;
}

int scr_hash_util_get_crc32(const scr_hash* hash, const char* key, uLong* val)
{
  int rc = SCR_FAILURE;

  /* check whether this key is even set */
  char* val_str = scr_hash_get_val(hash, key);
  if (val_str != NULL) {
    /* convert the key string */
    *val = (uLong) strtoul(val_str, NULL, 0);
    rc = SCR_SUCCESS;
  }

  return rc;
}

int scr_hash_util_get_int(const scr_hash* hash, const char* key, int* value)
{
  int rc = SCR_FAILURE;

  char* val_str = scr_hash_elem_get_first_val(hash, key);
  if (val_str != NULL) {
    *value = atoi(val_str);
    return SCR_SUCCESS;
  }

  return rc;
}

int scr_hash_util_get_unsigned_long(const scr_hash* hash, const char* key, unsigned long* val)
{
  int rc = SCR_FAILURE;

  /* check whether this key is even set */
  char* val_str = scr_hash_get_val(hash, key);
  if (val_str != NULL) {
    /* convert the key string */
    *val = strtoul(val_str, NULL, 0);
    rc = SCR_SUCCESS;
  }

  return rc;
}


