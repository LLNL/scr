/* Implements some common tasks for operations on kvtrees */

#include "kvtree.h"
#include "kvtree_util.h"
#include "kvtree_helpers.h"
#include "kvtree_err.h"

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <stdint.h>

/* need at least version 8.5 of queue.h from Berkeley */
#include "queue.h"

int kvtree_util_set_bytecount(kvtree* hash, const char* key, unsigned long count)
{
  /* first, unset any current setting */
  kvtree_unset(hash, key);

  /* then set the new value */
  kvtree_setf(hash, NULL, "%s %lu", key, count);

  return KVTREE_SUCCESS;
}

int kvtree_util_set_crc32(kvtree* hash, const char* key, uLong crc)
{
  /* first, unset any current setting */
  kvtree_unset(hash, key);

  /* then set the new value */
  kvtree_setf(hash, NULL, "%s %#x", key, (uint32_t) crc);

  return KVTREE_SUCCESS;
}

int kvtree_util_set_int(kvtree* hash, const char* key, int value)
{
  /* first, unset any current setting */
  kvtree_unset(hash, key);

  /* then set the new value */
  kvtree_set_kv_int(hash, key, value);

  return KVTREE_SUCCESS;
}

int kvtree_util_set_unsigned_long(kvtree* hash, const char* key, unsigned long value)
{
  /* first, unset any current setting */
  kvtree_unset(hash, key);

  /* then set the new value */
  kvtree_setf(hash, NULL, "%s %lu", key, value);

  return KVTREE_SUCCESS;
}

int kvtree_util_set_str(kvtree* hash, const char* key, const char* value)
{
  /* first, unset any current setting */
  kvtree_unset(hash, key);

  /* then set the new value */
  kvtree_set_kv(hash, key, value);

  return KVTREE_SUCCESS;
}

int kvtree_util_set_int64(kvtree* hash, const char* key, int64_t value)
{
  /* first, unset any current setting */
  kvtree_unset(hash, key);

  /* then set the new value */
  kvtree_setf(hash, NULL, "%s %lld", key, value);

  return KVTREE_SUCCESS;
}

int kvtree_util_set_double(kvtree* hash, const char* key, double value)
{
  /* first, unset any current setting */
  kvtree_unset(hash, key);

  /* then set the new value */
  kvtree_setf(hash, NULL, "%s %f", key, value);

  return KVTREE_SUCCESS;
}

int kvtree_util_set_ptr(kvtree* hash, const char* key, void* value)
{
  /* first, unset any current setting */
  kvtree_unset(hash, key);

  /* then set the new value */
  kvtree_setf(hash, NULL, "%s %p", key, value);

  return KVTREE_SUCCESS;
}

int kvtree_util_get_bytecount(const kvtree* hash, const char* key, unsigned long* val)
{
  int rc = KVTREE_FAILURE;

  /* check whether this key is even set */
  char* val_str = kvtree_get_val(hash, key);
  if (val_str != NULL) {
    /* convert the key string */
    *val = strtoul(val_str, NULL, 0);
    rc = KVTREE_SUCCESS;
  }

  return rc;
}

int kvtree_util_get_crc32(const kvtree* hash, const char* key, uLong* val)
{
  int rc = KVTREE_FAILURE;

  /* check whether this key is even set */
  char* val_str = kvtree_get_val(hash, key);
  if (val_str != NULL) {
    /* convert the key string */
    *val = (uLong) strtoul(val_str, NULL, 0);
    rc = KVTREE_SUCCESS;
  }

  return rc;
}

int kvtree_util_get_int(const kvtree* hash, const char* key, int* value)
{
  int rc = KVTREE_FAILURE;

  char* val_str = kvtree_elem_get_first_val(hash, key);
  if (val_str != NULL) {
    *value = atoi(val_str);
    return KVTREE_SUCCESS;
  }

  return rc;
}

int kvtree_util_get_unsigned_long(const kvtree* hash, const char* key, unsigned long* val)
{
  int rc = KVTREE_FAILURE;

  /* check whether this key is even set */
  char* val_str = kvtree_get_val(hash, key);
  if (val_str != NULL) {
    /* convert the key string */
    *val = strtoul(val_str, NULL, 0);
    rc = KVTREE_SUCCESS;
  }

  return rc;
}

int kvtree_util_get_str(const kvtree* hash, const char* key, char** val)
{
  int rc = KVTREE_FAILURE;

  /* check whether this key is even set */
  char* val_str = kvtree_get_val(hash, key);
  if (val_str != NULL) {
    /* convert the key string */
    *val = val_str;
    rc = KVTREE_SUCCESS;
  }

  return rc;
}

int kvtree_util_get_int64(const kvtree* hash, const char* key, int64_t* val)
{
  int rc = KVTREE_FAILURE;

  /* check whether this key is even set */
  char* val_str = kvtree_get_val(hash, key);
  if (val_str != NULL) {
    /* convert the key string */
    *val = (int64_t) strtoll(val_str, NULL, 0);
    rc = KVTREE_SUCCESS;
  }

  return rc;
}

int kvtree_util_get_double(const kvtree* hash, const char* key, double* val)
{
  int rc = KVTREE_FAILURE;

  /* check whether this key is even set */
  char* val_str = kvtree_get_val(hash, key);
  if (val_str != NULL) {
    /* convert the key string */
    double val_tmp;
    if (kvtree_atod(val_str, &val_tmp) == KVTREE_SUCCESS) {
      *val = val_tmp;
      rc = KVTREE_SUCCESS;
    }
  }

  return rc;
}

int kvtree_util_get_ptr(const kvtree* hash, const char* key, void** val)
{
  int rc = KVTREE_FAILURE;

  /* check whether this key is even set */
  char* val_str = kvtree_get_val(hash, key);
  if (val_str != NULL) {
    /* convert the key string */
    void* val_tmp;
    int sscanf_rc = sscanf(val_str, "%p", &val_tmp);
    if (sscanf_rc == 1) {
      *val = val_tmp;
      rc = KVTREE_SUCCESS;
    }
  }

  return rc;
}
