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

/* Defines a recursive hash data structure, where at the top level,
 * there is a list of elements indexed by string.  Each
 * of these elements in turn consists of a list of elements
 * indexed by string, and so on. */

#include "scr.h"
#include "scr_err.h"
#include "scr_hash.h"
#include "scr_io.h"
#include "scr_util.h"
#include "scr_path.h"

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>

/* need at least version 8.5 of queue.h from Berkeley */
#include "queue.h"

#include <stdint.h>

#define SCR_FILE_MAGIC          (0x951fc3f5)
#define SCR_FILE_TYPE_HASH      (1)
#define SCR_FILE_VERSION_HASH_1 (1)

#define SCR_FILE_HASH_HEADER_SIZE (20)
#define SCR_FILE_FLAGS_CRC32 (0x1) /* indicates that crc32 is stored at end of file */

/*
=========================================
Allocate and delete hash objects
=========================================
*/

/* allocates a new hash element */
static scr_hash_elem* scr_hash_elem_new()
{
  scr_hash_elem* elem = (scr_hash_elem*) malloc(sizeof(scr_hash_elem));
  if (elem != NULL) {
    elem->key  = NULL;
    elem->hash = NULL;
  } else {
    scr_abort(-1, "Failed to allocate memory for hash element @ %s:%d",
      __FILE__, __LINE__
    );
  }
  return elem;
}

/* frees a hash element */
static int scr_hash_elem_delete(scr_hash_elem* elem)
{
  if (elem != NULL) {
    /* free the key which was strdup'ed */
    scr_free(&(elem->key));

    /* free the hash */
    scr_hash_delete(&elem->hash);
    elem->hash = NULL;

    /* finally, free the element structure itself */
    scr_free(&elem);
  } 
  return SCR_SUCCESS;
}

/* allocates a new hash */
scr_hash* scr_hash_new()
{
  scr_hash* hash = (scr_hash*) malloc(sizeof(scr_hash));
  if (hash != NULL) {
    LIST_INIT(hash);
  } else {
    scr_abort(-1, "Failed to allocate memory for hash object @ %s:%d",
      __FILE__, __LINE__
    );
  }
  return hash;
}

/* frees a hash */
int scr_hash_delete(scr_hash** ptr_hash)
{
  if (ptr_hash != NULL) {
    scr_hash* hash = *ptr_hash;
    if (hash != NULL) {
      while (!LIST_EMPTY(hash)) {
        scr_hash_elem* elem = LIST_FIRST(hash);
        LIST_REMOVE(elem, pointers);
        scr_hash_elem_delete(elem);
      }
      scr_free(ptr_hash);
    }
  }
  return SCR_SUCCESS;
}

/*
=========================================
size, get, set, unset, and merge functions
=========================================
*/

/* given an element, set its key and hash fields */
static scr_hash_elem* scr_hash_elem_init(scr_hash_elem* elem, const char* key, scr_hash* hash)
{
  if (elem != NULL) {
    if (key != NULL) {
      elem->key = strdup(key);
    } else {
      /* bad idea to allow key to be set to NULL */
      elem->key = NULL;
      scr_err("Setting hash element key to NULL @ %s:%d",
        __FILE__, __LINE__
      );
    }
    elem->hash = hash;
  }
  return elem;
}

/* return size of hash (number of keys) */
int scr_hash_size(const scr_hash* hash)
{
  int count = 0;
  if (hash != NULL) {
    scr_hash_elem* elem;
    LIST_FOREACH(elem, hash, pointers) {
      count++;
    }
  }
  return count;
}

/* given a hash and a key, return the hash associated with key,
 * returns NULL if not found */
scr_hash* scr_hash_get(const scr_hash* hash, const char* key)
{
  scr_hash_elem* elem = scr_hash_elem_get(hash, key);
  if (elem != NULL) {
    return elem->hash;
  }
  return NULL;
}

/* given a hash, a key, and a hash value, set (or reset) the key's
 * hash and return the pointer to the new hash */
scr_hash* scr_hash_set(scr_hash* hash, const char* key, scr_hash* hash_value)
{
  /* check that we have a valid hash to insert into and a valid key
   * name */
  if (hash == NULL || key == NULL) {
    return NULL;
  }

  /* if there is a match in the hash, pull out that element */
  scr_hash_elem* elem = scr_hash_elem_extract(hash, key);
  if (elem == NULL) {
    /* nothing found, so create a new element and set it */
    elem = scr_hash_elem_new();
    scr_hash_elem_init(elem, key, hash_value);
  } else {
    /* this key already exists, delete its current hash and reset it */
    if (elem->hash != NULL) {
      scr_hash_delete(&elem->hash);
    }
    elem->hash = hash_value;
  }

  /* insert the element into the hash */
  LIST_INSERT_HEAD(hash, elem, pointers);

  /* return the pointer to the hash of the element */
  return elem->hash;
}

/* given a hash and a key, extract and return hash for specified key,
 * returns NULL if not found */
scr_hash* scr_hash_extract(scr_hash* hash, const char* key)
{
  if (hash == NULL) {
    return NULL;
  }

  scr_hash_elem* elem = scr_hash_elem_extract(hash, key);
  if (elem != NULL) {
    scr_hash* elem_hash = elem->hash;
    elem->hash = NULL;
    scr_hash_elem_delete(elem);
    return elem_hash;
  }
  return NULL;
}

/* given a hash and a key, extract and delete any matching element */
int scr_hash_unset(scr_hash* hash, const char* key)
{
  if (hash == NULL) {
    return SCR_SUCCESS;
  }

  scr_hash_elem* elem = scr_hash_elem_extract(hash, key);
  if (elem != NULL) {
    scr_hash_elem_delete(elem);
  }
  return SCR_SUCCESS;
}

/* unset all values in the hash, but don't delete it */
int scr_hash_unset_all(scr_hash* hash)
{
  scr_hash_elem* elem = scr_hash_elem_first(hash);
  while (elem != NULL) {
    /* remember this element */
    scr_hash_elem* tmp = elem;

    /* get the next element */
    elem = scr_hash_elem_next(elem);

    /* extract and delete the current element by address */
    scr_hash_elem_extract_by_addr(hash, tmp);
    scr_hash_elem_delete(tmp);
  }
  return SCR_SUCCESS;
}

/* merges (copies) elements from hash2 into hash1 */
int scr_hash_merge(scr_hash* hash1, const scr_hash* hash2)
{
  /* need hash1 to be valid to insert anything into it */
  if (hash1 == NULL) {
    return SCR_FAILURE;
  }

  /* if hash2 is NULL, there is nothing to insert, so we're done */
  if (hash2 == NULL) {
    return SCR_SUCCESS;
  }

  int rc = SCR_SUCCESS;

  /* iterate over the elements in hash2 */
  scr_hash_elem* elem;
  for (elem = scr_hash_elem_first(hash2);
       elem != NULL;
       elem = scr_hash_elem_next(elem))
  {
    /* get the key for this element */
    char* key = scr_hash_elem_key(elem);

    /* get hash for the matching element in hash1, if it has one */
    scr_hash* key_hash1 = scr_hash_get(hash1, key);
    if (key_hash1 == NULL) {
      /* hash1 had no element with this key, so create one */
      key_hash1 = scr_hash_set(hash1, key, scr_hash_new());
    }

    /* merge the hash for this key from hash2 with the hash for this
     * key from hash1 */
    scr_hash* key_hash2 = scr_hash_elem_hash(elem);
    if (scr_hash_merge(key_hash1, key_hash2) != SCR_SUCCESS) {
      rc = SCR_FAILURE;
    }
  }

  return rc;
}

/* traverse the given hash using a printf-like format string setting
 * an arbitrary list of keys to set (or reset) the hash associated
 * with the last-most key */
scr_hash* scr_hash_setf(scr_hash* hash, scr_hash* hash_value, const char* format, ...)
{
  /* check that we have a hash */
  if (hash == NULL) {
    return NULL;
  }

  scr_hash* h = hash;

  /* make a copy of the format specifier, since strtok will clobber
   * it */
  char* format_copy = strdup(format);
  if (format_copy == NULL) {
    scr_abort(-1, "Failed to duplicate format string @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* we break up tokens by spaces */
  char* search = " ";
  char* token = NULL;

  /* count how many keys we have */
  token = strtok(format_copy, search);
  int count = 0;
  while (token != NULL) {
    token = strtok(NULL, search);
    count++;
  }

  /* free our copy of the format specifier */
  scr_free(&format_copy);

  /* make a copy of the format specifier, since strtok will clobber
   * it */
  format_copy = strdup(format);
  if (format_copy == NULL) {
    scr_abort(-1, "Failed to duplicate format string @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* for each format specifier, convert the next key argument to a
   * string and look up the hash for that key */
  va_list args;
  va_start(args, format);
  token = strtok(format_copy, search);
  int i = 0;
  while (i < count && token != NULL && h != NULL) {
    /* interpret the format and convert the current key argument to
     * a string */
    char key[SCR_MAX_LINE];
    int size = 0;
    if (strcmp(token, "%s") == 0) {
      size = snprintf(key, sizeof(key), token, va_arg(args, char*));
    } else if (strcmp(token, "%d")  == 0) {
      size = snprintf(key, sizeof(key), token, va_arg(args, int));
    } else if (strcmp(token, "%lld") == 0) {
      size = snprintf(key, sizeof(key), token, va_arg(args, long long));
    } else if (strcmp(token, "%lu") == 0) {
      size = snprintf(key, sizeof(key), token, va_arg(args, unsigned long));
    } else if (strcmp(token, "%#x") == 0) {
      size = snprintf(key, sizeof(key), token, va_arg(args, unsigned int));
    } else if (strcmp(token, "%#lx") == 0) {
      size = snprintf(key, sizeof(key), token, va_arg(args, unsigned long));
    } else if (strcmp(token, "%llu") == 0) {
      size = snprintf(key, sizeof(key), token, va_arg(args, unsigned long long));
    } else if (strcmp(token, "%f") == 0) {
      size = snprintf(key, sizeof(key), token, va_arg(args, double));
    } else {
      scr_abort(-1, "Unsupported hash key format '%s' @ %s:%d",
        token, __FILE__, __LINE__
      );
    }

    /* check that we were able to fit the string into our buffer */
    if (size >= sizeof(key)) {
      scr_abort(-1, "Key buffer too small, have %lu need %d bytes @ %s:%d",
        sizeof(key), size, __FILE__, __LINE__
      );
    }

    if (i < count-1) {
      /* check whether we have an entry for this key in the current
       * hash */
      scr_hash* tmp = scr_hash_get(h, key);

      /* didn't find an entry for this key, so create one */
      if (tmp == NULL) {
        tmp = scr_hash_set(h, key, scr_hash_new());
      }

      /* now we have a hash for this key, continue with the next key */
      h = tmp;
    } else {
      /* we are at the last key, so set its hash using the value
       * provided by the caller */
      h = scr_hash_set(h, key, hash_value);
    }

    /* get the next format string */
    token = strtok(NULL, search);
    i++;
  }
  va_end(args);

  /* free our copy of the format specifier */
  scr_free(&format_copy);

  /* return the hash we found */
  return h;
}

/* return hash associated with list of keys */
scr_hash* scr_hash_getf(const scr_hash* hash, const char* format, ...)
{
  /* check that we have a hash */
  if (hash == NULL) {
    return NULL;
  }

  const scr_hash* h = hash;

  /* make a copy of the format specifier, since strtok clobbers it */
  char* format_copy = strdup(format);
  if (format_copy == NULL) {
    scr_abort(-1, "Failed to duplicate format string @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* we break up tokens by spaces */
  char* search = " ";
  char* token = NULL;

  /* count how many keys we have */
  token = strtok(format_copy, search);
  int count = 0;
  while (token != NULL) {
    token = strtok(NULL, search);
    count++;
  }

  /* free our copy of the format specifier */
  scr_free(&format_copy);

  /* make a copy of the format specifier, since strtok clobbers it */
  format_copy = strdup(format);
  if (format_copy == NULL) {
    scr_abort(-1, "Failed to duplicate format string @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* for each format specifier, convert the next key argument to a
   * string and look up the hash for that key */
  va_list args;
  va_start(args, format);
  token = strtok(format_copy, search);
  int i = 0;
  while (i < count && token != NULL && h != NULL) {
    /* interpret the format and convert the current key argument to
     * a string */
    char key[SCR_MAX_LINE];
    int size = 0;
    if (strcmp(token, "%s") == 0) {
      size = snprintf(key, sizeof(key), token, va_arg(args, char*));
    } else if (strcmp(token, "%d")  == 0) {
      size = snprintf(key, sizeof(key), token, va_arg(args, int));
    } else if (strcmp(token, "%lld") == 0) {
      size = snprintf(key, sizeof(key), token, va_arg(args, long long));
    } else if (strcmp(token, "%lu") == 0) {
      size = snprintf(key, sizeof(key), token, va_arg(args, unsigned long));
    } else if (strcmp(token, "%#x") == 0) {
      size = snprintf(key, sizeof(key), token, va_arg(args, unsigned int));
    } else if (strcmp(token, "%#lx") == 0) {
      size = snprintf(key, sizeof(key), token, va_arg(args, unsigned long));
    } else if (strcmp(token, "%llu") == 0) {
      size = snprintf(key, sizeof(key), token, va_arg(args, unsigned long long));
    } else if (strcmp(token, "%f") == 0) {
      size = snprintf(key, sizeof(key), token, va_arg(args, double));
    } else {
      scr_abort(-1, "Unsupported hash key format '%s' @ %s:%d",
        token, __FILE__, __LINE__
      );
    }

    /* check that we were able to fit the string into our buffer */
    if (size >= sizeof(key)) {
      scr_abort(-1, "Key buffer too small, have %lu need %d bytes @ %s:%d",
        sizeof(key), size, __FILE__, __LINE__
      );
    }

    /* get hash for this key */
    h = scr_hash_get(h, key);

    /* get the next format string */
    token = strtok(NULL, search);
    i++;
  }
  va_end(args);

  /* free our copy of the format specifier */
  scr_free(&format_copy);

  /* return the hash we found */
  return (scr_hash*) h;
}

/* sort strings in ascending order */
static int scr_hash_cmp_fn_str_asc(const void* a, const void* b)
{
  int cmp = strcmp((char*)a, (char*)b);
  return cmp;
}

/* sort strings in descending order */
static int scr_hash_cmp_fn_str_desc(const void* a, const void* b)
{
  int cmp = strcmp((char*)b, (char*)a);
  return cmp;
}

/* sort integers in ascending order */
static int scr_hash_cmp_fn_int_asc(const void* a, const void* b)
{
  return (int) (*(int*)a - *(int*)b);
}

/* sort integers in descending order */
static int scr_hash_cmp_fn_int_desc(const void* a, const void* b)
{
  return (int) (*(int*)b - *(int*)a);
}

/* sort the hash assuming the keys are ints */
int scr_hash_sort(scr_hash* hash, int direction)
{
  /* get the size of the hash */
  int count = scr_hash_size(hash);

  /* define a structure to hold the key and elem address */
  struct sort_elem {
    char* key;
    scr_hash_elem* addr;
  };

  /* allocate space for each element */
  struct sort_elem* list = (struct sort_elem*) malloc(count * sizeof(struct sort_elem));
  if (list == NULL) {
    return SCR_FAILURE;
  }

  /* walk the hash and fill in the keys */
  scr_hash_elem* elem = NULL;
  int index = 0;
  for (elem = scr_hash_elem_first(hash);
       elem != NULL;
       elem = scr_hash_elem_next(elem))
  {
    char* key = scr_hash_elem_key(elem);
    list[index].key = key;
    list[index].addr = elem;
    index++;
  }

  /* sort the elements by key */
  int (*fn)(const void* a, const void* b) = NULL;
  fn = &scr_hash_cmp_fn_str_asc;
  if (direction == SCR_HASH_SORT_DESCENDING) {
    fn = &scr_hash_cmp_fn_str_desc;
  }
  qsort(list, count, sizeof(struct sort_elem), fn);

  /* walk the sorted list backwards, extracting the element by address,
   * and inserting at the head */
  while (index > 0) {
    index--;
    elem = list[index].addr;
    LIST_REMOVE(elem, pointers);
    LIST_INSERT_HEAD(hash, elem, pointers);
  }

  /* free the list */
  scr_free(&list);

  return SCR_SUCCESS;
}

/* sort the hash assuming the keys are ints */
int scr_hash_sort_int(scr_hash* hash, int direction)
{
  /* get the size of the hash */
  int count = scr_hash_size(hash);

  /* define a structure to hold the key and elem address */
  struct sort_elem {
    int key;
    scr_hash_elem* addr;
  };

  /* allocate space for each element */
  struct sort_elem* list = (struct sort_elem*) malloc(count * sizeof(struct sort_elem));
  if (list == NULL) {
    return SCR_FAILURE;
  }

  /* walk the hash and fill in the keys */
  scr_hash_elem* elem = NULL;
  int index = 0;
  for (elem = scr_hash_elem_first(hash);
       elem != NULL;
       elem = scr_hash_elem_next(elem))
  {
    int key = scr_hash_elem_key_int(elem);
    list[index].key = key;
    list[index].addr = elem;
    index++;
  }

  /* sort the elements by key */
  int (*fn)(const void* a, const void* b) = NULL;
  fn = &scr_hash_cmp_fn_int_asc;
  if (direction == SCR_HASH_SORT_DESCENDING) {
    fn = &scr_hash_cmp_fn_int_desc;
  }
  qsort(list, count, sizeof(struct sort_elem), fn);

  /* walk the sorted list backwards, extracting the element by address,
   * and inserting at the head */
  while (index > 0) {
    index--;
    elem = list[index].addr;
    LIST_REMOVE(elem, pointers);
    LIST_INSERT_HEAD(hash, elem, pointers);
  }

  /* free the list */
  scr_free(&list);

  return SCR_SUCCESS;
}

/* given a hash, return a list of all keys converted to ints */
/* caller must free list when done with it */
int scr_hash_list_int(const scr_hash* hash, int* n, int** v)
{
  /* assume there aren't any keys */
  *n = 0;
  *v = NULL;

  /* count the number of keys */
  int count = scr_hash_size(hash);
  if (count == 0) {
    return SCR_SUCCESS;
  }

  /* now allocate array of ints to save keys */
  int* list = (int*) malloc(count * sizeof(int));
  if (list == NULL) {
    scr_abort(-1, "Failed to allocate integer list at %s:%d",
      __FILE__, __LINE__
    );
  }

  /* record key values in array */
  count = 0;
  scr_hash_elem* elem;
  for (elem = scr_hash_elem_first(hash);
       elem != NULL;
       elem = scr_hash_elem_next(elem))
  {
    int key = scr_hash_elem_key_int(elem);
    list[count] = key;
    count++;
  }

  /* sort the keys */
  qsort(list, count, sizeof(int), &scr_hash_cmp_fn_int_asc);

  *n = count;
  *v = list;

  return SCR_SUCCESS;
}

/*
=========================================
get, set, and unset hashes using a key/value pair
=========================================
*/

/* shortcut to create a key and subkey in a hash with one call */
scr_hash* scr_hash_set_kv(scr_hash* hash, const char* key, const char* val)
{
  if (hash == NULL) {
    return NULL;
  }

  scr_hash* k = scr_hash_get(hash, key);
  if (k == NULL) {
    k = scr_hash_set(hash, key, scr_hash_new());
  }

  scr_hash* v = scr_hash_get(k, val);
  if (v == NULL) {
    v = scr_hash_set(k, val, scr_hash_new());
  }

  return v;
}

/* same as scr_hash_set_kv, but with the subkey specified as an int */
scr_hash* scr_hash_set_kv_int(scr_hash* hash, const char* key, int val)
{
  /* TODO: this feels kludgy, but I guess as long as the ASCII string
   * is longer than a max int (or minimum int with leading minus sign)
   * which is 11 chars, we're ok ("-2147483648" to "2147483647") */
  char tmp[100];
  sprintf(tmp, "%d", val);
  return scr_hash_set_kv(hash, key, tmp);
}

/* shortcut to get hash assocated with the subkey of a key in a hash
 * with one call */
scr_hash* scr_hash_get_kv(const scr_hash* hash, const char* key, const char* val)
{
  if (hash == NULL) {
    return NULL;
  }

  scr_hash* k = scr_hash_get(hash, key);
  if (k == NULL) {
    return NULL;
  }

  scr_hash* v = scr_hash_get(k, val);
  if (v == NULL) {
    return NULL;
  }

  return v;
}

/* same as scr_hash_get_kv, but with the subkey specified as an int */
scr_hash* scr_hash_get_kv_int(const scr_hash* hash, const char* key, int val)
{
  /* TODO: this feels kludgy, but I guess as long as the ASCII string
   * is longer than a max int (or minimum int with leading minus sign)
   * which is 11 chars, we're ok ("-2147483648" to "2147483647") */
  char tmp[100];
  sprintf(tmp, "%d", val);
  return scr_hash_get_kv(hash, key, tmp);
}

/* unset subkey under key, and if that removes the only element for
 * key, unset key as well */
int scr_hash_unset_kv(scr_hash* hash, const char* key, const char* val)
{
  if (hash == NULL) {
    return SCR_SUCCESS;
  }

  scr_hash* v = scr_hash_get(hash, key);
  int rc = scr_hash_unset(v, val);
  if (scr_hash_size(v) == 0) {
    rc = scr_hash_unset(hash, key);
  }

  return rc;
}

/* same as scr_hash_unset_kv, but with the subkey specified as an int */
int scr_hash_unset_kv_int(scr_hash* hash, const char* key, int val)
{
  /* TODO: this feels kludgy, but I guess as long as the ASCII string
   * is longer than a max int (or minimum int with leading minus sign)
   * which is 11 chars, we're ok ("-2147483648" to "2147483647") */
  char tmp[100];
  sprintf(tmp, "%d", val);
  return scr_hash_unset_kv(hash, key, tmp);
}

/*
=========================================
Hash element functions
=========================================
*/

/* returns the first element for a given hash */
scr_hash_elem* scr_hash_elem_first(const scr_hash* hash)
{
  if (hash == NULL) {
    return NULL;
  }
  scr_hash_elem* elem = LIST_FIRST(hash);
  return elem;
}

/* given a hash element, returns the next element */
scr_hash_elem* scr_hash_elem_next(const scr_hash_elem* elem)
{
  if (elem == NULL) {
    return NULL;
  }
  scr_hash_elem* next = LIST_NEXT(elem, pointers);
  return next;
}

/* returns a pointer to the key of the specified element */
char* scr_hash_elem_key(const scr_hash_elem* elem)
{
  if (elem == NULL) {
    return NULL;
  }
  char* key = elem->key;
  return key;
}

/* same as scr_hash_elem_key, but converts the key as an int (returns
 * 0 if key is not defined) */
int scr_hash_elem_key_int(const scr_hash_elem* elem)
{
  if (elem == NULL) {
    return 0;
  }
  int i = atoi(elem->key);
  return i;
}

/* returns a pointer to the hash of the specified element */
scr_hash* scr_hash_elem_hash(const scr_hash_elem* elem)
{
  if (elem == NULL) {
    return NULL;
  }
  scr_hash* hash = elem->hash;
  return hash;
}

/* given a hash and a key, find first matching element and return its
 * address, returns NULL if not found */
scr_hash_elem* scr_hash_elem_get(const scr_hash* hash, const char* key)
{
  if (hash == NULL || key == NULL) {
    return NULL;
  }

  scr_hash_elem* elem;
  LIST_FOREACH(elem, hash, pointers) {
    if (elem->key != NULL && strcmp(elem->key, key) == 0) {
      return elem;
    }
  }
  return NULL;
}

/* given a hash and a key, return a pointer to the key of the first
 * element of that key's hash */
char* scr_hash_elem_get_first_val(const scr_hash* hash, const char* key)
{
  /* lookup the hash, then return a pointer to the key of the first
   * element */
  char* v = NULL;
  scr_hash* h = scr_hash_get(hash, key);
  if (h != NULL) {
    scr_hash_elem* e = scr_hash_elem_first(h);
    if (e != NULL) {
      v = scr_hash_elem_key(e);
    }
  }
  return v;
}

/* given a hash and a key, find first matching element, remove it
 * from the hash, and return it */
scr_hash_elem* scr_hash_elem_extract(scr_hash* hash, const char* key)
{
  scr_hash_elem* elem = scr_hash_elem_get(hash, key);
  if (elem != NULL) {
    LIST_REMOVE(elem, pointers);
  }
  return elem;
}

/* given a hash and a key (as integer), find first matching element,
 * remove it from the hash, and return it */
scr_hash_elem* scr_hash_elem_extract_int(scr_hash* hash, int key)
{
  char tmp[100];
  sprintf(tmp, "%d", key);

  scr_hash_elem* elem = scr_hash_elem_get(hash, tmp);
  if (elem != NULL) {
    LIST_REMOVE(elem, pointers);
  }
  return elem;
}

/* extract element from hash given the hash and the address of the
 * element */
scr_hash_elem* scr_hash_elem_extract_by_addr(scr_hash* hash, scr_hash_elem* elem)
{
  /* TODO: check that elem is really in hash */
  LIST_REMOVE(elem, pointers);
  return elem;
}

/* TODO: replace calls to get_first_val with this which provides
 * additional check */
/* returns key of first element belonging to the hash associated with
 * the given key in the given hash returns NULL if the key is not set
 * or if either hash is empty throws an error if the associated hash
 * has more than one element useful for keys that act as a single
 * key/value */
char* scr_hash_get_val(const scr_hash* hash, const char* key)
{
  char* value = NULL;

  /* check whether the specified key is even set */
  scr_hash* key_hash = scr_hash_get(hash, key);
  if (key_hash != NULL) {
    /* check that the size of this hash belonging to the key is
     * exactly 1 */
    int size = scr_hash_size(key_hash);
    if (size == 1) {
      /* get the key of the first element in this hash */
      scr_hash_elem* first = scr_hash_elem_first(key_hash);
      value = scr_hash_elem_key(first);
    } else {
      /* this is an error */
      scr_err("Hash for key %s expected to have exactly one element, but it has %d @ %s:%d",
        key, size, __FILE__, __LINE__
      );
    }
  }

  return value;
}

/*
=========================================
Pack and unpack hash and elements into a char buffer
=========================================
*/

/* computes the number of bytes needed to pack the given hash element */
static size_t scr_hash_elem_pack_size(const scr_hash_elem* elem)
{
  size_t size = 0;
  if (elem != NULL) {
    if (elem->key != NULL) {
      size += strlen(elem->key) + 1;
    } else {
      size += 1;
    }
    size += scr_hash_pack_size(elem->hash);
  } else {
    size += 1;
    size += scr_hash_pack_size(NULL);
  }
  return size;
}

/* packs a hash element into specified buf and returns the number of
 * bytes written */
static size_t scr_hash_elem_pack(char* buf, const scr_hash_elem* elem)
{
  size_t size = 0;
  if (elem != NULL) {
    if (elem->key != NULL) {
      strcpy(buf + size, elem->key);
      size += strlen(elem->key) + 1;
    } else {
      buf[size] = '\0';
      size += 1;
    }
    size += scr_hash_pack(buf + size, elem->hash);
  } else {
    buf[size] = '\0';
    size += 1;
    size += scr_hash_pack(buf + size, NULL);
  }
  return size;
}

/* unpacks hash element from specified buffer and returns the number of
 * bytes read and a pointer to a newly allocated hash */
static size_t scr_hash_elem_unpack(const char* buf, scr_hash_elem* elem)
{
  /* check that we got an elem object to unpack data into */
  if (elem == NULL) {
    return 0;
  }

  /* read in the key and value strings */
  size_t size = 0;

  /* read in the KEY string */
  const char* key = buf;
  size += strlen(key) + 1;

  /* read in the hash object */
  scr_hash* hash = scr_hash_new();
  size += scr_hash_unpack(buf + size, hash);
  
  /* set our elem with the key and hash values we unpacked */
  scr_hash_elem_init(elem, key, hash);

  return size;
}

/* computes the number of bytes needed to pack the given hash */
size_t scr_hash_pack_size(const scr_hash* hash)
{
  size_t size = 0;
  if (hash != NULL) {
    scr_hash_elem* elem;

    /* add the size required to store the COUNT */
    size += sizeof(uint32_t);

    /* finally add the size of each element */
    LIST_FOREACH(elem, hash, pointers) {
      size += scr_hash_elem_pack_size(elem);
    }  
  } else {
    size += sizeof(uint32_t);
  }
  return size;
}

/* packs the given hash into specified buf and returns the number of
 * bytes written */
size_t scr_hash_pack(char* buf, const scr_hash* hash)
{
  size_t size = 0;
  if (hash != NULL) {
    scr_hash_elem* elem;

    /* count the items in the hash */
    uint32_t count = 0;
    LIST_FOREACH(elem, hash, pointers) {
      count++;
    }

    /* pack the count value */
    uint32_t count_network = scr_hton32(count);
    memcpy(buf + size, &count_network, sizeof(uint32_t));
    size += sizeof(uint32_t);

    /* pack each element */
    LIST_FOREACH(elem, hash, pointers) {
      size += scr_hash_elem_pack(buf + size, elem);
    }
  } else {
    /* no hash -- just pack the count of 0 */
    uint32_t count_network = scr_hton32((uint32_t) 0);
    memcpy(buf + size, &count_network, sizeof(uint32_t));
    size += sizeof(uint32_t);
  }
  return size;
}

/* unpacks hash from specified buffer into given hash object and
 * returns the number of bytes read */
size_t scr_hash_unpack(const char* buf, scr_hash* hash)
{
  /* check that we got a hash object to unpack data into */
  if (hash == NULL) {
    return 0;
  }

  /* allocate a new hash object and initialize it */
  size_t size = 0;

  /* read in the COUNT value */
  uint32_t count_network = 0;
  memcpy(&count_network, buf + size, sizeof(uint32_t));
  uint32_t count = scr_ntoh32(count_network);
  size += sizeof(uint32_t);

  /* for each element, read in its hash */
  int i;
  for (i = 0; i < count; i++) {
    scr_hash_elem* elem = scr_hash_elem_new();
    size += scr_hash_elem_unpack(buf + size, elem);
    LIST_INSERT_HEAD(hash, elem, pointers);
  }

  /* return the size */
  return size;
}

/*
=========================================
Read and write hash to a file
=========================================
*/

size_t scr_hash_persist_size(const scr_hash* hash)
{
  /* compute the size of the file (includes header, data, and
   * trailing crc32) */
  size_t pack_size = scr_hash_pack_size(hash);
  size_t size = SCR_FILE_HASH_HEADER_SIZE + pack_size;

  /* add room for the crc32 value */
  size += sizeof(uint32_t);

  return size;
}

/* persist hash in newly allocated buffer,
 * return buffer address and size to be freed by caller */
int scr_hash_write_persist(void** ptr_buf, size_t* ptr_size, const scr_hash* hash)
{
  /* check that we have a hash, a file name, and a file descriptor */
  if (ptr_buf == NULL || hash == NULL) {
    return SCR_FAILURE;
  }

  /* compute size of buffer to persist hash */
  size_t bufsize = scr_hash_persist_size(hash);

  /* allocate a buffer to pack the hash in */
  char* buf = (char*) malloc(bufsize);
  if (buf == NULL) {
    scr_err("Allocating %lu byte buffer to persist hash @ %s:%d",
      (unsigned long) bufsize, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  size_t size = 0;
  uint64_t filesize = (uint64_t) bufsize;

  /* write the SCR file magic number, the hash file id, and the
   * version number */
  scr_pack_uint32_t(buf, filesize, &size, (uint32_t) SCR_FILE_MAGIC);
  scr_pack_uint16_t(buf, filesize, &size, (uint16_t) SCR_FILE_TYPE_HASH);
  scr_pack_uint16_t(buf, filesize, &size, (uint16_t) SCR_FILE_VERSION_HASH_1);

  /* write the file size (includes header, data, and trailing crc) */
  scr_pack_uint64_t(buf, filesize, &size, (uint64_t) filesize);

  /* set the flags, indicate that the crc32 is set */
  uint32_t flags = 0x0;
  flags |= SCR_FILE_FLAGS_CRC32;
  scr_pack_uint32_t(buf, filesize, &size, (uint32_t) flags);

  /* pack the hash into the buffer */
  size += scr_hash_pack(buf + size, hash);

  /* compute the crc over the length of the file */
  uLong crc = crc32(0L, Z_NULL, 0);
  crc = crc32(crc, (const Bytef*) buf, (uInt) size);

  /* write the crc to the buffer */
  scr_pack_uint32_t(buf, filesize, &size, (uint32_t) crc);

  /* check that it adds up correctly */
  if (size != bufsize) {
    scr_abort(-1, "Failed to persist hash wrote %lu bytes != expected %lu @ %s:%d",
      (unsigned long) size, (unsigned long) bufsize, __FILE__, __LINE__
    );
  }

  /* save address of buffer in output parameter */
  *ptr_buf  = buf;
  *ptr_size = size;

  return SCR_SUCCESS;;
}

/* executes logic of scr_has_write with opened file descriptor */
ssize_t scr_hash_write_fd(const char* file, int fd, const scr_hash* hash)
{
  /* check that we have a hash, a file name, and a file descriptor */
  if (file == NULL || fd < 0 || hash == NULL) {
    return -1;
  }

  /* persist hash to buffer */
  void* buf;
  size_t size;
  scr_hash_write_persist(&buf, &size, hash);

  /* write buffer to file */
  ssize_t nwrite = scr_write_attempt(file, fd, buf, size);

  /* free the pack buffer */
  scr_free(&buf);

  /* if we didn't write all of the bytes, return an error */
  if (nwrite != size) {
    return -1;
  }

  return nwrite;
}

/* write the given hash to specified file */
int scr_hash_write(const char* file, const scr_hash* hash)
{
  int rc = SCR_SUCCESS;

  /* check that we have a hash and a file name */
  if (file == NULL || hash == NULL) {
    return SCR_FAILURE;
  }

  /* open the hash file */
  mode_t mode_file = scr_getmode(1, 1, 0);
  int fd = scr_open(file, O_WRONLY | O_CREAT | O_TRUNC, mode_file);
  if (fd < 0) {
    scr_err("Opening hash file for write: %s @ %s:%d",
      file, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* write the hash */
  ssize_t nwrite = scr_hash_write_fd(file, fd, hash);
  if (nwrite < 0) {
    rc = SCR_FAILURE;
  }

  /* close the hash file */
  if (scr_close(file, fd) != SCR_SUCCESS) {
    rc = SCR_FAILURE;
  }

  return rc;
}

/* write the given hash to specified file */
int scr_hash_write_path(const scr_path* file_path, const scr_hash* hash)
{
  /* check that we have a hash and a file name */
  if (file_path == NULL || hash == NULL) {
    return SCR_FAILURE;
  }

  /* check that path is not NULL */
  if (scr_path_is_null(file_path)) {
    return SCR_FAILURE;
  }

  /* get file name */
  char* file = scr_path_strdup(file_path);

  /* write the hash */
  int rc = scr_hash_write(file, hash);

  /* free the string we allocated for the file */
  scr_free(&file);

  return rc;
}

#if 0
/* reads a hash from its persisted state stored at buf which is at
 * least bufsize bytes long, merges hash into output parameter
 * and returns number of bytes processed or -1 on error */
ssize_t scr_hash_read_persist(const void* buf, size_t bufsize, scr_hash* hash)
{
  size_t size = 0;

  /* check that we have a buffer and a hash */
  if (buf == NULL || hash == NULL || bufsize < SCR_FILE_HASH_HEADER_SIZE) {
    return -1;
  }

  /* read in the magic number, the type, and the version number */
  uint32_t magic;
  uint16_t type, version;
  scr_unpack_uint32_t(buf, bufsize, &size, &magic);
  scr_unpack_uint16_t(buf, bufsize, &size, &type);
  scr_unpack_uint16_t(buf, bufsize, &size, &version);

  /* check that the magic number matches */
  /* check that the file type is something we understand */
  /* check that the file version matches */
  if (magic   != SCR_FILE_MAGIC ||
      type    != SCR_FILE_TYPE_HASH ||
      version != SCR_FILE_VERSION_HASH_1)
  {
    scr_err("Header does not match expected values @ %s:%d",
      __FILE__, __LINE__
    );
    return -1;
  }

  /* read the file size */
  uint64_t filesize;
  scr_unpack_uint64_t(buf, bufsize, &size, &filesize);

  /* read the flags field (32 bits) */
  uint32_t flags;
  scr_unpack_uint32_t(buf, bufsize, &size, &flags);

  /* check that filesize is at least as large as the header */
  if (filesize < SCR_FILE_HASH_HEADER_SIZE) {
    scr_err("Invalid file size stored in %s @ %s:%d",
      file, __FILE__, __LINE__
    );
    return -1;
  }

  /* check that the filesize is not larger than the buffer size */
  if (filesize > bufsize) {
    scr_err("Buffer %lu bytes too small for hash %lu bytes @ %s:%d",
      (unsigned long) bufsize, (unsigned long) filesize, __FILE__, __LINE__
    );
    return -1;
  }

  /* check the crc value if it's set */
  int crc_set = flags & SCR_FILE_FLAGS_CRC32;
  if (crc_set) {
    /* compute the crc value of the data */
    uLong crc = crc32(0L, Z_NULL, 0);
    crc = crc32(crc, (const Bytef*) buf, (uInt) filesize - sizeof(uint32_t));

    /* read the crc value */
    uint32_t crc_file_network, crc_file;
    memcpy(&crc_file_network, buf + filesize - sizeof(uint32_t), sizeof(uint32_t));
    crc_file = scr_ntoh32(crc_file_network);

    /* check the crc value */
    if (crc != crc_file) {
      scr_err("CRC32 mismatch detected in hash @ %s:%d",
        __FILE__, __LINE__
      );
      return -1;
    }
  }

  /* create a temporary hash to read data into, unpack, and merge */
  scr_hash* tmp_hash = scr_hash_new();
  scr_hash_unpack(buf + size, tmp_hash);
  scr_hash_merge(hash, tmp_hash);
  scr_hash_delete(&tmp_hash);

  /* return number of bytes processed */
  ssize_t ret = (ssize_t) filesize;
  return ret;
}
#endif

/* executes logic of scr_hash_read using an opened file descriptor */
ssize_t scr_hash_read_fd(const char* file, int fd, scr_hash* hash)
{
  ssize_t nread;
  size_t size = 0;

  /* check that we have a hash, a file name, and a file descriptor */
  if (file == NULL || fd < 0 || hash == NULL) {
    return -1;
  }

  /* read in the file header */
  char header[SCR_FILE_HASH_HEADER_SIZE];
  nread = scr_read_attempt(file, fd, header, SCR_FILE_HASH_HEADER_SIZE);
  if (nread != SCR_FILE_HASH_HEADER_SIZE) {
    return -1;
  }

  /* read in the magic number, the type, and the version number */
  uint32_t magic;
  uint16_t type, version;
  scr_unpack_uint32_t(header, sizeof(header), &size, &magic);
  scr_unpack_uint16_t(header, sizeof(header), &size, &type);
  scr_unpack_uint16_t(header, sizeof(header), &size, &version);

  /* check that the magic number matches */
  /* check that the file type is something we understand */
  /* check that the file version matches */
  if (magic   != SCR_FILE_MAGIC ||
      type    != SCR_FILE_TYPE_HASH ||
      version != SCR_FILE_VERSION_HASH_1)
  {
    scr_err("File header does not match expected values in %s @ %s:%d",
      file, __FILE__, __LINE__
    );
    return -1;
  }

  /* read the file size */
  uint64_t filesize;
  scr_unpack_uint64_t(header, sizeof(header), &size, &filesize);

  /* read the flags field (32 bits) */
  uint32_t flags;
  scr_unpack_uint32_t(header, sizeof(header), &size, &flags);

  /* check that the filesize is valid (positive) */
  if (filesize < SCR_FILE_HASH_HEADER_SIZE) {
    scr_err("Invalid file size stored in %s @ %s:%d",
      file, __FILE__, __LINE__
    );
    return -1;
  }

  /* allocate a buffer to read the hash and crc */
  char* buf = (char*) malloc(filesize);
  if (buf == NULL) {
    scr_err("Allocating %lu byte buffer to write hash to %s @ %s:%d",
      (unsigned long) filesize, file, __FILE__, __LINE__
    );
    return -1;
  }

  /* copy the header into the buffer */
  memcpy(buf, header, size);

  /* read the rest of the file into the buffer */
  ssize_t remainder = filesize - size;
  if (remainder > 0) {
    nread = scr_read_attempt(file, fd, buf + size, remainder);
    if (nread != remainder) {
      scr_err("Failed to read file %s @ %s:%d",
        file, __FILE__, __LINE__
      );
      scr_free(&buf);
      return -1;
    }
  }

  /* check the crc value if it's set */
  int crc_set = flags & SCR_FILE_FLAGS_CRC32;
  if (crc_set) {
    /* TODO: we should check that the remainder above is at least 4
     * (for the crc) */

    /* compute the crc value of the data */
    uLong crc = crc32(0L, Z_NULL, 0);
    crc = crc32(crc, (const Bytef*) buf, (uInt) filesize - sizeof(uint32_t));

    /* read the crc value */
    uint32_t crc_file_network, crc_file;
    memcpy(&crc_file_network, buf + filesize - sizeof(uint32_t), sizeof(uint32_t));
    crc_file = scr_ntoh32(crc_file_network);

    /* check the crc value */
    if (crc != crc_file) {
      scr_err("CRC32 mismatch detected in %s @ %s:%d",
        file, __FILE__, __LINE__
      );
      scr_free(&buf);
      return -1;
    }
  }

  /* create a temporary hash to read data into, unpack, and merge */
  scr_hash* tmp_hash = scr_hash_new();
  scr_hash_unpack(buf + size, tmp_hash);
  scr_hash_merge(hash, tmp_hash);
  scr_hash_delete(&tmp_hash);

  /* free the buffer holding the file contents */
  scr_free(&buf);

  return filesize;
}

/* opens specified file and reads in a hash storing its contents in
 * the given hash object */
int scr_hash_read(const char* file, scr_hash* hash)
{
  /* check that we have a hash and a file name */
  if (file == NULL || hash == NULL) {
    return SCR_FAILURE;
  }

  /* can't read file, return error (special case so as not to print
   * error message below) */
  if (scr_file_is_readable(file) != SCR_SUCCESS) {
    return SCR_FAILURE;
  }

  /* open the hash file */
  int fd = scr_open(file, O_RDONLY);
  if (fd < 0) {
    scr_err("Opening hash file for read %s @ %s:%d",
      file, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* got the file open, be sure to close it even if we hit an error in
   * the read */
  int rc = SCR_SUCCESS;

  /* read the hash */
  ssize_t nread = scr_hash_read_fd(file, fd, hash);
  if (nread < 0) {
    rc = SCR_FAILURE;
  }

  /* close the hash file */
  if (scr_close(file, fd) != SCR_SUCCESS) {
    rc = SCR_FAILURE;
  }

  return rc;
}

/* opens specified file and reads in a hash storing its contents in
 * the given hash object */
int scr_hash_read_path(const scr_path* file_path, scr_hash* hash)
{
  /* check that we have a hash and a file name */
  if (file_path == NULL || hash == NULL) {
    return SCR_FAILURE;
  }

  /* check that path is not NULL */
  if (scr_path_is_null(file_path)) {
    return SCR_FAILURE;
  }

  /* get file name */
  char* file = scr_path_strdup(file_path);

  /* read the hash */
  int rc = scr_hash_read(file, hash);

  /* free the string */
  scr_free(&file);

  return rc;
}

/* given a filename and hash, lock/open/read/close/unlock the file */
int scr_hash_read_with_lock(const char* file, scr_hash* hash)
{
  /* check that we got a filename */
  if (file == NULL) {
    scr_err("No filename specified @ %s:%d",
      __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* check that we got a hash to read data into */
  if (hash == NULL) {
    scr_err("No hash provided to read data into @ %s:%d",
      __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* open file with lock for read / write access */
  mode_t mode_file = scr_getmode(1, 1, 0);
  int fd = scr_open_with_lock(file, O_RDWR | O_CREAT, mode_file);
  if (fd >= 0) {
    /* read the file into the hash */
    scr_hash_read_fd(file, fd, hash);

    /* close the file and release the lock */
    scr_close_with_unlock(file, fd);

    return SCR_SUCCESS;
  } else {
    scr_err("Failed to open file with lock %s @ %s:%d",
      file, __FILE__, __LINE__
    );
  }

  return SCR_FAILURE;
}

/* given a filename and hash, lock the file, open it, and read it into
 * hash, set fd to the opened file descriptor */
int scr_hash_lock_open_read(const char* file, int* fd, scr_hash* hash)
{
  /* check that we got a pointer to a file descriptor */
  if (fd == NULL) {
    scr_err("Must provide a pointer to an int to return opened file descriptor @ %s:%d",
      __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* we at least got a pointer to a file descriptor,
   * initialize it to -1 */
  *fd = -1;

  /* check that we got a filename */
  if (file == NULL) {
    scr_err("No filename specified @ %s:%d",
      __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* check that we got a hash to read data into */
  if (hash == NULL) {
    scr_err("No hash provided to read data into @ %s:%d",
      __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* open file with lock for read / write access */
  mode_t mode_file = scr_getmode(1, 1, 0);
  *fd = scr_open_with_lock(file, O_RDWR | O_CREAT, mode_file);
  if (*fd >= 0) {
    /* read the file into the hash */
    scr_hash_read_fd(file, *fd, hash);
    return SCR_SUCCESS;
  }

  return SCR_FAILURE;
}

/* given a filename, an opened file descriptor, and a hash, overwrite
 * file with hash, close, and unlock file */
int scr_hash_write_close_unlock(const char* file, int* fd, const scr_hash* hash)
{
  /* check that we got a pointer to a file descriptor */
  if (fd == NULL) {
    scr_err("Must provide a pointer to an opened file descriptor @ %s:%d",
      __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* check that the file descriptor is open */
  if (*fd < 0) {
    scr_err("File descriptor does not point to a valid file @ %s:%d",
      __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* check that we got a filename */
  if (file == NULL) {
    scr_err("No filename specified @ %s:%d",
      __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* check that we got a hash to read data into */
  if (hash == NULL) {
    scr_err("No hash provided to write data from @ %s:%d",
      __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  if (*fd >= 0) {
    /* wind the file pointer back to the start of the file */
    lseek(*fd, 0, SEEK_SET);

    /* write the updated hash back to the file */
    ssize_t nwrite = scr_hash_write_fd(file, *fd, hash);

    /* truncate file to new size */
    if (nwrite >= 0) {
      ftruncate(*fd, (off_t) nwrite);
    }

    /* close the file and release the lock */
    scr_close_with_unlock(file, *fd);

    /* mark the file descriptor as closed */
    *fd = -1;
  }

  return SCR_SUCCESS;
}

/*
=========================================
Print hash and elements to stdout for debugging
=========================================
*/

/* prints specified hash element to stdout for debugging */
static int scr_hash_elem_print(const scr_hash_elem* elem, int indent)
{
  char tmp[SCR_MAX_FILENAME];
  int i;
  for (i=0; i<indent; i++) {
    tmp[i] = ' ';
  }
  tmp[indent] = '\0';

  if (elem != NULL) {
    if (elem->key != NULL) {
      printf("%s%s\n", tmp, elem->key);
    } else {
      printf("%sNULL KEY\n", tmp);
    }
    scr_hash_print(elem->hash, indent);
  } else {
    printf("%sNULL ELEMENT\n", tmp);
  }
  return SCR_SUCCESS;
}

/* prints specified hash to stdout for debugging */
int scr_hash_print(const scr_hash* hash, int indent)
{
  char tmp[SCR_MAX_FILENAME];
  int i;
  for (i=0; i<indent; i++) {
    tmp[i] = ' ';
  }
  tmp[indent] = '\0';

  if (hash != NULL) {
    scr_hash_elem* elem;
    LIST_FOREACH(elem, hash, pointers) {
      scr_hash_elem_print(elem, indent+2);
    }
  } else {
    printf("%sNULL LIST\n", tmp);
  }
  return SCR_SUCCESS;
}

#ifndef HIDE_TV
/*
=========================================
Pretty print for TotalView debug window
=========================================
*/

/* This enables a nicer display when diving on a hash variable
 * under the TotalView debugger.  It requires TV 8.8 or later. */

#include "tv_data_display.h"

static int TV_ttf_display_type(const scr_hash* hash)
{
  if (hash == NULL) {
    /* empty hash, nothing to display here */
    return TV_ttf_format_ok;
  }

  scr_hash_elem* elem = NULL;
  for (elem = scr_hash_elem_first(hash);
       elem != NULL;
       elem = scr_hash_elem_next(elem))
  {
    /* get the key name */
    char* key = scr_hash_elem_key(elem);

    /* get a pointer to the hash for this key */
    scr_hash* h = scr_hash_elem_hash(elem);

    /* add a row to the TV debug window */
    if (h != NULL) {
      /* compute the size of the hash for this key */
      int size = scr_hash_size(h);

      if (size == 0) {
        /* if the hash is empty, stop at the key */
        TV_ttf_add_row("value", TV_ttf_type_ascii_string, key);
      } else if (size == 1) {
        /* my hash has one value, this may be a key/value pair */
        char* value = scr_hash_elem_get_first_val(hash, key);
        scr_hash* h2 = scr_hash_get(h, value);
        if (h2 == NULL || scr_hash_size(h2) == 0) {
          /* my hash has one value, and the hash for that one value
           * is empty, so print this as a key/value pair */
          TV_ttf_add_row(key, TV_ttf_type_ascii_string, value);
        } else {
          /* my hash has one value, but the hash for that one value is
           * non-empty so we need to recurse into hash */
          TV_ttf_add_row(key, "scr_hash", h);
        }
      } else {
        /* the hash for this key contains multiple elements,
         * so this is not a key/value pair, which means we need to
         * recurse into hash */
        TV_ttf_add_row(key, "scr_hash", h);
      }
    } else {
      /* this key has a NULL hash, so stop at the key */
      TV_ttf_add_row("value", TV_ttf_type_ascii_string, key);
    }
  }

  return TV_ttf_format_ok;
}
#endif /* HIDE_TV */
