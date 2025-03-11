/** \file kvtree.c
 * Defines a recursive hash data structure, where at the top level,
 * there is a list of elements indexed by string.  Each
 * of these elements in turn consists of a list of elements
 * indexed by string, and so on. */

#include "kvtree.h"
#include "kvtree_err.h"
#include "kvtree.h"
#include "kvtree_io.h"
#include "kvtree_helpers.h"
#include "kvtree_util.h"

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include <libgen.h>
#include <dirent.h>
#include <limits.h>
#include <regex.h>


/* need at least version 8.5 of queue.h from Berkeley */
#include "queue.h"

#include <stdint.h>

#define KVTREE_FILE_MAGIC          (0x951fc3f5)
#define KVTREE_FILE_TYPE_HASH      (1)
#define KVTREE_FILE_VERSION_HASH_1 (1)

#define KVTREE_FILE_HASH_HEADER_SIZE (20)
#define KVTREE_FILE_FLAGS_CRC32 (0x1) /* indicates that crc32 is stored at end of file */

/* ================================================= */
/** @name Allocate and delete hash objects */
///@{
/** allocates a new hash element */
static kvtree_elem* kvtree_elem_new()
{
  kvtree_elem* elem = (kvtree_elem*) KVTREE_MALLOC(sizeof(kvtree_elem));
  elem->key  = NULL;
  elem->hash = NULL;
  return elem;
}

/** frees a hash element */
static int kvtree_elem_delete(kvtree_elem* elem)
{
  if (elem != NULL) {
    /* free the key which was strdup'ed */
    kvtree_free(&(elem->key));

    /* free the hash */
    kvtree_delete(&elem->hash);
    elem->hash = NULL;

    /* finally, free the element structure itself */
    kvtree_free(&elem);
  }
  return KVTREE_SUCCESS;
}

/** allocates a new hash */
kvtree* kvtree_new()
{
  kvtree* hash = (kvtree*) KVTREE_MALLOC(sizeof(kvtree));
  LIST_INIT(hash);
  return hash;
}

/** frees a hash */
int kvtree_delete(kvtree** ptr_hash)
{
  if (ptr_hash != NULL) {
    kvtree* hash = *ptr_hash;
    if (hash != NULL) {
      while (!LIST_EMPTY(hash)) {
        kvtree_elem* elem = LIST_FIRST(hash);
        LIST_REMOVE(elem, pointers);
        kvtree_elem_delete(elem);
      }
      kvtree_free(ptr_hash);
    }
  }
  return KVTREE_SUCCESS;
}
///@}

/* ================================================= */
/** @name size, get, set, unset, and merge functions */
///@{
/** given an element, set its key and hash fields */
static kvtree_elem* kvtree_elem_init(kvtree_elem* elem, const char* key, kvtree* hash)
{
  if (elem != NULL) {
    if (key != NULL) {
      elem->key = strdup(key);
    } else {
      /* bad idea to allow key to be set to NULL */
      elem->key = NULL;
      kvtree_err("Setting hash element key to NULL @ %s:%d",
        __FILE__, __LINE__
      );
    }
    elem->hash = hash;
  }
  return elem;
}

/**
 * Return size of hash (number of keys)
 *
 * For example, if your tree was:
 *
 * NAME
 *   axl_cp
 * TYPE
 *   2
 * STATE_FILE
 *   state_file
 * STATE
 *   4
 * STATUS
 *   4
 * FILE
 *   hello.txt
 *     STATUS
 *       3
 *     SIZE
 *       6
 *     GID
 *       57592
 *     UID
 *       57592
 *     MODE
 *       33268
 *     DEST
 *       hello2.txt._AXL
 *
 * This would return 6 since there's six top level elements:
 *     "NAME", "TYPE", "STATE_FILE", "STATE", "STATUS", "FILE".
 */
int kvtree_size(const kvtree* hash)
{
  int count = 0;
  if (hash != NULL) {
    kvtree_elem* elem;
    LIST_FOREACH(elem, hash, pointers) {
      count++;
    }
  }
  return count;
}

/** given a hash and a key, return the hash associated with key,
 * returns NULL if not found */
kvtree* kvtree_get(const kvtree* hash, const char* key)
{
  kvtree_elem* elem = kvtree_elem_get(hash, key);
  if (elem != NULL) {
    return elem->hash;
  }
  return NULL;
}

/** given a hash, a key, and a hash value, set (or reset) the key's
 * hash and return the pointer to the new hash */
kvtree* kvtree_set(kvtree* hash, const char* key, kvtree* hash_value)
{
  /* check that we have a valid hash to insert into and a valid key
   * name */
  if (hash == NULL || key == NULL) {
    return NULL;
  }

  /* if there is a match in the hash, pull out that element */
  kvtree_elem* elem = kvtree_elem_extract(hash, key);
  if (elem == NULL) {
    /* nothing found, so create a new element and set it */
    elem = kvtree_elem_new();
    kvtree_elem_init(elem, key, hash_value);
  } else {
    /* this key already exists, delete its current hash and reset it */
    if (elem->hash != NULL) {
      kvtree_delete(&elem->hash);
    }
    elem->hash = hash_value;
  }

  /* insert the element into the hash */
  LIST_INSERT_HEAD(hash, elem, pointers);

  /* return the pointer to the hash of the element */
  return elem->hash;
}

/** given a hash and a key, extract and return hash for specified key,
 * returns NULL if not found */
kvtree* kvtree_extract(kvtree* hash, const char* key)
{
  if (hash == NULL) {
    return NULL;
  }

  kvtree_elem* elem = kvtree_elem_extract(hash, key);
  if (elem != NULL) {
    kvtree* elem_hash = elem->hash;
    elem->hash = NULL;
    kvtree_elem_delete(elem);
    return elem_hash;
  }
  return NULL;
}

/** given a hash and a key, extract and delete any matching element */
int kvtree_unset(kvtree* hash, const char* key)
{
  if (hash == NULL) {
    return KVTREE_SUCCESS;
  }

  kvtree_elem* elem = kvtree_elem_extract(hash, key);
  if (elem != NULL) {
    kvtree_elem_delete(elem);
  }
  return KVTREE_SUCCESS;
}

/** unset all values in the hash, but don't delete it */
int kvtree_unset_all(kvtree* hash)
{
  kvtree_elem* elem = kvtree_elem_first(hash);
  while (elem != NULL) {
    /* remember this element */
    kvtree_elem* tmp = elem;

    /* get the next element */
    elem = kvtree_elem_next(elem);

    /* extract and delete the current element by address */
    kvtree_elem_extract_by_addr(hash, tmp);
    kvtree_elem_delete(tmp);
  }
  return KVTREE_SUCCESS;
}

/** merges (copies) elements from hash2 into hash1 */
int kvtree_merge(kvtree* hash1, const kvtree* hash2)
{
  /* need hash1 to be valid to insert anything into it */
  if (hash1 == NULL) {
    return KVTREE_FAILURE;
  }

  /* if hash2 is NULL, there is nothing to insert, so we're done */
  if (hash2 == NULL) {
    return KVTREE_SUCCESS;
  }

  int rc = KVTREE_SUCCESS;

  /* iterate over the elements in hash2 */
  kvtree_elem* elem;
  for (elem = kvtree_elem_first(hash2);
       elem != NULL;
       elem = kvtree_elem_next(elem))
  {
    /* get the key for this element */
    char* key = kvtree_elem_key(elem);

    /* get hash for the matching element in hash1, if it has one */
    kvtree* key_hash1 = kvtree_get(hash1, key);
    if (key_hash1 == NULL) {
      /* hash1 had no element with this key, so create one */
      key_hash1 = kvtree_set(hash1, key, kvtree_new());
    }

    /* merge the hash for this key from hash2 with the hash for this
     * key from hash1 */
    kvtree* key_hash2 = kvtree_elem_hash(elem);
    if (kvtree_merge(key_hash1, key_hash2) != KVTREE_SUCCESS) {
      rc = KVTREE_FAILURE;
    }
  }

  return rc;
}

/** traverse the given hash using a printf-like format string setting
 * an arbitrary list of keys to set (or reset) the hash associated
 * with the last-most key */
kvtree* kvtree_setf(kvtree* hash, kvtree* hash_value, const char* format, ...)
{
  /* check that we have a hash */
  if (hash == NULL) {
    return NULL;
  }

  kvtree* h = hash;
  char *rest = NULL;

  /* make a copy of the format specifier, since strtok will clobber
   * it */
  char* format_copy = strdup(format);
  if (format_copy == NULL) {
    kvtree_abort(-1, "Failed to duplicate format string @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* we break up tokens by spaces */
  char* search = " ";
  char* token = NULL;

  /* count how many keys we have */
  token = strtok_r(format_copy, search, &rest);
  int count = 0;
  while (token != NULL) {
    token = strtok_r(NULL, search, &rest);
    count++;
  }

  /* free our copy of the format specifier */
  kvtree_free(&format_copy);

  /* make a copy of the format specifier, since strtok will clobber
   * it */
  format_copy = strdup(format);
  if (format_copy == NULL) {
    kvtree_abort(-1, "Failed to duplicate format string @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* for each format specifier, convert the next key argument to a
   * string and look up the hash for that key */
  va_list args;
  va_start(args, format);
  token = strtok_r(format_copy, search, &rest);
  int i = 0;
  while (i < count && token != NULL && h != NULL) {
    /* interpret the format and convert the current key argument to
     * a string */
    char key[KVTREE_MAX_LINE];
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
    } else if (strcmp(token, "%p") == 0) {
      size = snprintf(key, sizeof(key), token, va_arg(args, void*));
    } else {
      kvtree_abort(-1, "Unsupported hash key format '%s' @ %s:%d",
        token, __FILE__, __LINE__
      );
    }

    /* check that we were able to fit the string into our buffer */
    if (size >= sizeof(key)) {
      kvtree_abort(-1, "Key buffer too small, have %lu need %d bytes @ %s:%d",
        sizeof(key), size, __FILE__, __LINE__
      );
    }

    if (i < count-1) {
      /* check whether we have an entry for this key in the current
       * hash */
      kvtree* tmp = kvtree_get(h, key);

      /* didn't find an entry for this key, so create one */
      if (tmp == NULL) {
        tmp = kvtree_set(h, key, kvtree_new());
      }

      /* now we have a hash for this key, continue with the next key */
      h = tmp;
    } else {
      /* we are at the last key, so set its hash using the value
       * provided by the caller */
      h = kvtree_set(h, key, hash_value);
    }

    /* get the next format string */
    token = strtok_r(NULL, search, &rest);
    i++;
  }
  va_end(args);

  /* free our copy of the format specifier */
  kvtree_free(&format_copy);

  /* return the hash we found */
  return h;
}

/** return hash associated with list of keys */
kvtree* kvtree_getf(const kvtree* hash, const char* format, ...)
{
  /* check that we have a hash */
  if (hash == NULL) {
    return NULL;
  }

  const kvtree* h = hash;
  char* rest = NULL;

  /* make a copy of the format specifier, since strtok clobbers it */
  char* format_copy = strdup(format);
  if (format_copy == NULL) {
    kvtree_abort(-1, "Failed to duplicate format string @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* we break up tokens by spaces */
  char* search = " ";
  char* token = NULL;

  /* count how many keys we have */
  token = strtok_r(format_copy, search, &rest);
  int count = 0;
  while (token != NULL) {
    token = strtok_r(NULL, search, &rest);
    count++;
  }

  /* free our copy of the format specifier */
  kvtree_free(&format_copy);

  /* make a copy of the format specifier, since strtok clobbers it */
  format_copy = strdup(format);
  if (format_copy == NULL) {
    kvtree_abort(-1, "Failed to duplicate format string @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* for each format specifier, convert the next key argument to a
   * string and look up the hash for that key */
  va_list args;
  va_start(args, format);
  token = strtok_r(format_copy, search, &rest);
  int i = 0;
  while (i < count && token != NULL && h != NULL) {
    /* interpret the format and convert the current key argument to
     * a string */
    char key[KVTREE_MAX_LINE];
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
    } else if (strcmp(token, "%p") == 0) {
      size = snprintf(key, sizeof(key), token, va_arg(args, void*));
    } else {
      kvtree_abort(-1, "Unsupported hash key format '%s' @ %s:%d",
        token, __FILE__, __LINE__
      );
    }

    /* check that we were able to fit the string into our buffer */
    if (size >= sizeof(key)) {
      kvtree_abort(-1, "Key buffer too small, have %lu need %d bytes @ %s:%d",
        sizeof(key), size, __FILE__, __LINE__
      );
    }

    /* get hash for this key */
    h = kvtree_get(h, key);

    /* get the next format string */
    token = strtok_r(NULL, search, &rest);
    i++;
  }
  va_end(args);

  /* free our copy of the format specifier */
  kvtree_free(&format_copy);

  /* return the hash we found */
  return (kvtree*) h;
}

/** define a structure to hold the key and elem address */
struct sort_elem_str {
  char* key;
  kvtree_elem* addr;
};

/** define a structure to hold the key and elem address */
struct sort_elem_int {
  int key;
  kvtree_elem* addr;
};

/** sort strings in ascending order */
static int kvtree_cmp_fn_str_asc(const void* a, const void* b)
{
  struct sort_elem_str* elem_a = (struct sort_elem_str*) a;
  struct sort_elem_str* elem_b = (struct sort_elem_str*) b;
  const char* str_a = elem_a->key;
  const char* str_b = elem_b->key;
  int cmp = strcmp(str_a, str_b);
  return cmp;
}

/** sort strings in descending order */
static int kvtree_cmp_fn_str_desc(const void* a, const void* b)
{
  struct sort_elem_str* elem_a = (struct sort_elem_str*) a;
  struct sort_elem_str* elem_b = (struct sort_elem_str*) b;
  const char* str_a = elem_a->key;
  const char* str_b = elem_b->key;
  int cmp = strcmp(str_b, str_a);
  return cmp;
}

/** sort integers in ascending order */
static int kvtree_cmp_fn_int_asc(const void* a, const void* b)
{
  struct sort_elem_int* elem_a = (struct sort_elem_int*) a;
  struct sort_elem_int* elem_b = (struct sort_elem_int*) b;
  int int_a = elem_a->key;
  int int_b = elem_b->key;
  return (int) (int_a - int_b);
}

/** sort integers in descending order */
static int kvtree_cmp_fn_int_desc(const void* a, const void* b)
{
  struct sort_elem_int* elem_a = (struct sort_elem_int*) a;
  struct sort_elem_int* elem_b = (struct sort_elem_int*) b;
  int int_a = elem_a->key;
  int int_b = elem_b->key;
  return (int) (int_b - int_a);
}

/** sort the hash assuming the keys are ints */
int kvtree_sort(kvtree* hash, int direction)
{
  /* get the size of the hash */
  int count = kvtree_size(hash);

  /* allocate space for each element */
  struct sort_elem_str* list = (struct sort_elem_str*) KVTREE_MALLOC(count * sizeof(struct sort_elem_str));

  /* walk the hash and fill in the keys */
  kvtree_elem* elem = NULL;
  int index = 0;
  for (elem = kvtree_elem_first(hash);
       elem != NULL;
       elem = kvtree_elem_next(elem))
  {
    char* key = kvtree_elem_key(elem);
    list[index].key = key;
    list[index].addr = elem;
    index++;
  }

  /* sort the elements by key */
  int (*fn)(const void* a, const void* b) = NULL;
  fn = &kvtree_cmp_fn_str_asc;
  if (direction == KVTREE_SORT_DESCENDING) {
    fn = &kvtree_cmp_fn_str_desc;
  }
  qsort(list, count, sizeof(struct sort_elem_str), fn);

  /* walk the sorted list backwards, extracting the element by address,
   * and inserting at the head */
  while (index > 0) {
    index--;
    elem = list[index].addr;
    LIST_REMOVE(elem, pointers);
    LIST_INSERT_HEAD(hash, elem, pointers);
  }

  /* free the list */
  kvtree_free(&list);

  return KVTREE_SUCCESS;
}

/** sort the hash assuming the keys are ints */
int kvtree_sort_int(kvtree* hash, int direction)
{
  /* get the size of the hash */
  int count = kvtree_size(hash);

  /* allocate space for each element */
  struct sort_elem_int* list = (struct sort_elem_int*) KVTREE_MALLOC(count * sizeof(struct sort_elem_int));

  /* walk the hash and fill in the keys */
  kvtree_elem* elem = NULL;
  int index = 0;
  for (elem = kvtree_elem_first(hash);
       elem != NULL;
       elem = kvtree_elem_next(elem))
  {
    int key = kvtree_elem_key_int(elem);
    list[index].key = key;
    list[index].addr = elem;
    index++;
  }

  /* sort the elements by key */
  int (*fn)(const void* a, const void* b) = NULL;
  fn = &kvtree_cmp_fn_int_asc;
  if (direction == KVTREE_SORT_DESCENDING) {
    fn = &kvtree_cmp_fn_int_desc;
  }
  qsort(list, count, sizeof(struct sort_elem_int), fn);

  /* walk the sorted list backwards, extracting the element by address,
   * and inserting at the head */
  while (index > 0) {
    index--;
    elem = list[index].addr;
    LIST_REMOVE(elem, pointers);
    LIST_INSERT_HEAD(hash, elem, pointers);
  }

  /* free the list */
  kvtree_free(&list);

  return KVTREE_SUCCESS;
}

/** given a hash, return a list of all keys converted to ints */
/* caller must free list when done with it */
int kvtree_list_int(const kvtree* hash, int* n, int** v)
{
  /* assume there aren't any keys */
  *n = 0;
  *v = NULL;

  /* count the number of keys */
  int count = kvtree_size(hash);
  if (count == 0) {
    return KVTREE_SUCCESS;
  }

  /* now allocate array of ints to save keys */
  int* list = (int*) KVTREE_MALLOC(count * sizeof(int));

  /* record key values in array */
  count = 0;
  kvtree_elem* elem;
  for (elem = kvtree_elem_first(hash);
       elem != NULL;
       elem = kvtree_elem_next(elem))
  {
    int key = kvtree_elem_key_int(elem);
    list[count] = key;
    count++;
  }

  /* sort the keys */
  qsort(list, count, sizeof(int), &kvtree_cmp_fn_int_asc);

  *n = count;
  *v = list;

  return KVTREE_SUCCESS;
}
///@}

/* ================================================= */
/** @name get, set, and unset hashes using a key/value pair */
///@{

/** shortcut to create a key and subkey in a hash with one call */
kvtree* kvtree_set_kv(kvtree* hash, const char* key, const char* val)
{
  if (hash == NULL) {
    return NULL;
  }

  kvtree* k = kvtree_get(hash, key);
  if (k == NULL) {
    k = kvtree_set(hash, key, kvtree_new());
  }

  kvtree* v = kvtree_get(k, val);
  if (v == NULL) {
    v = kvtree_set(k, val, kvtree_new());
  }

  return v;
}

/** same as kvtree_set_kv, but with the subkey specified as an int */
kvtree* kvtree_set_kv_int(kvtree* hash, const char* key, int val)
{
  /* TODO: this feels kludgy, but I guess as long as the ASCII string
   * is longer than a max int (or minimum int with leading minus sign)
   * which is 11 chars, we're ok ("-2147483648" to "2147483647") */
  char tmp[100];
  sprintf(tmp, "%d", val);
  return kvtree_set_kv(hash, key, tmp);
}

/** shortcut to get hash assocated with the subkey of a key in a hash
 * with one call */
kvtree* kvtree_get_kv(const kvtree* hash, const char* key, const char* val)
{
  if (hash == NULL) {
    return NULL;
  }

  kvtree* k = kvtree_get(hash, key);
  if (k == NULL) {
    return NULL;
  }

  kvtree* v = kvtree_get(k, val);
  if (v == NULL) {
    return NULL;
  }

  return v;
}

/** same as kvtree_get_kv, but with the subkey specified as an int */
kvtree* kvtree_get_kv_int(const kvtree* hash, const char* key, int val)
{
  /* TODO: this feels kludgy, but I guess as long as the ASCII string
   * is longer than a max int (or minimum int with leading minus sign)
   * which is 11 chars, we're ok ("-2147483648" to "2147483647") */
  char tmp[100];
  sprintf(tmp, "%d", val);
  return kvtree_get_kv(hash, key, tmp);
}

/** unset subkey under key, and if that removes the only element for
 * key, unset key as well */
int kvtree_unset_kv(kvtree* hash, const char* key, const char* val)
{
  if (hash == NULL) {
    return KVTREE_SUCCESS;
  }

  kvtree* v = kvtree_get(hash, key);
  int rc = kvtree_unset(v, val);
  if (kvtree_size(v) == 0) {
    rc = kvtree_unset(hash, key);
  }

  return rc;
}

/** same as kvtree_unset_kv, but with the subkey specified as an int */
int kvtree_unset_kv_int(kvtree* hash, const char* key, int val)
{
  /* TODO: this feels kludgy, but I guess as long as the ASCII string
   * is longer than a max int (or minimum int with leading minus sign)
   * which is 11 chars, we're ok ("-2147483648" to "2147483647") */
  char tmp[100];
  sprintf(tmp, "%d", val);
  return kvtree_unset_kv(hash, key, tmp);
}

/*
=========================================
Hash element functions
=========================================
*/

/** returns the first element for a given hash */
kvtree_elem* kvtree_elem_first(const kvtree* hash)
{
  if (hash == NULL) {
    return NULL;
  }
  kvtree_elem* elem = LIST_FIRST(hash);
  return elem;
}

/** given a hash element, returns the next element */
kvtree_elem* kvtree_elem_next(const kvtree_elem* elem)
{
  if (elem == NULL) {
    return NULL;
  }
  kvtree_elem* next = LIST_NEXT(elem, pointers);
  return next;
}

/** returns a pointer to the key of the specified element */
char* kvtree_elem_key(const kvtree_elem* elem)
{
  if (elem == NULL) {
    return NULL;
  }
  char* key = elem->key;
  return key;
}

/** same as kvtree_elem_key, but converts the key as an int (returns
 * 0 if key is not defined) */
int kvtree_elem_key_int(const kvtree_elem* elem)
{
  if (elem == NULL) {
    return 0;
  }
  int i = atoi(elem->key);
  return i;
}

/** returns a pointer to the hash of the specified element */
kvtree* kvtree_elem_hash(const kvtree_elem* elem)
{
  if (elem == NULL) {
    return NULL;
  }
  kvtree* hash = elem->hash;
  return hash;
}

/** given a hash and a key, find first matching element and return its
 * address, returns NULL if not found */
kvtree_elem* kvtree_elem_get(const kvtree* hash, const char* key)
{
  if (hash == NULL || key == NULL) {
    return NULL;
  }

  kvtree_elem* elem;
  LIST_FOREACH(elem, hash, pointers) {
    if (elem->key != NULL && strcmp(elem->key, key) == 0) {
      return elem;
    }
  }
  return NULL;
}

/** given a hash and a key, return a pointer to the key of the first
 * element of that key's hash */
char* kvtree_elem_get_first_val(const kvtree* hash, const char* key)
{
  /* lookup the hash, then return a pointer to the key of the first
   * element */
  char* v = NULL;
  kvtree* h = kvtree_get(hash, key);
  if (h != NULL) {
    kvtree_elem* e = kvtree_elem_first(h);
    if (e != NULL) {
      v = kvtree_elem_key(e);
    }
  }
  return v;
}

/** given a hash and a key, find first matching element, remove it
 * from the hash, and return it */
kvtree_elem* kvtree_elem_extract(kvtree* hash, const char* key)
{
  kvtree_elem* elem = kvtree_elem_get(hash, key);
  if (elem != NULL) {
    LIST_REMOVE(elem, pointers);
  }
  return elem;
}

/** given a hash and a key (as integer), find first matching element,
 * remove it from the hash, and return it */
kvtree_elem* kvtree_elem_extract_int(kvtree* hash, int key)
{
  char tmp[100];
  sprintf(tmp, "%d", key);

  kvtree_elem* elem = kvtree_elem_get(hash, tmp);
  if (elem != NULL) {
    LIST_REMOVE(elem, pointers);
  }
  return elem;
}

/** extract element from hash given the hash and the address of the
 * element */
kvtree_elem* kvtree_elem_extract_by_addr(kvtree* hash, kvtree_elem* elem)
{
  /* TODO: check that elem is really in hash */
  LIST_REMOVE(elem, pointers);
  return elem;
}

/* TODO: replace calls to get_first_val with this which provides
 * additional check */
/** returns key of first element belonging to the hash associated with
 * the given key in the given hash returns NULL if the key is not set
 * or if either hash is empty throws an error if the associated hash
 * has more than one element useful for keys that act as a single
 * key/value */
char* kvtree_get_val(const kvtree* hash, const char* key)
{
  char* value = NULL;

  /* check whether the specified key is even set */
  kvtree* key_hash = kvtree_get(hash, key);
  if (key_hash != NULL) {
    /* check that the size of this hash belonging to the key is
     * exactly 1 */
    int size = kvtree_size(key_hash);
    if (size == 1) {
      /* get the key of the first element in this hash */
      kvtree_elem* first = kvtree_elem_first(key_hash);
      value = kvtree_elem_key(first);
    } else {
      /* this is an error */
      kvtree_err("Hash for key %s expected to have exactly one element, but it has %d @ %s:%d",
        key, size, __FILE__, __LINE__
      );
    }
  }

  return value;
}
///@}

/* ================================================= */
/** @name Pack and unpack hash and elements into a char buffer */
///@{

/** computes the number of bytes needed to pack the given hash element */
static size_t kvtree_elem_pack_size(const kvtree_elem* elem)
{
  size_t size = 0;
  if (elem != NULL) {
    if (elem->key != NULL) {
      size += strlen(elem->key) + 1;
    } else {
      size += 1;
    }
    size += kvtree_pack_size(elem->hash);
  } else {
    size += 1;
    size += kvtree_pack_size(NULL);
  }
  return size;
}

/** packs a hash element into specified buf and returns the number of
 * bytes written */
static size_t kvtree_elem_pack(char* buf, const kvtree_elem* elem)
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
    size += kvtree_pack(buf + size, elem->hash);
  } else {
    buf[size] = '\0';
    size += 1;
    size += kvtree_pack(buf + size, NULL);
  }
  return size;
}

/** unpacks hash element from specified buffer and returns the number of
 * bytes read and a pointer to a newly allocated hash */
static size_t kvtree_elem_unpack(const char* buf, kvtree_elem* elem)
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
  kvtree* hash = kvtree_new();
  size += kvtree_unpack(buf + size, hash);

  /* set our elem with the key and hash values we unpacked */
  kvtree_elem_init(elem, key, hash);

  return size;
}

/** computes the number of bytes needed to pack the given hash */
size_t kvtree_pack_size(const kvtree* hash)
{
  size_t size = 0;
  if (hash != NULL) {
    kvtree_elem* elem;

    /* add the size required to store the COUNT */
    size += sizeof(uint32_t);

    /* finally add the size of each element */
    LIST_FOREACH(elem, hash, pointers) {
      size += kvtree_elem_pack_size(elem);
    }
  } else {
    size += sizeof(uint32_t);
  }
  return size;
}

/** packs the given hash into specified buf and returns the number of
 * bytes written */
size_t kvtree_pack(char* buf, const kvtree* hash)
{
  size_t size = 0;
  if (hash != NULL) {
    kvtree_elem* elem;

    /* count the items in the hash */
    uint32_t count = 0;
    LIST_FOREACH(elem, hash, pointers) {
      count++;
    }

    /* pack the count value */
    uint32_t count_network = kvtree_hton32(count);
    memcpy(buf + size, &count_network, sizeof(uint32_t));
    size += sizeof(uint32_t);

    /* pack each element */
    LIST_FOREACH(elem, hash, pointers) {
      size += kvtree_elem_pack(buf + size, elem);
    }
  } else {
    /* no hash -- just pack the count of 0 */
    uint32_t count_network = kvtree_hton32((uint32_t) 0);
    memcpy(buf + size, &count_network, sizeof(uint32_t));
    size += sizeof(uint32_t);
  }
  return size;
}

/** unpacks hash from specified buffer into given hash object and
 * returns the number of bytes read */
size_t kvtree_unpack(const char* buf, kvtree* hash)
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
  uint32_t count = kvtree_ntoh32(count_network);
  size += sizeof(uint32_t);

  /* for each element, read in its hash */
  int i;
  for (i = 0; i < count; i++) {
    kvtree_elem* elem = kvtree_elem_new();
    size += kvtree_elem_unpack(buf + size, elem);
    LIST_INSERT_HEAD(hash, elem, pointers);
  }

  /* return the size */
  return size;
}
///@}

/* ================================================= */
/** @name Read and write hash to a file */
///@{

/** computes the size needed to persist a hash
includes room for header, data, and crc32 */
size_t kvtree_persist_size(const kvtree* hash)
{
  /* compute the size of the file (includes header, data, and
   * trailing crc32) */
  size_t pack_size = kvtree_pack_size(hash);
  size_t size = KVTREE_FILE_HASH_HEADER_SIZE + pack_size;

  /* add room for the crc32 value */
  size += sizeof(uint32_t);

  return size;
}

/** persist hash in newly allocated buffer,
 * return buffer address and size to be freed by caller */
int kvtree_write_persist(void** ptr_buf, size_t* ptr_size, const kvtree* hash)
{
  /* check that we have a hash, a file name, and a file descriptor */
  if (ptr_buf == NULL || hash == NULL) {
    return KVTREE_FAILURE;
  }

  /* compute size of buffer to persist hash */
  size_t bufsize = kvtree_persist_size(hash);

  /* allocate a buffer to pack the hash in */
  char* buf = (char*) KVTREE_MALLOC(bufsize);

  size_t size = 0;
  uint64_t filesize = (uint64_t) bufsize;

  /* write the KVTREE file magic number, the hash file id, and the
   * version number */
  kvtree_pack_uint32_t(buf, filesize, &size, (uint32_t) KVTREE_FILE_MAGIC);
  kvtree_pack_uint16_t(buf, filesize, &size, (uint16_t) KVTREE_FILE_TYPE_HASH);
  kvtree_pack_uint16_t(buf, filesize, &size, (uint16_t) KVTREE_FILE_VERSION_HASH_1);

  /* write the file size (includes header, data, and trailing crc) */
  kvtree_pack_uint64_t(buf, filesize, &size, (uint64_t) filesize);

  /* set the flags, indicate that the crc32 is set */
  uint32_t flags = 0x0;
  flags |= KVTREE_FILE_FLAGS_CRC32;
  kvtree_pack_uint32_t(buf, filesize, &size, (uint32_t) flags);

  /* pack the hash into the buffer */
  size += kvtree_pack(buf + size, hash);

  /* compute the crc over the length of the file */
  uLong crc = crc32(0L, Z_NULL, 0);
  crc = crc32(crc, (const Bytef*) buf, (uInt) size);

  /* write the crc to the buffer */
  kvtree_pack_uint32_t(buf, filesize, &size, (uint32_t) crc);

  /* check that it adds up correctly */
  if (size != bufsize) {
    kvtree_abort(-1, "Failed to persist hash wrote %lu bytes != expected %lu @ %s:%d",
      (unsigned long) size, (unsigned long) bufsize, __FILE__, __LINE__
    );
  }

  /* save address of buffer in output parameter */
  *ptr_buf  = buf;
  *ptr_size = size;

  return KVTREE_SUCCESS;;
}

/** executes logic of kvtree_has_write with opened file descriptor */
ssize_t kvtree_write_fd(const char* file, int fd, const kvtree* hash)
{
  /* check that we have a hash, a file name, and a file descriptor */
  if (file == NULL || fd < 0 || hash == NULL) {
    return -1;
  }

  /* persist hash to buffer */
  void* buf;
  size_t size;
  kvtree_write_persist(&buf, &size, hash);

  /* write buffer to file */
  ssize_t nwrite = kvtree_write_attempt(file, fd, buf, size);

  /* free the pack buffer */
  kvtree_free(&buf);

  /* if we didn't write all of the bytes, return an error */
  if (nwrite != size) {
    return -1;
  }

  return nwrite;
}

/** write the given hash to specified file */
int kvtree_write_file(const char* file, const kvtree* hash)
{
  int rc = KVTREE_SUCCESS;

  /* check that we have a hash and a file name */
  if (file == NULL || hash == NULL) {
    return KVTREE_FAILURE;
  }

  /* open the hash file */
  mode_t mode_file = kvtree_getmode(1, 1, 0);
  int fd = kvtree_open(file, O_WRONLY | O_CREAT | O_TRUNC, mode_file);
  if (fd < 0) {
    kvtree_err("Opening hash file for write: %s @ %s:%d",
      file, __FILE__, __LINE__
    );
    return KVTREE_FAILURE;
  }

  /* write the hash */
  ssize_t nwrite = kvtree_write_fd(file, fd, hash);
  if (nwrite < 0) {
    rc = KVTREE_FAILURE;
  }

  /* close the hash file */
  if (kvtree_close(file, fd) != KVTREE_SUCCESS) {
    rc = KVTREE_FAILURE;
  }

  return rc;
}

#if 0
/** reads a hash from its persisted state stored at buf which is at
 * least bufsize bytes long, merges hash into output parameter
 * and returns number of bytes processed or -1 on error */
ssize_t kvtree_read_persist(const void* buf, size_t bufsize, kvtree* hash)
{
  size_t size = 0;

  /* check that we have a buffer and a hash */
  if (buf == NULL || hash == NULL || bufsize < KVTREE_FILE_HASH_HEADER_SIZE) {
    return -1;
  }

  /* read in the magic number, the type, and the version number */
  uint32_t magic;
  uint16_t type, version;
  kvtree_unpack_uint32_t(buf, bufsize, &size, &magic);
  kvtree_unpack_uint16_t(buf, bufsize, &size, &type);
  kvtree_unpack_uint16_t(buf, bufsize, &size, &version);

  /* check that the magic number matches */
  /* check that the file type is something we understand */
  /* check that the file version matches */
  if (magic   != KVTREE_FILE_MAGIC ||
      type    != KVTREE_FILE_TYPE_HASH ||
      version != KVTREE_FILE_VERSION_HASH_1)
  {
    kvtree_err("Header does not match expected values @ %s:%d",
      __FILE__, __LINE__
    );
    return -1;
  }

  /* read the file size */
  uint64_t filesize;
  kvtree_unpack_uint64_t(buf, bufsize, &size, &filesize);

  /* read the flags field (32 bits) */
  uint32_t flags;
  kvtree_unpack_uint32_t(buf, bufsize, &size, &flags);

  /* check that filesize is at least as large as the header */
  if (filesize < KVTREE_FILE_HASH_HEADER_SIZE) {
    kvtree_err("Invalid file size stored in %s @ %s:%d",
      file, __FILE__, __LINE__
    );
    return -1;
  }

  /* check that the filesize is not larger than the buffer size */
  if (filesize > bufsize) {
    kvtree_err("Buffer %lu bytes too small for hash %lu bytes @ %s:%d",
      (unsigned long) bufsize, (unsigned long) filesize, __FILE__, __LINE__
    );
    return -1;
  }

  /* check the crc value if it's set */
  int crc_set = flags & KVTREE_FILE_FLAGS_CRC32;
  if (crc_set) {
    /* compute the crc value of the data */
    uLong crc = crc32(0L, Z_NULL, 0);
    crc = crc32(crc, (const Bytef*) buf, (uInt) filesize - sizeof(uint32_t));

    /* read the crc value */
    uint32_t crc_file_network, crc_file;
    memcpy(&crc_file_network, buf + filesize - sizeof(uint32_t), sizeof(uint32_t));
    crc_file = kvtree_ntoh32(crc_file_network);

    /* check the crc value */
    if (crc != crc_file) {
      kvtree_err("CRC32 mismatch detected in hash @ %s:%d",
        __FILE__, __LINE__
      );
      return -1;
    }
  }

  /* create a temporary hash to read data into, unpack, and merge */
  kvtree* tmp_hash = kvtree_new();
  kvtree_unpack(buf + size, tmp_hash);
  kvtree_merge(hash, tmp_hash);
  kvtree_delete(&tmp_hash);

  /* return number of bytes processed */
  ssize_t ret = (ssize_t) filesize;
  return ret;
}
#endif

/** executes logic of kvtree_read using an opened file descriptor */
ssize_t kvtree_read_fd(const char* file, int fd, kvtree* hash)
{
  /* check that we have a hash, a file name, and a file descriptor */
  if (file == NULL || fd < 0 || hash == NULL) {
    kvtree_err("Bad file/fd/hash in %s @ %s:%d",
      file, __FILE__, __LINE__);
    return -1;
  }

  /* read in the file header */
  char header[KVTREE_FILE_HASH_HEADER_SIZE];
  ssize_t nread = kvtree_read_attempt(file, fd, header, KVTREE_FILE_HASH_HEADER_SIZE);
  if (nread < 0) {
    /* error while reading header */
    kvtree_err("Failed to read header from %s @ %s:%d",
      file, __FILE__, __LINE__);
    return -1;
  }
  if (nread == 0) {
    /* We've got an empty file.  It depends on the situation as to whether this is
     * an error or not.  Do not print an error, and return 0 so someone can test.
     * Any valid, non-empty file will return a positive size. */
    return 0;
  }
  if (nread != KVTREE_FILE_HASH_HEADER_SIZE) {
    /* read something but the expected size does not match */
    kvtree_err("Invalid header: read %zu bytes but expected %d in %s @ %s:%d",
      nread, KVTREE_FILE_HASH_HEADER_SIZE, file, __FILE__, __LINE__);
    return -1;
  }

  /* track our current offset within the read buffer */
  size_t size = 0;

  /* read in the magic number, the type, and the version number */
  uint32_t magic;
  uint16_t type, version;
  kvtree_unpack_uint32_t(header, sizeof(header), &size, &magic);
  kvtree_unpack_uint16_t(header, sizeof(header), &size, &type);
  kvtree_unpack_uint16_t(header, sizeof(header), &size, &version);

  /* check that the magic number matches */
  /* check that the file type is something we understand */
  /* check that the file version matches */
  if (magic   != KVTREE_FILE_MAGIC ||
      type    != KVTREE_FILE_TYPE_HASH ||
      version != KVTREE_FILE_VERSION_HASH_1)
  {
    kvtree_err("File header does not match expected values in %s @ %s:%d",
      file, __FILE__, __LINE__
    );
    return -1;
  }

  /* read the file size */
  uint64_t filesize;
  kvtree_unpack_uint64_t(header, sizeof(header), &size, &filesize);

  /* read the flags field (32 bits) */
  uint32_t flags;
  kvtree_unpack_uint32_t(header, sizeof(header), &size, &flags);

  /* check that the filesize is valid (positive) */
  if (filesize < KVTREE_FILE_HASH_HEADER_SIZE) {
    kvtree_err("Invalid file size stored in %s @ %s:%d",
      file, __FILE__, __LINE__
    );
    return -1;
  }

  /* allocate a buffer to read the hash and crc */
  char* buf = (char*) KVTREE_MALLOC(filesize);

  /* copy the header into the buffer */
  memcpy(buf, header, size);

  /* read the rest of the file into the buffer */
  ssize_t remainder = filesize - size;
  if (remainder > 0) {
    nread = kvtree_read_attempt(file, fd, buf + size, remainder);
    if (nread < 0) {
      kvtree_err("Failed to read file %s @ %s:%d",
        file, __FILE__, __LINE__);
      kvtree_free(&buf);
      return -1;
    }
    if (nread != remainder) {
      kvtree_err("Failed to read file %s (read %zu bytes but expected %zu: filesize %zu) @ %s:%d",
        file, nread, remainder, filesize, __FILE__, __LINE__);
      kvtree_free(&buf);
      return -1;
    }
  }

  /* check the crc value if it's set */
  int crc_set = flags & KVTREE_FILE_FLAGS_CRC32;
  if (crc_set) {
    /* TODO: we should check that the remainder above is at least 4
     * (for the crc) */

    /* compute the crc value of the data */
    uLong crc = crc32(0L, Z_NULL, 0);
    crc = crc32(crc, (const Bytef*) buf, (uInt) filesize - sizeof(uint32_t));

    /* read the crc value */
    uint32_t crc_file_network, crc_file;
    memcpy(&crc_file_network, buf + filesize - sizeof(uint32_t), sizeof(uint32_t));
    crc_file = kvtree_ntoh32(crc_file_network);

    /* check the crc value */
    if (crc != crc_file) {
      kvtree_err("CRC32 mismatch detected in %s @ %s:%d",
        file, __FILE__, __LINE__
      );
      kvtree_free(&buf);
      return -1;
    }
  }

  /* create a temporary hash to read data into, unpack, and merge */
  kvtree* tmp_hash = kvtree_new();
  kvtree_unpack(buf + size, tmp_hash);
  kvtree_merge(hash, tmp_hash);
  kvtree_delete(&tmp_hash);

  /* free the buffer holding the file contents */
  kvtree_free(&buf);

  return filesize;
}

/** opens specified file and reads in a hash storing its contents in
 * the given hash object */
int kvtree_read_file(const char* file, kvtree* hash)
{
  /* check that we have a hash and a file name */
  if (file == NULL || hash == NULL) {
    return KVTREE_FAILURE;
  }

  /* can't read file, return error (special case so as not to print
   * error message below) */
  if (kvtree_file_is_readable(file) != KVTREE_SUCCESS) {
    return KVTREE_FAILURE;
  }

  /* open the hash file */
  int fd = kvtree_open(file, O_RDONLY);
  if (fd < 0) {
    kvtree_err("Opening hash file for read %s @ %s:%d",
      file, __FILE__, __LINE__
    );
    return KVTREE_FAILURE;
  }

  /* got the file open, be sure to close it even if we hit an error in
   * the read */
  int rc = KVTREE_SUCCESS;

  /* read the hash */
  ssize_t nread = kvtree_read_fd(file, fd, hash);
  if (nread < 0) {
    rc = KVTREE_FAILURE;
  }

  /* close the hash file */
  if (kvtree_close(file, fd) != KVTREE_SUCCESS) {
    rc = KVTREE_FAILURE;
  }

  return rc;
}

/*
 * Boilerplate code to check that 'hash' is valid, open 'file'
 * and acquire a write lock.
 *
 * Return a fd for 'file' >= 0 on success, -1 on failure.
 */
static
int kvtree_read_write_with_lock(const char* file, kvtree* hash, int write)
{
  /* check that we got a filename */
  if (file == NULL) {
    kvtree_err("No filename specified @ %s:%d",
      __FILE__, __LINE__
    );
    return -1;
  }

  /* check that we got a hash to read data into */
  if (hash == NULL) {
    kvtree_err("No hash provided to read data into @ %s:%d",
      __FILE__, __LINE__
    );
    return -1;
  }

  /* open file with lock for read / write access */
  mode_t mode_file = kvtree_getmode(1, 1, 0);
  int fd = kvtree_open_with_lock(file, O_RDWR | O_CREAT, mode_file, write);

  return fd;
}

/*
 * Read the kvtree from 'file' into 'hash'.  This is the locking version that
 * only allows one reader/writer to 'file' at a time.
 */
int kvtree_read_with_lock(const char* file, kvtree* hash)
{
  int fd = kvtree_read_write_with_lock(file, hash, 0);
  if (fd >= 0) {
    int rc;
    /* read the file into the hash */
    rc = kvtree_read_fd(file, fd, hash);
    if (rc == -1) {
      kvtree_err("Couldn't read file @ %s:%d", __FILE__, __LINE__);
      kvtree_close_with_unlock(file, fd);
      return KVTREE_FAILURE;
    }

    /* close the file and release the lock */
    rc = kvtree_close_with_unlock(file, fd);
    if (rc != 0) {
      return KVTREE_FAILURE;
    }

    return KVTREE_SUCCESS;
  } else {
    kvtree_err("Failed to open file with lock %s @ %s:%d",
      file, __FILE__, __LINE__);
  }

  return KVTREE_FAILURE;
}

/*
 * Write the kvtree from 'hash' to 'file'.  This is the locking version that
 * only allows one reader/writer to 'file' at a time.
 */
int kvtree_write_with_lock(const char* file, kvtree* hash)
{
  int fd = kvtree_read_write_with_lock(file, hash, 1);
  if (fd >= 0) {
    /* write the file into the hash */
    kvtree_write_fd(file, fd, hash);

    /* close the file and release the lock */
    kvtree_close_with_unlock(file, fd);

    return KVTREE_SUCCESS;
  } else {
    kvtree_err("Failed to open file with lock %s @ %s:%d",
      file, __FILE__, __LINE__
    );
  }

  return KVTREE_FAILURE;
}

/** given a filename and hash, lock the file, open it, and read it into
 * hash, set fd to the opened file descriptor */
int kvtree_lock_open_read(const char* file, int* fd, kvtree* hash)
{
  /* check that we got a pointer to a file descriptor */
  if (fd == NULL) {
    kvtree_err("Must provide a pointer to an int to return opened file descriptor @ %s:%d",
      __FILE__, __LINE__
    );
    return KVTREE_FAILURE;
  }

  /* we at least got a pointer to a file descriptor,
   * initialize it to -1 */
  *fd = -1;

  /* check that we got a filename */
  if (file == NULL) {
    kvtree_err("No filename specified @ %s:%d",
      __FILE__, __LINE__
    );
    return KVTREE_FAILURE;
  }

  /* check that we got a hash to read data into */
  if (hash == NULL) {
    kvtree_err("No hash provided to read data into @ %s:%d",
      __FILE__, __LINE__
    );
    return KVTREE_FAILURE;
  }

  /* open file with lock for read / write access */
  mode_t mode_file = kvtree_getmode(1, 1, 0);

  /*
   * NOTE: even though this function is kvtree_lock_open_read(), we still
   * take a write lock below since our code uses it for a read-modify-write.
   */
  *fd = kvtree_open_with_lock(file, O_RDWR | O_CREAT, mode_file, 1);
  if (*fd >= 0) {
    /* read the file into the hash */
    kvtree_read_fd(file, *fd, hash);
    return KVTREE_SUCCESS;
  }

  return KVTREE_FAILURE;
}

/** given a filename, an opened file descriptor, and a hash, overwrite
 * file with hash, close, and unlock file */
int kvtree_write_close_unlock(const char* file, int* fd, const kvtree* hash)
{
  /* check that we got a pointer to a file descriptor */
  if (fd == NULL) {
    kvtree_err("Must provide a pointer to an opened file descriptor @ %s:%d",
      __FILE__, __LINE__
    );
    return KVTREE_FAILURE;
  }

  /* check that the file descriptor is open */
  if (*fd < 0) {
    kvtree_err("File descriptor does not point to a valid file @ %s:%d",
      __FILE__, __LINE__
    );
    return KVTREE_FAILURE;
  }

  /* check that we got a filename */
  if (file == NULL) {
    kvtree_err("No filename specified @ %s:%d",
      __FILE__, __LINE__
    );
    return KVTREE_FAILURE;
  }

  /* check that we got a hash to read data into */
  if (hash == NULL) {
    kvtree_err("No hash provided to write data from @ %s:%d",
      __FILE__, __LINE__
    );
    return KVTREE_FAILURE;
  }

  if (*fd >= 0) {
    /* wind the file pointer back to the start of the file */
    lseek(*fd, 0, SEEK_SET);

    /* write the updated hash back to the file */
    ssize_t nwrite = kvtree_write_fd(file, *fd, hash);

    /* truncate file to new size */
    if (nwrite >= 0) {
      if (ftruncate(*fd, (off_t) nwrite) == -1) {
        kvtree_err("ftruncate() failed: errno=%d (%s) @ %s:%d",
          errno, strerror(errno), __FILE__, __LINE__
        );
      }
    }

    /* close the file and release the lock */
    kvtree_close_with_unlock(file, *fd);

    /* mark the file descriptor as closed */
    *fd = -1;
  }

  return KVTREE_SUCCESS;
}

/** write kvtree as gather/scatter file, input kvtree must be in form:
 *   0
 *     <kvtree_for_rank_0>
 *   1
 *     <kvtree_for_rank_1> */
int kvtree_write_to_gather(const char* prefix, kvtree* data, int ranks)
{
  int rc = KVTREE_SUCCESS;
  /* record up to 8K entries per file */
  long entries_per_file = 8192;

  /*
   * KVTREE_ENTRIES_PER_FILE is only used for testing.  Specifically, you can
   * set it to something low like 1 to force kvwrite_write_to_gather() to write
   * multiple kvtree files.
   */
  if (getenv("KVTREE_ENTRIES_PER_FILE")) {
    entries_per_file = atol(getenv("KVTREE_ENTRIES_PER_FILE"));
  }

  /* we hardcode this to be two levels deep */

  /* sort so that elements are ordered by rank value */
  kvtree_sort_int(data, KVTREE_SORT_ASCENDING);

  /* create hash for primary map and encode level */
  kvtree* files_hash = kvtree_new();
  kvtree_set_kv_int(files_hash, "LEVEL", 1);

  /* iterate over each rank to record its info */
  int writer = 0;
  int max_rank = -1;
  kvtree_elem* elem = kvtree_elem_first(data);
  while (elem != NULL) {
    /* create a hash to record an entry from each rank */
    kvtree* entries = kvtree_new();
    kvtree_set_kv_int(entries, "LEVEL", 0);

    /* record the total number of ranks in each file */
    kvtree_set_kv_int(entries, "RANKS", ranks);

    int count = 0;
    while (count < entries_per_file) {
      /* get rank id */
      int rank = kvtree_elem_key_int(elem);
      if (rank > max_rank) {
        max_rank = rank;
      }

      /* copy hash of current rank under RANK/<rank> in entries */
      kvtree* elem_hash = kvtree_elem_hash(elem);
      kvtree* rank_hash = kvtree_set_kv_int(entries, "RANK", rank);
      kvtree_merge(rank_hash, elem_hash);
      count++;

      /* break early if we reach the end */
      elem = kvtree_elem_next(elem);
      if (elem == NULL) {
        break;
      }
    }

    /* build name for part file */
    char filename[1024];
    snprintf(filename, sizeof(filename), "%s.0.%d", prefix, writer);

    /* write hash to file */
    if (kvtree_write_file(filename, entries) != KVTREE_SUCCESS) {
      rc = KVTREE_FAILURE;
      elem = NULL;
    }

    /* delete part hash and path */
    kvtree_delete(&entries);

    /* record file name of part in files hash, relative to prefix directory */
    char partname[1024];
    snprintf(partname, sizeof(partname), ".0.%d", writer);
    unsigned long offset = 0;
    kvtree* files_rank_hash = kvtree_set_kv_int(files_hash, "RANK", writer);
    kvtree_util_set_str(files_rank_hash, "FILE", partname);
    kvtree_util_set_bytecount(files_rank_hash, "OFFSET", offset);

    /* get id of next writer */
    writer += count;
  }

  /* record total number of ranks in job as max rank + 1 */
  kvtree_set_kv_int(files_hash, "RANKS", ranks);

  /* write out root file */
  if (kvtree_write_file(prefix, files_hash) != KVTREE_SUCCESS) {
    rc = KVTREE_FAILURE;
  }
  kvtree_delete(&files_hash);

  return rc;
}

/*
 * Given a prefix passed to a kvtree_write_gather() (like "/tmp/rank2file",
 * read all the rank data from all the scattered subfiles, and construct a
 * kvtree in 'data'.  'data' will look something like:
 *
 *       26
 *         FILE
 *           ckpt.1/rank_26.ckpt
 *       24
 *         FILE
 *           ckpt.1/rank_24.ckpt
 *       25
 *         FILE
 *           ckpt.1/rank_25.ckpt
 *       18
 *         FILE
 *           ckpt.1/rank_18.ckpt
 *       ...
 *
 *  ... with each top-level element being the rank number.
 */
int kvtree_read_scatter_single(const char* prefix, kvtree* data)
{
  /*
   * Read in high level kvtree.
   */
  kvtree* final_tree = kvtree_new();
  int rc = kvtree_read_file(prefix, final_tree);
  if (rc != KVTREE_SUCCESS) {
    kvtree_delete(&final_tree);
    kvtree_err("Couldn't read in high level kvtree file %s\n", prefix);
    return rc;
  }

  unsigned long expected_ranks;
  rc = kvtree_util_get_unsigned_long(final_tree, "RANKS", &expected_ranks);
  if (rc != KVTREE_SUCCESS) {
    kvtree_delete(&final_tree);
    kvtree_err("Couldn't get RANKS for %s\n", prefix);
    return rc;
  }
  kvtree_delete(&final_tree);

  /* Start construction of the final tree that we will return */
  final_tree = kvtree_new();

  /* Look at all the subfiles with our prefix and read in the file lists */
  char* prefix_dir_copy = strdup(prefix);
  char* prefix_dir = dirname(prefix_dir_copy);
  char* prefix_file_copy = strdup(prefix);
  char* prefix_file = basename(prefix_file_copy);

  /* Create our regex to match our prefix files */
  char pattern[PATH_MAX];
  snprintf(pattern, sizeof(pattern), "^%s\\.0\\.[0-9]+$", prefix_file);
  pattern[sizeof(pattern) - 1] = '\0';

  regex_t regex;
  rc = regcomp(&regex, pattern, REG_EXTENDED);
  if (rc) {
    rc = KVTREE_FAILURE;
    goto end;
  }

  DIR* d = opendir(prefix_dir);
  if (!d) {
    rc = KVTREE_FAILURE;
    goto end;
  }

  /* For each file/dir in our prefix dir */
  unsigned long actual_ranks = 0;
  struct dirent* dir;
  while ((dir = readdir(d)) != NULL) {
    /*
     * Search for any file starting with "prefix_file."
     * like "rank2file.".
     */
    /* Is this one of our subfiles?  It should have .0.x in the extension */
    rc = regexec(&regex, dir->d_name, 0, NULL, 0);
    if (rc == 0) {  /* Did we get both numbers in the extension? */
      /* subfile matches */

      /* Construct the full path to the subfile kvtree */
      char tmp[PATH_MAX];
      memset(tmp, 0, sizeof(tmp));
      snprintf(tmp, sizeof(tmp), "%s/%s", prefix_dir, dir->d_name);
      tmp[sizeof(tmp) - 1] = '\0';

      /* Read in the subfile */
      kvtree* subfile_tree = kvtree_new();
      rc = kvtree_read_file(tmp, subfile_tree);
      if (rc == KVTREE_SUCCESS) {
        /* Sanity: each subfile we want will have LEVEL=0 */
        unsigned long level;
        rc = kvtree_util_get_unsigned_long(subfile_tree, "LEVEL", &level);
        if (rc == KVTREE_SUCCESS && level == 0) {
          /* Break off and remove the "RANK" subtree from the rank2file tree */
          kvtree* subfile_rank_tree = kvtree_extract(subfile_tree, "RANK");
          if (subfile_rank_tree) {
            if (kvtree_merge(final_tree, subfile_rank_tree) == KVTREE_SUCCESS) {
              actual_ranks += kvtree_size(subfile_rank_tree);
            }
          }
        }
      }
      kvtree_delete(&subfile_tree);
    }
  }
  closedir(d);
  kvtree_merge(data, final_tree);
  rc = KVTREE_SUCCESS;

end:
  kvtree_free(&prefix_dir_copy);
  kvtree_free(&prefix_file_copy);
  kvtree_delete(&final_tree);
  if (rc != KVTREE_SUCCESS || actual_ranks != expected_ranks) {
    return KVTREE_FAILURE;
  }

  return KVTREE_SUCCESS;
}
///@}

/* ================================================= */
/** @name Print hash and elements to stdout for debugging */
///@{

/** prints specified hash element to stdout for debugging */
static int kvtree_elem_print(const kvtree_elem* elem, int indent, int mode)
{
  char tmp[KVTREE_MAX_FILENAME];
  int i;
  for (i = 0; i < indent; i++) {
    tmp[i] = ' ';
  }
  tmp[indent] = '\0';

  if (mode == KVTREE_PRINT_TREE) {
    if (elem != NULL) {
      if (elem->key != NULL) {
        printf("%s%s\n", tmp, elem->key);
      } else {
        printf("%sNULL KEY\n", tmp);
      }
      kvtree_print_mode(elem->hash, indent, mode);
    } else {
      printf("%sNULL ELEMENT\n", tmp);
    }

    return KVTREE_SUCCESS;
  }

  if (mode == KVTREE_PRINT_KEYVAL) {
    if (elem != NULL) {
      if (elem->key != NULL) {
        if (kvtree_size(elem->hash) == 1) {
          /* hash for current element has one value, this may be a key/value pair */
          kvtree_elem* elem2 = kvtree_elem_first(elem->hash);
          if (elem2->hash == NULL || kvtree_size(elem2->hash) == 0) {
            /* my hash has one value, and the hash for that one value
             * is empty, so print this as a key/value pair */
            printf("%s%s = %s\n", tmp, elem->key, elem2->key);
          } else {
            /* my hash has one value, but the hash for that one value is
             * non-empty so we need to recurse into hash */
            printf("%s%s\n", tmp, elem->key);
            kvtree_print_mode(elem->hash, indent, mode);
          }
        } else {
          /* hash for current element has 0 or more than one items,
           * so we need to recurse into hash */
          printf("%s%s\n", tmp, elem->key);
          kvtree_print_mode(elem->hash, indent, mode);
        }
      } else {
        printf("%sNULL KEY\n", tmp);
        kvtree_print_mode(elem->hash, indent, mode);
      }
    } else {
      printf("%sNULL ELEMENT\n", tmp);
    }

    return KVTREE_SUCCESS;
  }

  return KVTREE_SUCCESS;
}

/** prints specified hash to stdout for debugging */
int kvtree_print_mode(const kvtree* hash, int indent, int mode)
{
  char tmp[KVTREE_MAX_FILENAME];
  int i;
  for (i = 0; i < indent; i++) {
    tmp[i] = ' ';
  }
  tmp[indent] = '\0';

  if (hash != NULL) {
    kvtree_elem* elem;
    LIST_FOREACH(elem, hash, pointers) {
      kvtree_elem_print(elem, indent + 2, mode);
    }
  } else {
    printf("%sNULL LIST\n", tmp);
  }
  return KVTREE_SUCCESS;
}

/** prints specified hash to stdout for debugging */
int kvtree_print(const kvtree* hash, int indent)
{
  return kvtree_print_mode(hash, indent, KVTREE_PRINT_TREE);
}

/** logs specified hash element to stdout for debugging */
static int kvtree_elem_log(const kvtree_elem* elem, int log_level, int indent)
{
  char tmp[KVTREE_MAX_FILENAME];
  int i;
  for (i = 0; i < indent; i++) {
    tmp[i] = ' ';
  }
  tmp[indent] = '\0';

  if (elem != NULL) {
    if (elem->key != NULL) {
      kvtree_dbg(log_level, "%s%s\n", tmp, elem->key);
    } else {
      kvtree_dbg(log_level, "%sNULL KEY\n", tmp);
    }
    kvtree_log(elem->hash, log_level, indent);
  } else {
    kvtree_dbg(log_level, "%sNULL ELEMENT\n", tmp);
  }
  return KVTREE_SUCCESS;
}

/** prints specified hash to stdout for debugging */
int kvtree_log(const kvtree* hash, int log_level, int indent)
{
  char tmp[KVTREE_MAX_FILENAME];
  int i;
  for (i = 0; i < indent; i++) {
    tmp[i] = ' ';
  }
  tmp[indent] = '\0';

  if (hash != NULL) {
    kvtree_elem* elem;
    LIST_FOREACH(elem, hash, pointers) {
      kvtree_elem_log(elem, log_level, indent+2);
    }
  } else {
    kvtree_dbg(log_level, "%sNULL LIST\n", tmp);
  }
  return KVTREE_SUCCESS;
}
///@}

/* ================================================= */
#ifdef HAVE_TV
/** @name Pretty print for TotalView debug window */
///@{

/** This enables a nicer display when diving on a hash variable
 *  under the TotalView debugger.  It requires TV 8.8 or later. */

#include "tv_data_display.h"

static int TV_ttf_display_type(const kvtree* hash)
{
  if (hash == NULL) {
    /* empty hash, nothing to display here */
    return TV_ttf_format_ok;
  }

  kvtree_elem* elem = NULL;
  for (elem = kvtree_elem_first(hash);
       elem != NULL;
       elem = kvtree_elem_next(elem))
  {
    /* get the key name */
    char* key = kvtree_elem_key(elem);

    /* get a pointer to the hash for this key */
    kvtree* h = kvtree_elem_hash(elem);

    /* add a row to the TV debug window */
    if (h != NULL) {
      /* compute the size of the hash for this key */
      int size = kvtree_size(h);

      if (size == 0) {
        /* if the hash is empty, stop at the key */
        TV_ttf_add_row("value", TV_ttf_type_ascii_string, key);
      } else if (size == 1) {
        /* my hash has one value, this may be a key/value pair */
        char* value = kvtree_elem_get_first_val(hash, key);
        kvtree* h2 = kvtree_get(h, value);
        if (h2 == NULL || kvtree_size(h2) == 0) {
          /* my hash has one value, and the hash for that one value
           * is empty, so print this as a key/value pair */
          TV_ttf_add_row(key, TV_ttf_type_ascii_string, value);
        } else {
          /* my hash has one value, but the hash for that one value is
           * non-empty so we need to recurse into hash */
          TV_ttf_add_row(key, "kvtree", h);
        }
      } else {
        /* the hash for this key contains multiple elements,
         * so this is not a key/value pair, which means we need to
         * recurse into hash */
        TV_ttf_add_row(key, "kvtree", h);
      }
    } else {
      /* this key has a NULL hash, so stop at the key */
      TV_ttf_add_row("value", TV_ttf_type_ascii_string, key);
    }
  }

  return TV_ttf_format_ok;
}
///@}
#endif /* HIDE_TV */
