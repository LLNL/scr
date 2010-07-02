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
#include "scr_hash.h"
#include "scr_io.h"

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

#define SCR_HASH_FILE_MAGIC     (0x951fc3f5)
#define SCR_HASH_FILE_TYPE      (1)
#define SCR_HASH_FILE_VERSION_1 (1)

/*
=========================================
Allocate and delete hash objects
=========================================
*/

/* allocates a new hash element */
static struct scr_hash_elem* scr_hash_elem_new()
{
  struct scr_hash_elem* elem = (struct scr_hash_elem*) malloc(sizeof(struct scr_hash_elem));
  if (elem != NULL) {
    elem->key  = NULL;
    elem->hash = NULL;
  }
  return elem;
}

/* frees a hash element */
static int scr_hash_elem_delete(struct scr_hash_elem* elem)
{
  if (elem != NULL) {
    /* free the key which was strdup'ed */
    if (elem->key != NULL) {
      free(elem->key);
      elem->key = NULL;
    }

    /* free the hash */
    scr_hash_delete(elem->hash);
    elem->hash = NULL;

    /* finally, free the element structure itself */
    free(elem);
  } 
  return SCR_SUCCESS;
}

/* allocates a new hash */
struct scr_hash* scr_hash_new()
{
  struct scr_hash* hash = (struct scr_hash*) malloc(sizeof(struct scr_hash));
  if (hash != NULL) {
    LIST_INIT(hash);
  }
  return hash;
}

/* frees a hash */
int scr_hash_delete(struct scr_hash* hash)
{
  if (hash != NULL) {
    while (!LIST_EMPTY(hash)) {
      struct scr_hash_elem* elem = LIST_FIRST(hash);
      LIST_REMOVE(elem, pointers);
      scr_hash_elem_delete(elem);
    }
    free(hash);
  }
  return SCR_SUCCESS;
}

/*
=========================================
size, get, set, unset, and merge functions
=========================================
*/

/* return size of hash (number of keys) */
int scr_hash_size(const struct scr_hash* hash)
{
  int count = 0;
  if (hash != NULL) {
    struct scr_hash_elem* elem;
    LIST_FOREACH(elem, hash, pointers) {
      count++;
    }
  }
  return count;
}

/* given a hash and a key, return the hash associated with key, returns NULL if not found */
struct scr_hash* scr_hash_get(const struct scr_hash* hash, const char* key)
{
  struct scr_hash_elem* elem = scr_hash_elem_get(hash, key);
  if (elem != NULL) {
    return elem->hash;
  }
  return NULL;
}

/* given a hash, a key, and a hash value, set (or reset) the key's hash and return the pointer to the new hash */
struct scr_hash* scr_hash_set(struct scr_hash* hash, const char* key, struct scr_hash* hash_value)
{
  if (hash == NULL) {
    return NULL;
  }

  /* if there is a match in the hash, pull out that element */
  struct scr_hash_elem* elem = scr_hash_elem_extract(hash, key);
  if (elem == NULL) {
    /* nothing found, so create a new element and set it */
    elem = scr_hash_elem_new();
    scr_hash_elem_set(elem, key, hash_value);
  } else {
    /* this key already exists, delete its current hash and reset it */
    if (elem->hash != NULL) {
      scr_hash_delete(elem->hash);
    }
    elem->hash = hash_value;
  }
  /* insert the element into the hash */
  LIST_INSERT_HEAD(hash, elem, pointers);

  /* return the pointer to the hash of the element */
  return elem->hash;
}

/* given a hash and a key, extract and delete any matching element */
int scr_hash_unset(struct scr_hash* hash, const char* key)
{
  if (hash == NULL) {
    return SCR_SUCCESS;
  }

  struct scr_hash_elem* elem = scr_hash_elem_extract(hash, key);
  if (elem != NULL) {
    scr_hash_elem_delete(elem);
  }
  return SCR_SUCCESS;
}

/* unset all values in the hash, but don't delete it */
int scr_hash_unset_all(struct scr_hash* hash)
{
  struct scr_hash_elem* elem = scr_hash_elem_first(hash);
  while (elem != NULL) {
    /* remember this element */
    struct scr_hash_elem* tmp = elem;

    /* get the next element */
    elem = scr_hash_elem_next(elem);

    /* extract and delete the current element by address */
    scr_hash_elem_extract_by_addr(hash, tmp);
    scr_hash_elem_delete(tmp);
  }
  return SCR_SUCCESS;
}

/* merges (copies) elements from hash2 into hash1 */
int scr_hash_merge(struct scr_hash* hash1, const struct scr_hash* hash2)
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
  struct scr_hash_elem* elem;
  for (elem = scr_hash_elem_first(hash2);
       elem != NULL;
       elem = scr_hash_elem_next(elem))
  {
    /* get the key for this element */
    char* key = scr_hash_elem_key(elem);

    /* get hash for the matching element in hash1, if it has one */
    struct scr_hash* key_hash1 = scr_hash_get(hash1, key);
    if (key_hash1 == NULL) {
      /* hash1 had no element with this key, so create one */
      key_hash1 = scr_hash_set(hash1, key, scr_hash_new());
    }

    /* merge the hash for this key from hash2 with the hash for this key from hash1 */
    struct scr_hash* key_hash2 = scr_hash_elem_hash(elem);
    if (scr_hash_merge(key_hash1, key_hash2) != SCR_SUCCESS) {
      rc = SCR_FAILURE;
    }
  }

  return rc;
}

/* traverse the given hash using a printf-like format string setting an arbitrary list of keys
 * to set (or reset) the hash associated with the last-most key */
struct scr_hash* scr_hash_setf(struct scr_hash* hash, struct scr_hash* hash_value, const char* format, ...)
{
  /* check that we have a hash */
  if (hash == NULL) {
    return NULL;
  }

  struct scr_hash* h = hash;

  /* make a copy of the format specifier, since strtok will clobber it */
  char* format_copy = strdup(format);
  if (format_copy == NULL) {
    scr_err("Failed to duplicate format string @ %s:%d",
            __FILE__, __LINE__
    );
    exit(1);
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
  if (format_copy != NULL) {
    free(format_copy);
  }

  /* make a copy of the format specifier, since strtok will clobber it */
  format_copy = strdup(format);
  if (format_copy == NULL) {
    scr_err("Failed to duplicate format string @ %s:%d",
            __FILE__, __LINE__
    );
    exit(1);
  }

  /* for each format specifier, convert the next key argument to a string and look up the hash for that key */
  va_list args;
  va_start(args, format);
  token = strtok(format_copy, search);
  int i = 0;
  while (i < count && token != NULL && h != NULL) {
    /* interpret the format and convert the current key argument to a string */
    char key[SCR_MAX_LINE];
    int size = 0;
    if (strcmp(token, "%s") == 0) {
      size = snprintf(key, sizeof(key), token, va_arg(args, char*));
    } else if (strcmp(token, "%d")  == 0) {
      size = snprintf(key, sizeof(key), token, va_arg(args, int));
    } else if (strcmp(token, "%lu") == 0) {
      size = snprintf(key, sizeof(key), token, va_arg(args, unsigned long));
    } else if (strcmp(token, "%#lx") == 0) {
      size = snprintf(key, sizeof(key), token, va_arg(args, unsigned long));
    } else if (strcmp(token, "%llu") == 0) {
      size = snprintf(key, sizeof(key), token, va_arg(args, unsigned long long));
    } else if (strcmp(token, "%f") == 0) {
      size = snprintf(key, sizeof(key), token, va_arg(args, double));
    } else {
      scr_err("Unsupported hash key format '%s' @ %s:%d",
              token, __FILE__, __LINE__
      );
      exit(1);
    }

    /* check that we were able to fit the string into our buffer */
    if (size >= sizeof(key)) {
      scr_err("Key buffer too small, have %lu need %d bytes @ %s:%d",
              sizeof(key), size, __FILE__, __LINE__
      );
      exit(1);
    }

    if (i < count-1) {
      /* check whether we have an entry for this key in the current hash */
      struct scr_hash* tmp = scr_hash_get(h, key);

      /* didn't find an entry for this key, so create one */
      if (tmp == NULL) {
        tmp = scr_hash_set(h, key, scr_hash_new());
      }

      /* now we have a hash for this key, continue with the next key */
      h = tmp;
    } else {
      /* we are at the last key, so set its hash using the value provided by the caller */
      h = scr_hash_set(h, key, hash_value);
    }

    /* get the next format string */
    token = strtok(NULL, search);
    i++;
  }
  va_end(args);

  /* free our copy of the format specifier */
  if (format_copy != NULL) {
    free(format_copy);
  }

  /* return the hash we found */
  return h;
}

static int scr_hash_int_cmp_fn(const void* a, const void* b)
{
  return (int) (*(int*)a - *(int*)b);
}

/* sort the hash assuming the keys are ints */
int scr_hash_sort_int(struct scr_hash* hash)
{
  /* get the size of the hash */
  int count = scr_hash_size(hash);

  /* define a structure to hold the key and elem address */
  struct sort_elem {
    int key;
    struct scr_hash_elem* addr;
  };

  /* allocate space for each element */
  struct sort_elem* list = (struct sort_elem*) malloc(count * sizeof(struct sort_elem));
  if (list == NULL) {
    return SCR_FAILURE;
  }

  /* walk the hash and fill in the keys */
  struct scr_hash_elem* elem = NULL;
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
  qsort(list, count, sizeof(struct sort_elem), &scr_hash_int_cmp_fn);

  /* walk the sorted list backwards, extracting the element by address, and inserting at the head */
  while (index > 0) {
    index--;
    elem = list[index].addr;
    LIST_REMOVE(elem, pointers);
    LIST_INSERT_HEAD(hash, elem, pointers);
  }

  /* free the list */
  if (list != NULL) {
    free(list);
    list = NULL;
  }

  return SCR_SUCCESS;
}

/*
=========================================
get, set, and unset hashes using a key/value pair
=========================================
*/

/* shortcut to create a key and subkey in a hash with one call */
struct scr_hash* scr_hash_set_kv(struct scr_hash* hash, const char* key, const char* val)
{
  if (hash == NULL) {
    return NULL;
  }

  struct scr_hash* k = scr_hash_get(hash, key);
  if (k == NULL) {
    k = scr_hash_set(hash, key, scr_hash_new());
  }

  struct scr_hash* v = scr_hash_get(k, val);
  if (v == NULL) {
    v = scr_hash_set(k, val, scr_hash_new());
  }

  return v;
}

/* same as scr_hash_set_kv, but with the subkey specified as an int */
struct scr_hash* scr_hash_set_kv_int(struct scr_hash* hash, const char* key, int val)
{
  /* TODO: this feels kludgy, but I guess as long as the ASCII string is longer
   * than a max int (or minimum int with leading minus sign) which is 11 chars, we're ok
   * ("-2147483648" to "2147483647") */
  char tmp[100];
  sprintf(tmp, "%d", val);
  return scr_hash_set_kv(hash, key, tmp);
}

/* shortcut to get hash assocated with the subkey of a key in a hash with one call */
struct scr_hash* scr_hash_get_kv(const struct scr_hash* hash, const char* key, const char* val)
{
  if (hash == NULL) {
    return NULL;
  }

  struct scr_hash* k = scr_hash_get(hash, key);
  if (k == NULL) {
    return NULL;
  }

  struct scr_hash* v = scr_hash_get(k, val);
  if (v == NULL) {
    return NULL;
  }

  return v;
}

/* same as scr_hash_get_kv, but with the subkey specified as an int */
struct scr_hash* scr_hash_get_kv_int(const struct scr_hash* hash, const char* key, int val)
{
  /* TODO: this feels kludgy, but I guess as long as the ASCII string is longer
   * than a max int (or minimum int with leading minus sign) which is 11 chars, we're ok
   * ("-2147483648" to "2147483647") */
  char tmp[100];
  sprintf(tmp, "%d", val);
  return scr_hash_get_kv(hash, key, tmp);
}

/* unset subkey under key, and if that removes the only element for key, unset key as well */
int scr_hash_unset_kv(struct scr_hash* hash, const char* key, const char* val)
{
  if (hash == NULL) {
    return SCR_SUCCESS;
  }

  struct scr_hash* v = scr_hash_get(hash, key);
  int rc = scr_hash_unset(v, val);
  if (scr_hash_size(v) == 0) {
    rc = scr_hash_unset(hash, key);
  }

  return rc;
}

/* same as scr_hash_unset_kv, but with the subkey specified as an int */
int scr_hash_unset_kv_int(struct scr_hash* hash, const char* key, int val)
{
  /* TODO: this feels kludgy, but I guess as long as the ASCII string is longer
   * than a max int (or minimum int with leading minus sign) which is 11 chars, we're ok
   * ("-2147483648" to "2147483647") */
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
struct scr_hash_elem* scr_hash_elem_first(const struct scr_hash* hash)
{
  if (hash == NULL) {
    return NULL;
  }
  struct scr_hash_elem* elem = LIST_FIRST(hash);
  return elem;
}

/* given a hash element, returns the next element */
struct scr_hash_elem* scr_hash_elem_next(const struct scr_hash_elem* elem)
{
  if (elem == NULL) {
    return NULL;
  }
  struct scr_hash_elem* next = LIST_NEXT(elem, pointers);
  return next;
}

/* returns a pointer to the key of the specified element */
char* scr_hash_elem_key(const struct scr_hash_elem* elem)
{
  if (elem == NULL) {
    return NULL;
  }
  char* key = elem->key;
  return key;
}

/* same as scr_hash_elem_key, but converts the key as an int (returns 0 if key is not defined) */
int scr_hash_elem_key_int(const struct scr_hash_elem* elem)
{
  if (elem == NULL) {
    return 0;
  }
  int i = atoi(elem->key);
  return i;
}

/* returns a pointer to the hash of the specified element */
struct scr_hash* scr_hash_elem_hash(const struct scr_hash_elem* elem)
{
  if (elem == NULL) {
    return NULL;
  }
  struct scr_hash* hash = elem->hash;
  return hash;
}

/* given an element, set its key and hash fields (NOTE: not complement of get as written) */
struct scr_hash_elem* scr_hash_elem_set(struct scr_hash_elem* elem, const char* key, struct scr_hash* hash)
{
  if (elem != NULL) {
    if (key != NULL) {
      elem->key = strdup(key);
    } else {
      elem->key = NULL;
    }
    elem->hash = hash;
  }
  return elem;
}

/* given a hash and a key, find first matching element and return its address, returns NULL if not found */
struct scr_hash_elem* scr_hash_elem_get(const struct scr_hash* hash, const char* key)
{
  if (hash == NULL) {
    return NULL;
  }

  struct scr_hash_elem* elem;
  LIST_FOREACH(elem, hash, pointers) {
    if (elem->key != NULL && strcmp(elem->key, key) == 0) {
      return elem;
    }
  }
  return NULL;
}

/* given a hash and a key, return a pointer to the key of the first element of that key's hash */
char* scr_hash_elem_get_first_val(const struct scr_hash* hash, const char* key)
{
  /* lookup the hash, then return a pointer to the key of the first element */
  char* v = NULL;
  struct scr_hash* h = scr_hash_get(hash, key);
  if (h != NULL) {
    struct scr_hash_elem* e = scr_hash_elem_first(h);
    if (e != NULL) {
      v = scr_hash_elem_key(e);
    }
  }
  return v;
}

/* given a hash and a key, find first matching element, remove it from the hash, and return it */
struct scr_hash_elem* scr_hash_elem_extract(struct scr_hash* hash, const char* key)
{
  struct scr_hash_elem* elem = scr_hash_elem_get(hash, key);
  if (elem != NULL) {
    LIST_REMOVE(elem, pointers);
  }
  return elem;
}

/* extract element from hash given the hash and the address of the element */
struct scr_hash_elem* scr_hash_elem_extract_by_addr(struct scr_hash* hash, struct scr_hash_elem* elem)
{
  /* TODO: check that elem is really in hash */
  LIST_REMOVE(elem, pointers);
  return elem;
}

/*
=========================================
Pack and unpack hash and elements into a char buffer
=========================================
*/

/* computes the number of bytes needed to pack the given hash element */
static size_t scr_hash_get_pack_size_elem(const struct scr_hash_elem* elem)
{
  size_t size = 0;
  if (elem != NULL) {
    if (elem->key != NULL) {
      size += strlen(elem->key) + 1;
    } else {
      size += 1;
    }
    size += scr_hash_get_pack_size(elem->hash);
  } else {
    size += 1;
    size += scr_hash_get_pack_size(NULL);
  }
  return size;
}

/* packs a hash element into specified buf and returns the number of bytes written */
static size_t scr_hash_pack_elem(char* buf, const struct scr_hash_elem* elem)
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

/* unpacks hash element from specified buffer and returns the number of bytes read and a pointer to a newly allocated hash */
static size_t scr_hash_unpack_elem(const char* buf, struct scr_hash_elem* elem)
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
  struct scr_hash* hash = scr_hash_new();
  size += scr_hash_unpack(buf + size, hash);
  
  /* set our elem with the key and hash values we unpacked */
  scr_hash_elem_set(elem, key, hash);

  return size;
}

/* computes the number of bytes needed to pack the given hash */
size_t scr_hash_get_pack_size(const struct scr_hash* hash)
{
  size_t size = 0;
  if (hash != NULL) {
    struct scr_hash_elem* elem;

    /* add the size required to store the COUNT */
    size += sizeof(uint32_t);

    /* finally add the size of each element */
    LIST_FOREACH(elem, hash, pointers) {
      size += scr_hash_get_pack_size_elem(elem);
    }  
  } else {
    size += sizeof(uint32_t);
  }
  return size;
}

/* packs the given hash into specified buf and returns the number of bytes written */
size_t scr_hash_pack(char* buf, const struct scr_hash* hash)
{
  size_t size = 0;
  if (hash != NULL) {
    struct scr_hash_elem* elem;

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
      size += scr_hash_pack_elem(buf + size, elem);
    }
  } else {
    /* no hash -- just pack the count of 0 */
    uint32_t count_network = scr_hton32((uint32_t) 0);
    memcpy(buf + size, &count_network, sizeof(uint32_t));
    size += sizeof(uint32_t);
  }
  return size;
}

/* unpacks hash from specified buffer and returns the number of bytes read and a pointer to a newly allocated hash */
size_t scr_hash_unpack(const char* buf, struct scr_hash* hash)
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
    struct scr_hash_elem* elem = scr_hash_elem_new();
    size += scr_hash_unpack_elem(buf + size, elem);
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

/* executes logic of scr_has_write with opened file descriptor */
ssize_t scr_hash_write_fd(const char* file, int fd, const struct scr_hash* hash)
{
  ssize_t nwrite = 0;

  /* check that we have a hash, a file name, and a file descriptor */
  if (file == NULL || fd < 0 || hash == NULL) {
    return -1;
  }

  /* allocate a buffer to pack the hash in */
  size_t pack_size = scr_hash_get_pack_size(hash);
  uint64_t filesize = 16 + pack_size + 4;
  char* buf = (char*) malloc(filesize);
  if (buf == NULL) {
    scr_err("Allocating %lu byte buffer to write hash to %s @ %s:%d",
            (unsigned long) filesize, file, __FILE__, __LINE__
    );
    return -1;
  }

  size_t size = 0;

  /* write the SCR file magic number */
  uint32_t magic = SCR_HASH_FILE_MAGIC;
  uint32_t magic_network = scr_hton32(magic);
  memcpy(buf + size, &magic_network, sizeof(uint32_t));
  size += sizeof(uint32_t);

  /* write the file type */
  uint16_t type = SCR_HASH_FILE_TYPE;
  uint16_t type_network = scr_hton16(type);
  memcpy(buf + size, &type_network, sizeof(uint16_t));
  size += sizeof(uint16_t);

  /* write the version number */
  uint16_t version = SCR_HASH_FILE_VERSION_1;
  uint16_t version_network = scr_hton16(version);
  memcpy(buf + size, &version_network, sizeof(uint16_t));
  size += sizeof(uint16_t);

  /* write the pack length of the hash data */
  uint64_t filesize_network = scr_hton64((uint64_t) filesize);
  memcpy(buf + size, &filesize_network, sizeof(uint64_t));
  size += sizeof(uint64_t);

  /* pack the hash into the buffer */
  size += scr_hash_pack(buf + size, hash);

  /* compute the crc over the length of the file */
  uLong crc = crc32(0L, Z_NULL, 0);
  crc = crc32(crc, (const Bytef*) buf, (uInt) size);

  /* write the crc to the buffer */
  uint32_t crc_network = scr_hton32((uint32_t) crc);
  memcpy(buf + size, &crc_network, sizeof(uint32_t));
  size += sizeof(uint32_t);

  /* write the hash to the file */
  nwrite = scr_write_attempt(file, fd, buf, size);

  /* free the pack buffer */
  free(buf);
  buf = NULL;

  /* if we didn't write all of the bytes, return an error */
  if (nwrite != size) {
    return -1;
  }

  return nwrite;
}

/* write the given hash to specified file */
int scr_hash_write(const char* file, const struct scr_hash* hash)
{
  int rc = SCR_SUCCESS;

  /* check that we have a hash and a file name */
  if (file == NULL || hash == NULL) {
    return SCR_FAILURE;
  }

  /* open the hash file */
  int fd = scr_open(file, O_WRONLY | O_CREAT | O_TRUNC, S_IRUSR | S_IWUSR);
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

/* executes logic of scr_hash_read using an opened file descriptor */
ssize_t scr_hash_read_fd(const char* file, int fd, struct scr_hash* hash)
{
  ssize_t nread;
  ssize_t size = 0;

  /* check that we have a hash, a file name, and a file descriptor */
  if (file == NULL || fd < 0 || hash == NULL) {
    return -1;
  }

  /* create a temporary hash to read data into */
  struct scr_hash* tmp_hash = scr_hash_new();
  if (tmp_hash == NULL) {
    return -1;
  }

  /* read in the file header */
  char header[16];
  nread = scr_read_attempt(file, fd, header, 16);
  if (nread != 16) {
    scr_hash_delete(tmp_hash);
    return -1;
  }

  /* read in the magic number */
  uint32_t magic_network, magic;
  memcpy(&magic_network, header + size, sizeof(uint32_t));
  magic = scr_ntoh32(magic_network);
  size += sizeof(uint32_t);

  /* read in the file type */
  uint16_t type_network, type;
  memcpy(&type_network, header + size, sizeof(uint16_t));
  type = scr_ntoh16(type_network);
  size += sizeof(uint16_t);

  /* read in the file version */
  uint16_t version_network, version;
  memcpy(&version_network, header + size, sizeof(uint16_t));
  version = scr_ntoh16(version_network);
  size += sizeof(uint16_t);

  /* read the file size */
  uint64_t filesize_network, filesize;
  memcpy(&filesize_network, header + size, sizeof(uint64_t));
  filesize = scr_ntoh64(filesize_network);
  size += sizeof(uint64_t);

  /* check that the magic number matches */
  /* check that the file type is something we understand */
  /* check that the file version matches */
  if (magic   != SCR_HASH_FILE_MAGIC ||
      type    != SCR_HASH_FILE_TYPE ||
      version != SCR_HASH_FILE_VERSION_1)
  {
    scr_err("File header does not match expected values in %s @ %s:%d",
            file, __FILE__, __LINE__
    );
    scr_hash_delete(tmp_hash);
    return -1;
  }

  /* allocate a buffer to read the hash and crc */
  char* buf = (char*) malloc(filesize);
  if (buf == NULL) {
    scr_err("Allocating %lu byte buffer to write hash to %s @ %s:%d",
            (unsigned long) filesize, file, __FILE__, __LINE__
    );
    scr_hash_delete(tmp_hash);
    return -1;
  }

  /* copy the header into the buffer */
  memcpy(buf, header, size);

  /* read the rest of the file into the buffer */
  nread = scr_read_attempt(file, fd, buf + size, filesize - size);
  if (nread != filesize - size) {
    scr_err("Failed to read file %s @ %s:%d",
            file, __FILE__, __LINE__
    );
    free(buf);
    scr_hash_delete(tmp_hash);
    return -1;
  }

  /* compute the crc value */
  uLong crc = crc32(0L, Z_NULL, 0);
  crc = crc32(crc, (const Bytef*) buf, (uInt) filesize - 4);

  /* read the crc value */
  uint32_t crc_file_network, crc_file;
  memcpy(&crc_file_network, buf + filesize - 4, sizeof(uint32_t));
  crc_file = scr_ntoh32(crc_file_network);

  /* check the crc value */
  if (crc != crc_file) {
    scr_err("CRC mismatch detected in %s @ %s:%d",
            file, __FILE__, __LINE__
    );
    free(buf);
    scr_hash_delete(tmp_hash);
    return -1;
  }

  /* unpack the hash */
  scr_hash_unpack(buf + size, tmp_hash);

  /* merge and delete the temporary hash */
  scr_hash_merge(hash, tmp_hash);
  scr_hash_delete(tmp_hash);

  return filesize;
}

/* opens specified file and reads in a hash filling a pointer with a newly allocated hash */
int scr_hash_read(const char* file, struct scr_hash* hash)
{
  /* check that we have a hash and a file name */
  if (file == NULL || hash == NULL) {
    return SCR_FAILURE;
  }

  /* can't read file, return error (special case so as not to print error message below) */
  if (access(file, R_OK) < 0) {
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

  /* got the file open, be sure to close it even if we hit an error in the read */
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

/* given a filename and hash, lock/open/read/close/unlock the file */
int scr_hash_read_with_lock(const char* file, struct scr_hash* hash)
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
  int fd = scr_open_with_lock(file, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
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

/* given a filename and hash, lock the file, open it, and read it into hash, set fd to the opened file descriptor */
int scr_hash_lock_open_read(const char* file, int* fd, struct scr_hash* hash)
{
  /* check that we got a pointer to a file descriptor */
  if (fd == NULL) {
    scr_err("Must provide a pointer to an int to return opened file descriptor @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* we at least got a pointer to a file descriptor, initialize it to -1 */
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
  *fd = scr_open_with_lock(file, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  if (*fd >= 0) {
    /* read the file into the hash */
    scr_hash_read_fd(file, *fd, hash);
    return SCR_SUCCESS;
  }

  return SCR_FAILURE;
}

/* given a filename, an opened file descriptor, and a hash, overwrite file with hash, close, and unlock file */
int scr_hash_write_close_unlock(const char* file, int* fd, const struct scr_hash* hash)
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
static int scr_hash_print_elem(const struct scr_hash_elem* elem, int indent)
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
int scr_hash_print(const struct scr_hash* hash, int indent)
{
  char tmp[SCR_MAX_FILENAME];
  int i;
  for (i=0; i<indent; i++) {
    tmp[i] = ' ';
  }
  tmp[indent] = '\0';

  if (hash != NULL) {
    struct scr_hash_elem* elem;
    LIST_FOREACH(elem, hash, pointers) {
      scr_hash_print_elem(elem, indent+2);
    }
  } else {
    printf("%sNULL LIST\n", tmp);
  }
  return SCR_SUCCESS;
}

/*
=========================================
Pretty print for TotalView debug window
=========================================
*/

/* This enables a nicer display when diving on a hash variable
 * under the TotalView debugger.  It requires TV 8.8 or later. */

#include "tv_data_display.h"

static int TV_display_type(const struct scr_hash* hash)
{
  if (hash == NULL) {
    /* empty hash, nothing to display here */
    return TV_format_OK;
  }

  struct scr_hash_elem* elem = NULL;
  for (elem = scr_hash_elem_first(hash);
       elem != NULL;
       elem = scr_hash_elem_next(elem))
  {
    /* get the key name */
    char* key = scr_hash_elem_key(elem);

    /* get a pointer to the hash for this key */
    struct scr_hash* h = scr_hash_elem_hash(elem);

    /* add a row to the TV debug window */
    if (h != NULL) {
      /* compute the size of the hash for this key */
      int size = scr_hash_size(h);

      if (size == 0) {
        /* if the hash is empty, stop at the key */
        TV_add_row("value", TV_ascii_string_type, key);
      } else if (size == 1) {
        /* my hash has one value, this may be a key/value pair */
        char* value = scr_hash_elem_get_first_val(hash, key);
        struct scr_hash* h2 = scr_hash_get(h, value);
        if (h2 == NULL || scr_hash_size(h2) == 0) {
          /* my hash has one value, and the hash for that one value is empty,
           * so print this as a key/value pair */
          TV_add_row(key, TV_ascii_string_type, value);
        } else {
          /* recurse into hash */
          TV_add_row(key, "struct scr_hash", h);
        }
      } else {
        /* recurse into hash */
        TV_add_row(key, "struct scr_hash", h);
      }
    } else {
      /* this key has a NULL hash, so stop at the key */
      TV_add_row("value", TV_ascii_string_type, key);
    }
  }

  return TV_format_OK;
}
