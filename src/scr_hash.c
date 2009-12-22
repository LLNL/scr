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

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <string.h>
#include <errno.h>

/* need at least version 8.5 of queue.h from Berkeley */
/*#include <sys/queue.h>*/
#include "queue.h"

/*
=========================================
Allocate and delete hash and element objects
=========================================
*/

/* allocates a new hash object */
struct scr_hash* scr_hash_new()
{
  struct scr_hash* hash = (struct scr_hash*) malloc(sizeof(struct scr_hash));
  if (hash != NULL) { LIST_INIT(hash); }
  return hash;
}

/* deletes all items in a hash, one by one, and then the hash itself */
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

/* allocates a new hash element and sets its key and hash fields */
struct scr_hash_elem* scr_hash_elem_new()
{
  struct scr_hash_elem* elem = (struct scr_hash_elem*) malloc(sizeof(struct scr_hash_elem));
  if (elem != NULL) {
    elem->key  = NULL;
    elem->hash = NULL;
  }
  return elem;
}

/* deletes a hash element */
int scr_hash_elem_delete(struct scr_hash_elem* elem)
{
  if (elem != NULL) {
    /* free the key and values strings which were strdup'ed */
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

/*
=========================================
size, set, get, unset functions
=========================================
*/

/* return size of hash (number of keys) */
int scr_hash_size(struct scr_hash* hash)
{
  int count = 0;
  if (hash != NULL) {
    struct scr_hash_elem* elem;
    LIST_FOREACH(elem, hash, pointers) { count++; }
  }
  return count;
}

/* given a hash and a key, return elem in hash which matches, returns NULL if not found */
struct scr_hash* scr_hash_get(struct scr_hash* hash, const char* key)
{
  struct scr_hash_elem* elem = scr_hash_elem_get(hash, key);
  if (elem != NULL) { return elem->hash; }
  return NULL;
}

/* given a hash, a key, and a value, set (or reset) key with value */
struct scr_hash* scr_hash_set(struct scr_hash* hash, const char* key, struct scr_hash* hash_value)
{
  if (hash == NULL) { return NULL; }

  /* if there is a match in the hash, pull out that element */
  struct scr_hash_elem* elem = scr_hash_elem_extract(hash, key);
  if (elem == NULL) {
    /* nothing found, so create a new element and set it */
    elem = scr_hash_elem_new();
    scr_hash_elem_set(elem, key, hash_value);
  } else {
    /* found something with this key already, delete its current hash and reset it */
    if (elem->hash != NULL) { scr_hash_delete(elem->hash); }
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
  if (hash == NULL) { return SCR_SUCCESS; }

  struct scr_hash_elem* elem = scr_hash_elem_extract(hash, key);
  if (elem != NULL) { scr_hash_elem_delete(elem); }
  return SCR_SUCCESS;
}

/*
=========================================
get, set, and unset functions using integer values for the key
=========================================
*/

/* given a hash and a key, return elem in hash which matches, returns NULL if not found */
struct scr_hash* scr_hash_get_int(struct scr_hash* hash, int key)
{
  /* TODO: this feels kludgy, but I guess as long as the ASCII string is longer
   * than a max int (or minimum int with leading minus sign) which is 11 chars, we're ok
   * ("-2147483648" to "2147483647") */
  char tmp[100];
  sprintf(tmp, "%d", key);
  return scr_hash_get(hash, tmp);
}

/* given a hash, a key, and a value, set (or reset) key with value */
struct scr_hash* scr_hash_set_int(struct scr_hash* hash, int key, struct scr_hash* hash_value)
{
  /* TODO: this feels kludgy, but I guess as long as the ASCII string is longer
   * than a max int (or minimum int with leading minus sign) which is 11 chars, we're ok
   * ("-2147483648" to "2147483647") */
  char tmp[100];
  sprintf(tmp, "%d", key);
  return scr_hash_set(hash, tmp, hash_value);
}

/* given a hash and a key, extract and delete any matching element */
int scr_hash_unset_int(struct scr_hash* hash, int key)
{
  /* TODO: this feels kludgy, but I guess as long as the ASCII string is longer
   * than a max int (or minimum int with leading minus sign) which is 11 chars, we're ok
   * ("-2147483648" to "2147483647") */
  char tmp[100];
  sprintf(tmp, "%d", key);
  return scr_hash_unset(hash, tmp);
}

/*
=========================================
set key/value pairs
=========================================
*/

/* shortcut to set key/value pair */
struct scr_hash* scr_hash_set_kv(struct scr_hash* hash, const char* key, const char* val)
{
  if (hash == NULL) { return NULL; }

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

/* shortcut to set key/value pair using an int for the value */
struct scr_hash* scr_hash_set_kv_int(struct scr_hash* hash, const char* key, int val)
{
  /* TODO: this feels kludgy, but I guess as long as the ASCII string is longer
   * than a max int (or minimum int with leading minus sign) which is 11 chars, we're ok
   * ("-2147483648" to "2147483647") */
  char tmp[100];
  sprintf(tmp, "%d", val);
  return scr_hash_set_kv(hash, key, tmp);
}

/* shortcut to get hash assocated with key/value pair */
struct scr_hash* scr_hash_get_kv(struct scr_hash* hash, const char* key, const char* val)
{
  if (hash == NULL) { return NULL; }

  struct scr_hash* k = scr_hash_get(hash, key);
  if (k == NULL) { return NULL; }
 
  struct scr_hash* v = scr_hash_get(k, val);
  if (v == NULL) { return NULL; }
  
  return v; 
}

/* shortcut to get hash assocated with key/value pair using as int for the value */
struct scr_hash* scr_hash_get_kv_int(struct scr_hash* hash, const char* key, int val)
{
  /* TODO: this feels kludgy, but I guess as long as the ASCII string is longer
   * than a max int (or minimum int with leading minus sign) which is 11 chars, we're ok
   * ("-2147483648" to "2147483647") */
  char tmp[100];
  sprintf(tmp, "%d", val);
  return scr_hash_get_kv(hash, key, tmp);
}

/* unset value under key, and if that empties the list for key, unset key as well */
int scr_hash_unset_kv(struct scr_hash* hash, const char* key, const char* val)
{
  if (hash == NULL) { return SCR_SUCCESS; }

  struct scr_hash* v = scr_hash_get(hash, key);
  int rc = scr_hash_unset(v, val);
  if (scr_hash_size(v) == 0) {
    rc = scr_hash_unset(hash, key);
  }

  return rc;
}

/* unset value under key using an integer, and if that empties the list for key, unset key as well */
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
struct scr_hash_elem* scr_hash_elem_first(struct scr_hash* hash)
{
  if (hash == NULL) { return NULL; }
  return LIST_FIRST(hash);
}

/* given a hash element, returns the next element */
struct scr_hash_elem* scr_hash_elem_next(struct scr_hash_elem* elem)
{
  if (elem == NULL) { return NULL; }
  return LIST_NEXT(elem, pointers);
}

/* returns a pointer to the key string */
char* scr_hash_elem_key(struct scr_hash_elem* elem)
{
  return elem->key;
}

/* returns a pointer to the key string */
int scr_hash_elem_key_int(struct scr_hash_elem* elem)
{
  return atoi(elem->key);
}

/* returns a pointer to the hash */
struct scr_hash* scr_hash_elem_hash(struct scr_hash_elem* elem)
{
  return elem->hash;
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

/* given a hash and a key, find first matching element and return its address */
struct scr_hash_elem* scr_hash_elem_get(struct scr_hash* hash, const char* key)
{
  if (hash == NULL) { return NULL; }

  struct scr_hash_elem* elem;
  LIST_FOREACH(elem, hash, pointers) {
    if (elem->key != NULL && strcmp(elem->key, key) == 0) { return elem; }
  }
  return NULL;
}

/* given a hash and a key, find first matching element, remove it from the hash, and return it */
struct scr_hash_elem* scr_hash_elem_extract(struct scr_hash* hash, const char* key)
{
  struct scr_hash_elem* elem = scr_hash_elem_get(hash, key);
  if (elem != NULL) { LIST_REMOVE(elem, pointers); }
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

/* computes the number of bytes needed to pack the given hash */
size_t scr_hash_get_pack_size(struct scr_hash* hash)
{
  size_t size = 0;
  if (hash != NULL) {
    struct scr_hash_elem* elem;

    /* first, count the items in the hash */
    int count = 0;
    LIST_FOREACH(elem, hash, pointers) { count++; }

    /* now record the size of the COUNT string that would be written during the pack */
    char tmp[100];
    sprintf(tmp, "%d", count);
    size += strlen(tmp) + 1;

    /* finally add in the size of each element */
    LIST_FOREACH(elem, hash, pointers) {
      size += scr_hash_get_pack_size_elem(elem);
    }  
  } else {
    size += strlen("0") + 1;
  }
  return size;
}

/* computes the number of bytes needed to pack the given hash element */
size_t scr_hash_get_pack_size_elem(struct scr_hash_elem* elem)
{
  size_t size = 0;
  if (elem != NULL) {
    if (elem->key   != NULL) {
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

/* packs the given hash into specified buf and returns the number of bytes written */
size_t scr_hash_pack(struct scr_hash* hash, char* buf)
{
  size_t size = 0;
  if (hash != NULL) {
    struct scr_hash_elem* elem;

    /* first count the items in the hash */
    int count = 0;
    LIST_FOREACH(elem, hash, pointers) { count++; }

    /* pack the count value */
    char tmp[100];
    sprintf(tmp, "%d", count);
    strcpy(buf + size, tmp);
    size += strlen(tmp) + 1;

    /* and finally pack each element */
    LIST_FOREACH(elem, hash, pointers) {
      size += scr_hash_pack_elem(elem, buf + size);
    }
  } else {
    /* now pack the count value of 0 */
    char tmp[] = "0";
    strcpy(buf + size, tmp);
    size += strlen(tmp) + 1;
  }
  return size;
}

/* packs a hash element into specified buf and returns the number of bytes written */
size_t scr_hash_pack_elem(struct scr_hash_elem* elem, char* buf)
{
  size_t size = 0;
  if (elem != NULL) {
    if (elem->key   != NULL) {
      strcpy(buf + size, elem->key);
      size += strlen(elem->key) + 1;
    } else {
      buf[size] = '\0';
      size += 1;
    }
    size += scr_hash_pack(elem->hash, buf + size);
  } else {
    buf[size] = '\0';
    size += 1;
    size += scr_hash_pack(NULL, buf + size);
  }
  return size;
}

/* unpacks hash from specified buffer and returns the number of bytes read and a pointer to a newly allocated hash */
size_t scr_hash_unpack(char* buf, struct scr_hash* hash)
{
  /* check that we got a hash object to unpack data into */
  if (hash == NULL) { return 0; }

  /* allocate a new hash object and initialize it */
  size_t size = 0;

  /* read in the COUNT value */
  char count_str[SCR_MAX_FILENAME];
  strcpy(count_str, buf + size);
  size += strlen(count_str) + 1;
  int count = atoi(count_str);

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

/* unpacks hash element from specified buffer and returns the number of bytes read and a pointer to a newly allocated hash */
size_t scr_hash_unpack_elem(char* buf, struct scr_hash_elem* elem)
{
  /* check that we got an elem object to unpack data into */
  if (elem == NULL) { return 0; }

  /* read in the key and value strings */
  size_t size = 0;
  char key[SCR_MAX_FILENAME];

  /* read in the KEY string */
  strcpy(key, buf + size);
  size += strlen(key) + 1;

  /* read in the hash object */
  struct scr_hash* hash = scr_hash_new();
  size += scr_hash_unpack(buf + size, hash);
  
  /* set our elem with the key and hash values we unpacked */
  scr_hash_elem_set(elem, key, hash);

  return size;
}

/*
=========================================
Read and write hash to a file
=========================================
*/

/* write the given hash to specified file */
int scr_hash_write(const char* file, struct scr_hash* hash)
{
  /* check that we have a hash and a file name */
  if (file == NULL || hash == NULL) { return SCR_FAILURE; }

  /* open the hash file */
  FILE* fs = fopen(file, "w");
  if (fs == NULL) {
    /*
    scr_err("Opening hash file for read: fopen(%s, \"r\") errno=%d %m @ %s:%d",
              file, errno, __FILE__, __LINE__
    );
    */
    return SCR_FAILURE;
  }

  /* write the hash */
  scr_hash_write_hash(fs, hash);

  /* mark the file as complete */
  fprintf(fs, "End\n");

  /* close the hash file */
  fclose(fs);

  return SCR_SUCCESS;
}

/* write the given hash to the given open file stream */
int scr_hash_write_hash(FILE* fs, struct scr_hash* hash)
{
  if (hash != NULL) {
    struct scr_hash_elem* elem;

    /* first count the items in the hash */
    int count = 0;
    LIST_FOREACH(elem, hash, pointers) { count++; }

    /* write the count value */
    fprintf(fs, "C:%d\n", count);

    /* and finally pack each element */
    LIST_FOREACH(elem, hash, pointers) {
      scr_hash_write_elem(fs, elem);
    }
  } else {
    /* for an empty hash, write a count of 0 */
    fprintf(fs, "C:0\n");
  }

  return SCR_SUCCESS;
}

/* write the given hash element to the given open file stream */
int scr_hash_write_elem(FILE* fs, struct scr_hash_elem* elem)
{
  if (elem != NULL) {
    if (elem->key != NULL) {
      fprintf(fs, "%s\n", elem->key);
    } else {
      fprintf(fs, "\n");
    }
    scr_hash_write_hash(fs, elem->hash);
  } else {
    fprintf(fs, "\n");
    scr_hash_write_hash(fs, NULL);
  }
  return SCR_SUCCESS;
}

/* opens specified file and reads in a hash filling a pointer with a newly allocated hash */
int scr_hash_read(const char* file, struct scr_hash* hash)
{
  /* check that we have a hash and a file name */
  if (file == NULL || hash == NULL) { return SCR_FAILURE; }

  /* open the hash file */
  FILE* fs = fopen(file, "r");
  if (fs == NULL) {
    /*
    scr_err("Opening hash file for read: fopen(%s, \"r\") errno=%d %m @ %s:%d",
              file, errno, __FILE__, __LINE__
    );
    */
    return SCR_FAILURE;
  }

  /* read the hash */
  int rc = scr_hash_read_hash(fs, hash);

  /* check for the End tag */
  char buf[1024] = "";
  char* p = fgets(buf, sizeof(buf), fs);
  if (p == NULL || strcmp(buf, "End\n") != 0) {
    rc = SCR_FAILURE;
  }

  /* close the hash file */
  fclose(fs);

  return rc;
}

/* reads a hash from the given open file stream, sets provided pointer to a newly allocated hash */
int scr_hash_read_hash(FILE* fs, struct scr_hash* hash)
{
  int rc = SCR_SUCCESS;

  /* check that we got a hash to read data into */
  if (hash == NULL) { return SCR_FAILURE; }

  /* read in the COUNT value */
  int count = 0;
  int p = fscanf(fs, "C:%d\n", &count);
  if (p == EOF) {
    rc = SCR_FAILURE;
  }

  /* for each element, read in its hash */
  int i;
  for (i = 0; i < count; i++) {
    struct scr_hash_elem* elem = scr_hash_elem_new();
    int tmp_rc = scr_hash_read_elem(fs, elem);
    if (tmp_rc != SCR_SUCCESS) {
      rc = tmp_rc;
      scr_hash_elem_delete(elem);
      break;
    }
    LIST_INSERT_HEAD(hash, elem, pointers);
  }

  /* return the success code */
  return rc;
}

/* reads an element from the given open file stream, sets provided pointer to a newly allocated element */
size_t scr_hash_read_elem(FILE* fs, struct scr_hash_elem* elem)
{
  int rc = SCR_SUCCESS;

  /* check that we got an elem to read data into */
  if (elem == NULL) { return SCR_FAILURE; }

  /* read in the key and value strings */
  char key[SCR_MAX_FILENAME];

  /* read in the KEY string */
  char* p = fgets(key, sizeof(key), fs);
  if (p == NULL) {
    rc = SCR_FAILURE;
  }
  key[strlen(key)-1] = '\0';

  /* read in the hash object */
  struct scr_hash* hash = scr_hash_new();
  int tmp_rc = scr_hash_read_hash(fs, hash);
  if (tmp_rc != SCR_SUCCESS) {
    rc = tmp_rc;
  }
  
  /* set our element with key and hash values we read in */
  scr_hash_elem_set(elem, key, hash);

  return rc;
}

/*
=========================================
Print hash and elements to stdout for debugging
=========================================
*/

/* prints specified hash to stdout for debugging */
int scr_hash_print(struct scr_hash* hash, int indent)
{
  char tmp[SCR_MAX_FILENAME];
  int i;
  for (i=0; i<indent; i++) { tmp[i] = ' '; }
  tmp[indent] = '\0';

  if (hash != NULL) {
    printf("%sLIST: (\n", tmp);
    struct scr_hash_elem* elem;
    LIST_FOREACH(elem, hash, pointers) {
      scr_hash_print_elem(elem, indent+2);
    }
    printf("%s)\n", tmp);
  } else {
    printf("%sNULL LIST\n", tmp);
  }
  return SCR_SUCCESS;
}

/* prints specified hash element to stdout for debugging */
int scr_hash_print_elem(struct scr_hash_elem* elem, int indent)
{
  char tmp[SCR_MAX_FILENAME];
  int i;
  for (i=0; i<indent; i++) { tmp[i] = ' '; }
  tmp[indent] = '\0';

  if (elem != NULL) {
    if (elem->key != NULL) {
      printf("%sKEY: %s\n", tmp, elem->key);
    } else {
      printf("%sNULL KEY\n", tmp);
    }
    scr_hash_print(elem->hash, indent);
  } else {
    printf("%sNULL ELEMENT\n", tmp);
  }
  return SCR_SUCCESS;
}
