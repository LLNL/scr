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

#ifndef SCR_HASH_H
#define SCR_HASH_H

#include <stdio.h>
#include <sys/types.h>

/* need at least version 8.5 of queue.h from Berkeley */
/*#include <sys/queue.h>*/
#include "queue.h"

/*
#define SCR_KVL_FOREACH(var, head)  LIST_FOREACH((var), (head), pointers)
*/

/*
=========================================
This file defines the data structure for a hash,
where each element contains a value field (stored as string)
and a pointer to another hash.
=========================================
*/

/*
=========================================
Define hash and element structures
=========================================
*/

/* define the structure for the head of a hash */
struct scr_hash_elem;
LIST_HEAD(scr_hash, scr_hash_elem);

/* define the structure for an element of a hash */
struct scr_hash_elem {
  char* key;
  struct scr_hash* hash;
  LIST_ENTRY(scr_hash_elem) pointers;
};

/*
=========================================
Allocate and delete hash and element objects
=========================================
*/

/* allocates a new hash object */
struct scr_hash* scr_hash_new();

/* deletes all items in a hash, one by one */
int scr_hash_delete(struct scr_hash* hash);

/* allocates a new element */
struct scr_hash_elem* scr_hash_elem_new();

/* deletes a hash element */
int scr_hash_elem_delete(struct scr_hash_elem* elem);

/*
=========================================
size, set, get, unset functions
=========================================
*/

/* return size of hash (number of keys) */
int scr_hash_size(struct scr_hash* hash);

/* given a hash and a key, return first elem in hash which matches, returns NULL if not found */
struct scr_hash* scr_hash_get(struct scr_hash* hash, const char* key);

/* given a hash, a key, and a value, set (or reset) key with value */
struct scr_hash* scr_hash_set(struct scr_hash* hash, const char* key, struct scr_hash* hash_value);

/* given a hash and a key, extract and delete any matching element */
int scr_hash_unset(struct scr_hash* hash, const char* key);

/*
=========================================
get, set, and unset functions using integer values for the key
=========================================
*/

/* given a hash and a key, return elem in hash which matches, returns NULL if not found */
struct scr_hash* scr_hash_get_int(struct scr_hash* hash, int key);

/* given a hash, a key, and a value, set (or reset) key with value */
struct scr_hash* scr_hash_set_int(struct scr_hash* hash, int key, struct scr_hash* hash_value);

/* given a hash and a key, extract and delete any matching element */
int scr_hash_unset_int(struct scr_hash* hash, int key);

/*
=========================================
set key/value pairs
=========================================
*/

/* shortcut to set key/value pair */
struct scr_hash* scr_hash_set_kv(struct scr_hash* hash, const char* key, const char* val);

/* shortcut to set key/value pair using an int for the value */
struct scr_hash* scr_hash_set_kv_int(struct scr_hash* hash, const char* key, int val);

/* shortcut to get hash assocated with key/value pair */
struct scr_hash* scr_hash_get_kv(struct scr_hash* hash, const char* key, const char* val);

/* shortcut to get hash assocated with key/value pair using as int for the value */
struct scr_hash* scr_hash_get_kv_int(struct scr_hash* hash, const char* key, int val);

/* unset value under key, and if that empties the list for key, unset key as well */
int scr_hash_unset_kv(struct scr_hash* hash, const char* key, const char* val);

/* unset value under key using an integer, and if that empties the list for key, unset key as well */
int scr_hash_unset_kv_int(struct scr_hash* hash, const char* key, int val);

/*
=========================================
Hash element functions
=========================================
*/

/* returns the first element for a given hash */
struct scr_hash_elem* scr_hash_elem_first(struct scr_hash* hash);

/* given a hash element, returns the next element */
struct scr_hash_elem* scr_hash_elem_next(struct scr_hash_elem* elem);

/* returns a pointer to the key string */
char* scr_hash_elem_key(struct scr_hash_elem* elem);

/* returns the key as an int */
int scr_hash_elem_key_int(struct scr_hash_elem* elem);

/* returns a pointer to the hash */
struct scr_hash* scr_hash_elem_hash(struct scr_hash_elem* elem);

/* given an element, set its key and hash fields (NOTE: not complement of get as written) */
struct scr_hash_elem* scr_hash_elem_set(struct scr_hash_elem* elem, const char* key, struct scr_hash* hash);

/* given a hash and a key, find first matching element and return its address */
struct scr_hash_elem* scr_hash_elem_get(struct scr_hash* hash, const char* key);

/* given a hash and a key, find first matching element, remove it from the hash, and return it */
struct scr_hash_elem* scr_hash_elem_extract(struct scr_hash* hash, const char* key);

/* extract element from hash given the hash and the address of the element */
struct scr_hash_elem* scr_hash_elem_extract_by_addr(struct scr_hash* hash, struct scr_hash_elem* elem);

/*
=========================================
Pack and unpack hash and elements into a char buffer
=========================================
*/

/* computes the number of bytes needed to pack the given hash */
size_t scr_hash_get_pack_size(struct scr_hash* hash);

/* computes the number of bytes needed to pack the given hash element */
size_t scr_hash_get_pack_size_elem(struct scr_hash_elem* elem);

/* packs the given hash into specified buf and returns the number of bytes written */
size_t scr_hash_pack(struct scr_hash* hash, char* buf);

/* packs a element into specified buf and returns the number of bytes written */
size_t scr_hash_pack_elem(struct scr_hash_elem* elem, char* buf);

/* unpacks hash from specified buffer and returns the number of bytes read and a pointer to a newly allocated hash */
size_t scr_hash_unpack(char* buf, struct scr_hash* hash);

/* unpacks hash element from specified buffer and returns the number of bytes read and a pointer to a newly allocated hash */
size_t scr_hash_unpack_elem(char* buf, struct scr_hash_elem* elem);

/*
=========================================
Read and write hash to a file
=========================================
*/

/* write the given hash to specified file */
int scr_hash_write(const char* file, struct scr_hash* hash);

/* write the given hash to the given open file stream */
int scr_hash_write_hash(FILE* fs, struct scr_hash* hash);

/* write the given hash element to the given open file stream */
int scr_hash_write_elem(FILE* fs, struct scr_hash_elem* elem);

/* opens specified file and reads in a hash filling a pointer with a newly allocated hash */
int scr_hash_read(const char* file, struct scr_hash* hash);

/* reads a hash from the given open file stream, sets provided pointer to a newly allocated hash */
int scr_hash_read_hash(FILE* fs, struct scr_hash* hash);

/* reads an element from the given open file stream, sets provided pointer to a newly allocated element */
size_t scr_hash_read_elem(FILE* fs, struct scr_hash_elem* elem);

/*
=========================================
Print hash and elements to stdout for debugging
=========================================
*/

/* prints specified hash to stdout for debugging */
int scr_hash_print(struct scr_hash* hash, int indent);

/* prints specified hash element to stdout for debugging */
int scr_hash_print_elem(struct scr_hash_elem* elem, int indent);

#endif
