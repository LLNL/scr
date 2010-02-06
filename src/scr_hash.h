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

#include <stdarg.h>
#include <sys/types.h>

/* need at least version 8.5 of queue.h from Berkeley */
#include "queue.h"

/*
=========================================
This file defines the data structure for a hash,
which is an unordered list of elements,
where each element contains a key (char string)
and a pointer to another hash.
=========================================
*/

/*
=========================================
Define common hash key strings
========================================
*/

/* generic hash keys */
#define SCR_FLUSH_KEY_CKPT ("CKPT")

#define SCR_FLUSH_KEY_LOCATION ("LOCATION")
#define SCR_FLUSH_KEY_LOCATION_CACHE    ("CACHE")
#define SCR_FLUSH_KEY_LOCATION_PFS      ("PFS")
#define SCR_FLUSH_KEY_LOCATION_FLUSHING ("FLUSHING")

#define SCR_FILEMAP_KEY_PARTNER ("PARTNER")

/* transfer file keys */
#define SCR_TRANSFER_KEY_FILES   ("FILES")
#define SCR_TRANSFER_KEY_BW      ("BW")
#define SCR_TRANSFER_KEY_PERCENT ("PERCENT")

#define SCR_TRANSFER_KEY_COMMAND ("COMMAND")
#define SCR_TRANSFER_KEY_COMMAND_RUN  ("RUN")
#define SCR_TRANSFER_KEY_COMMAND_STOP ("STOP")
#define SCR_TRANSFER_KEY_COMMAND_EXIT ("EXIT")

#define SCR_TRANSFER_KEY_STATE ("STATE")
#define SCR_TRANSFER_KEY_STATE_RUN  ("RUNNING")
#define SCR_TRANSFER_KEY_STATE_STOP ("STOPPED")
#define SCR_TRANSFER_KEY_STATE_EXIT ("EXITING")

#define SCR_TRANSFER_KEY_FLAG ("FLAG")
#define SCR_TRANSFER_KEY_FLAG_DONE ("DONE")

/* ckpt config file keys */
#define SCR_CONFIG_KEY_CACHEDESC  ("CACHEDESC")
#define SCR_CONFIG_KEY_BASE       ("BASE")
#define SCR_CONFIG_KEY_SIZE       ("SIZE")

#define SCR_CONFIG_KEY_CKPTDESC   ("CKPTDESC")
#define SCR_CONFIG_KEY_ENABLED    ("ENABLED")
#define SCR_CONFIG_KEY_INDEX      ("INDEX")
#define SCR_CONFIG_KEY_INTERVAL   ("INTERVAL")
#define SCR_CONFIG_KEY_DIRECTORY  ("DIRECTORY")
#define SCR_CONFIG_KEY_TYPE       ("TYPE")
#define SCR_CONFIG_KEY_HOP_DISTANCE ("HOP_DISTANCE")
#define SCR_CONFIG_KEY_SET_SIZE     ("SET_SIZE")
#define SCR_CONFIG_KEY_GROUPS     ("GROUPS")
#define SCR_CONFIG_KEY_GROUP_ID   ("GROUP_ID")
#define SCR_CONFIG_KEY_GROUP_SIZE ("GROUP_SIZE")
#define SCR_CONFIG_KEY_GROUP_RANK ("GROUP_RANK")

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
Allocate and delete hash objects
=========================================
*/

/* allocates a new hash */
struct scr_hash* scr_hash_new();

/* frees a hash */
int scr_hash_delete(struct scr_hash* hash);

/*
=========================================
size, get, set, unset, and merge functions
=========================================
*/

/* return size of hash (number of keys) */
int scr_hash_size(const struct scr_hash* hash);

/* given a hash and a key, return the hash associated with key, returns NULL if not found */
struct scr_hash* scr_hash_get(const struct scr_hash* hash, const char* key);

/* given a hash, a key, and a hash value, set (or reset) the key's hash */
struct scr_hash* scr_hash_set(struct scr_hash* hash, const char* key, struct scr_hash* hash_value);

/* given a hash and a key, extract and delete any matching element */
int scr_hash_unset(struct scr_hash* hash, const char* key);

/* unset all values in the hash, but don't delete it */
int scr_hash_unset_all(struct scr_hash* hash);

/* merges (copies) elements from hash2 into hash1 */
int scr_hash_merge(struct scr_hash* hash1, const struct scr_hash* hash2);

/* traverse the given hash using a printf-like format string setting an arbitrary list of keys
 * to set (or reset) the hash associated with the last key */
struct scr_hash* scr_hash_setf(struct scr_hash* hash, struct scr_hash* hash_value, const char* format, ...);

/*
=========================================
get, set, and unset hashes using a key/value pair
=========================================
*/

/* shortcut to create a key and subkey in a hash with one call */
struct scr_hash* scr_hash_set_kv(struct scr_hash* hash, const char* key, const char* val);

/* same as scr_hash_set_kv, but with the subkey specified as an int */
struct scr_hash* scr_hash_set_kv_int(struct scr_hash* hash, const char* key, int val);

/* shortcut to get hash assocated with the subkey of a key in a hash with one call */
struct scr_hash* scr_hash_get_kv(const struct scr_hash* hash, const char* key, const char* val);

/* same as scr_hash_get_kv, but with the subkey specified as an int */
struct scr_hash* scr_hash_get_kv_int(const struct scr_hash* hash, const char* key, int val);

/* unset subkey under key, and if that removes the only element for key, unset key as well */
int scr_hash_unset_kv(struct scr_hash* hash, const char* key, const char* val);

/* same as scr_hash_unset_kv, but with the subkey specified as an int */
int scr_hash_unset_kv_int(struct scr_hash* hash, const char* key, int val);

/*
=========================================
Hash element functions
=========================================
*/

/* returns the first element for a given hash */
struct scr_hash_elem* scr_hash_elem_first(const struct scr_hash* hash);

/* given a hash element, returns the next element */
struct scr_hash_elem* scr_hash_elem_next(const struct scr_hash_elem* elem);

/* returns a pointer to the key of the specified element */
char* scr_hash_elem_key(const struct scr_hash_elem* elem);

/* same as scr_hash_elem_key, but converts the key as an int (returns 0 if key is not defined) */
int scr_hash_elem_key_int(const struct scr_hash_elem* elem);

/* returns a pointer to the hash of the specified element */
struct scr_hash* scr_hash_elem_hash(const struct scr_hash_elem* elem);

/* given an element, set its key and hash fields (NOTE: not complement of get as written) */
struct scr_hash_elem* scr_hash_elem_set(struct scr_hash_elem* elem, const char* key, struct scr_hash* hash);

/* given a hash and a key, find first matching element and return its address, returns NULL if not found */
struct scr_hash_elem* scr_hash_elem_get(const struct scr_hash* hash, const char* key);

/* given a hash and a key, return a pointer to the key of the first element of that key's hash */
char* scr_hash_elem_get_first_val(const struct scr_hash* hash, const char* key);

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
size_t scr_hash_get_pack_size(const struct scr_hash* hash);

/* packs the given hash into specified buf and returns the number of bytes written */
size_t scr_hash_pack(char* buf, const struct scr_hash* hash);

/* unpacks hash from specified buffer and returns the number of bytes read and a pointer to a newly allocated hash */
size_t scr_hash_unpack(const char* buf, struct scr_hash* hash);

/*
=========================================
Read and write hash to a file
=========================================
*/

/* executes logic of scr_has_write with opened file descriptor */
ssize_t scr_hash_write_fd(const char* file, int fd, const struct scr_hash* hash);

/* executes logic of scr_hash_read using an opened file descriptor */
ssize_t scr_hash_read_fd(const char* file, int fd, struct scr_hash* hash);

/* write the given hash to specified file */
int scr_hash_write(const char* file, const struct scr_hash* hash);

/* opens specified file and reads in a hash filling a pointer with a newly allocated hash */
int scr_hash_read(const char* file, struct scr_hash* hash);

/* given a filename and hash, lock/open/read/close/unlock the file storing its contents in the hash */
int scr_hash_read_with_lock(const char* file, struct scr_hash* hash);

/* given a filename and hash, lock the file, open it, and read it into hash, set fd to the opened file descriptor */
int scr_hash_lock_open_read(const char* file, int* fd, struct scr_hash* hash);

/* given a filename, an opened file descriptor, and a hash, overwrite file with hash, close, and unlock file */
int scr_hash_write_close_unlock(const char* file, int* fd, const struct scr_hash* hash);

/*
=========================================
Print hash and elements to stdout for debugging
=========================================
*/

/* prints specified hash to stdout for debugging */
int scr_hash_print(const struct scr_hash* hash, int indent);

#endif
