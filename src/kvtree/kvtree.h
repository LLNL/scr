/* Several lines are reproduced from queue.h, under the following license */

/*
 * Copyright (c) 1991, 1993
 *	The Regents of the University of California.  All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 * 3. Neither the name of the University nor the names of its contributors
 *    may be used to endorse or promote products derived from this software
 *    without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED.  IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 *
 *	@(#)queue.h	8.5 (Berkeley) 8/20/94
 */
#ifndef KVTREE_H
#define KVTREE_H

#include <stdarg.h>
#include <sys/types.h>

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
extern "C" {
#endif

/** \defgroup kvtree KVTree
 *  \brief KVTree is the core data structure of all of the components.
 *
 * Each KVTree object contains a list of key/value pairs. Each key is a string,
 * each value is another kvtree object. This is a nested data structures,
 * similar to a python dict or perl hash. The library provides functions to
 * serialize a kvtree object to / from a file. It also optionally provides MPI
 * send / recv functions to transfer an object from one process to another.
 *
 * In addition to getter and setter utilities, this library provides
 * serialization (for persisting an KVTree to a file) and optional MPI
 * functionality.
 */

/** \file kvtree.h
 *  \ingroup kvtree
 *  \brief This file defines the data structure for a hash,
 * which is an unordered list of elements,
 * where each element contains a key (char string)
 * and a pointer to another hash.
 */
#define KVTREE_VERSION "1.5.0"
#define KVTREE_SUCCESS (0)
#define KVTREE_MAX_FILENAME (1024)

#define KVTREE_PRINT_TREE   (1)
#define KVTREE_PRINT_KEYVAL (2)

/********************************************************/
/** \name Sort directions for sorting keys in hash */
///@{
#define KVTREE_SORT_ASCENDING  (0)
#define KVTREE_SORT_DESCENDING (1)
///@}

/********************************************************/
/** \name Define hash and element structures */
///@{

/** \struct define the structure for the head of a hash */
struct kvtree_elem_struct;

// the following 3 lines reproduced from queue.h
struct kvtree_struct{
  struct kvtree_elem_struct *lh_first;
};

/** \struct define the structure for an element of a hash */
struct kvtree_elem_struct {
  char* key;
  struct kvtree_struct* hash;
  // the following 4 lines reproduced from queue.h
  struct{
    struct kvtree_elem_struct *le_next; /* next element */
    struct kvtree_elem_struct **le_prev; /* address of previos next element */
  } pointers;
};

/** \typedef kvtree */
typedef struct kvtree_struct      kvtree;
typedef struct kvtree_elem_struct kvtree_elem;
///@}

/********************************************************/
/** \name Allocate and delete hash objects */
///@{

/** allocates a new hash */
kvtree* kvtree_new(void);

/** frees a hash */
int kvtree_delete(kvtree** ptr_hash);
///@}

/********************************************************/
/** \name size, get, set, unset, and merge functions */
///@{

/** return size of hash (number of keys) */
int kvtree_size(const kvtree* hash);

/** given a hash and a key, return the hash associated with key, returns NULL if not found */
kvtree* kvtree_get(const kvtree* hash, const char* key);

/** given a hash, a key, and a hash value, set (or reset) the key's hash */
kvtree* kvtree_set(kvtree* hash, const char* key, kvtree* hash_value);

/** given a hash and a key, extract and return hash for specified key, returns NULL if not found */
kvtree* kvtree_extract(kvtree* hash, const char* key);

/** given a hash and a key, extract and delete any matching element */
int kvtree_unset(kvtree* hash, const char* key);

/** unset all values in the hash, but don't delete it */
int kvtree_unset_all(kvtree* hash);

/** merges (copies) elements from hash2 into hash1 */
int kvtree_merge(kvtree* hash1, const kvtree* hash2);

/** traverse the given hash using a printf-like format string setting an arbitrary list of keys
 * to set (or reset) the hash associated with the last key */
kvtree* kvtree_setf(kvtree* hash, kvtree* hash_value, const char* format, ...);

/** same as above, but simply returns the hash associated with the list of keys */
kvtree* kvtree_getf(const kvtree* hash, const char* format, ...);

/** sort the hash assuming the keys are strings */
int kvtree_sort(kvtree* hash, int direction);

/** sort the hash assuming the keys are ints */
int kvtree_sort_int(kvtree* hash, int direction);

/** return list of keys in hash as integers, caller must free list */
int kvtree_list_int(const kvtree* hash, int* num, int** list);
///@}

/********************************************************/
/** \name get, set, and unset hashes using a key/value pair */
///@{

/** shortcut to create a key and subkey in a hash with one call */
kvtree* kvtree_set_kv(kvtree* hash, const char* key, const char* val);

/** same as kvtree_set_kv, but with the subkey specified as an int */
kvtree* kvtree_set_kv_int(kvtree* hash, const char* key, int val);

/** shortcut to get hash assocated with the subkey of a key in a hash with one call */
kvtree* kvtree_get_kv(const kvtree* hash, const char* key, const char* val);

/** same as kvtree_get_kv, but with the subkey specified as an int */
kvtree* kvtree_get_kv_int(const kvtree* hash, const char* key, int val);

/** unset subkey under key, and if that removes the only element for key, unset key as well */
int kvtree_unset_kv(kvtree* hash, const char* key, const char* val);

/** same as kvtree_unset_kv, but with the subkey specified as an int */
int kvtree_unset_kv_int(kvtree* hash, const char* key, int val);
///@}

/********************************************************/
/** \name Hash element functions */
///@{

/** returns the first element for a given hash */
kvtree_elem* kvtree_elem_first(const kvtree* hash);

/** given a hash element, returns the next element */
kvtree_elem* kvtree_elem_next(const kvtree_elem* elem);

/** returns a pointer to the key of the specified element */
char* kvtree_elem_key(const kvtree_elem* elem);

/** same as kvtree_elem_key, but converts the key as an int (returns 0 if key is not defined) */
int kvtree_elem_key_int(const kvtree_elem* elem);

/** returns a pointer to the hash of the specified element */
kvtree* kvtree_elem_hash(const kvtree_elem* elem);

/** given a hash and a key, find first matching element and return its address, returns NULL if not found */
kvtree_elem* kvtree_elem_get(const kvtree* hash, const char* key);

/** given a hash and a key, return a pointer to the key of the first element of that key's hash */
char* kvtree_elem_get_first_val(const kvtree* hash, const char* key);

/** given a hash and a key, find first matching element, remove it from the hash, and return it */
kvtree_elem* kvtree_elem_extract(kvtree* hash, const char* key);

/** given a hash and a key, find first matching element, remove it from the hash, and return it */
kvtree_elem* kvtree_elem_extract_int(kvtree* hash, int key);

/** extract element from hash given the hash and the address of the element */
kvtree_elem* kvtree_elem_extract_by_addr(kvtree* hash, kvtree_elem* elem);

char* kvtree_get_val(const kvtree* hash, const char* key);
///@}

/********************************************************/
/** \name Pack and unpack hash and elements into a char buffer */
///@{

/** computes the number of bytes needed to pack the given hash */
size_t kvtree_pack_size(const kvtree* hash);

/** packs the given hash into specified buf and returns the number of bytes written */
size_t kvtree_pack(char* buf, const kvtree* hash);

/** unpacks hash from specified buffer into given hash object and returns the number of bytes read */
size_t kvtree_unpack(const char* buf, kvtree* hash);
///@}

/********************************************************/
/** \name Read and write hash to a file */
///@{

/** persist hash in newly allocated buffer,
 * return buffer address and size to be freed by caller */
int kvtree_write_persist(void** ptr_buf, size_t* ptr_size, const kvtree* hash);

/** executes logic of kvtree_has_write with opened file descriptor */
ssize_t kvtree_write_fd(const char* file, int fd, const kvtree* hash);

/** executes logic of kvtree_read using an opened file descriptor */
ssize_t kvtree_read_fd(const char* file, int fd, kvtree* hash);

/** write the given hash to specified file */
int kvtree_write_file(const char* file, const kvtree* hash);

/** opens specified file and reads in a hash storing its contents in the given hash object */
int kvtree_read_file(const char* file, kvtree* hash);

/** given a filename and hash, lock/open/read/close/unlock the file storing its contents in the hash */
int kvtree_read_with_lock(const char* file, kvtree* hash);

/** given a filename and hash, lock/open/read/close/unlock the file storing its contents in the hash */
int kvtree_write_with_lock(const char* file, kvtree* hash);

/** given a filename and hash, lock the file, open it, and read it into hash, set fd to the opened file descriptor.
 * Note that this function actually acquires a write lock, not a read lock, allowing you do a read-modify-write
 * before you unlock it.  */
int kvtree_lock_open_read(const char* file, int* fd, kvtree* hash);

/** given a filename, an opened file descriptor, and a hash, overwrite file with hash, close, and unlock file */
int kvtree_write_close_unlock(const char* file, int* fd, const kvtree* hash);

/** write kvtree as gather/scatter file, input kvtree must be in form:
 *
 *      0
 *        <kvtree_for_rank_0>
 *      1
 *        <kvtree_for_rank_1>
 *
 * requires exactly one entry for each rank starting at 0 couting up to ranks-1
 * items in kvtree do not need to be sorted before the call */
int kvtree_write_to_gather(const char* prefix, kvtree* data, int ranks);
///@}

/* Read a scatter/gather file and all of its subfiles into a single kvtree */
int kvtree_read_scatter_single(const char* prefix, kvtree* data);

/********************************************************/
/** \name Print hash and elements to stdout for debugging */
///@{

/** prints specified hash to stdout for debugging */
int kvtree_print(const kvtree* hash, int indent);

/** prints specified hash to stdout for debugging */
int kvtree_print_mode(const kvtree* hash, int indent, int mode);

/** logs specified hash for debugging */
int kvtree_log(const kvtree* hash, int log_level, int indent);
///@}

/** enable C++ codes to include this header directly */
#ifdef __cplusplus
} /* extern "C" */
#endif

#endif
