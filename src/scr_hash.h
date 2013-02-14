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

#include "scr_path.h"

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
Sort directions for sorting keys in hash
========================================
*/

#define SCR_HASH_SORT_ASCENDING  (0)
#define SCR_HASH_SORT_DESCENDING (1)

/*
=========================================
Define common hash key strings
========================================
*/

/* generic hash keys */
#define SCR_KEY_DATASET   ("DSET")
#define SCR_KEY_PATH      ("PATH")
#define SCR_KEY_SEGMENT   ("SEG")
#define SCR_KEY_CONTAINER ("CTR")
#define SCR_KEY_ID        ("ID")
#define SCR_KEY_NAME      ("NAME")
#define SCR_KEY_SIZE      ("SIZE")
#define SCR_KEY_OFFSET    ("OFFSET")
#define SCR_KEY_LENGTH    ("LENGTH")
#define SCR_KEY_RANK      ("RANK")
#define SCR_KEY_RANKS     ("RANKS")
#define SCR_KEY_DIRECTORY ("DIR")
#define SCR_KEY_FILE      ("FILE")
#define SCR_KEY_FILES     ("FILES")
#define SCR_KEY_META      ("META")
#define SCR_KEY_COMPLETE  ("COMPLETE")
#define SCR_KEY_CRC       ("CRC")

/* these keys are kept in hashes stored in files for long periods of time,
 * thus we associate a version number with them in order to read old files */
#define SCR_SUMMARY_KEY_VERSION ("VERSION")

#define SCR_SUMMARY_FILE_VERSION_5 (5)
#define SCR_SUMMARY_5_KEY_CKPT      ("CKPT")
#define SCR_SUMMARY_5_KEY_RANK      ("RANK")
#define SCR_SUMMARY_5_KEY_RANKS     ("RANKS")
#define SCR_SUMMARY_5_KEY_COMPLETE  ("COMPLETE")
#define SCR_SUMMARY_5_KEY_FILE      ("FILE")
#define SCR_SUMMARY_5_KEY_FILES     ("FILES")
#define SCR_SUMMARY_5_KEY_SIZE      ("SIZE")
#define SCR_SUMMARY_5_KEY_CRC       ("CRC")
#define SCR_SUMMARY_5_KEY_NOFETCH   ("NOFETCH")

#define SCR_SUMMARY_FILE_VERSION_6 (6)
#define SCR_SUMMARY_6_KEY_DATASET   ("DSET")
#define SCR_SUMMARY_6_KEY_RANK2FILE ("RANK2FILE")
#define SCR_SUMMARY_6_KEY_LEVEL     ("LEVEL")
#define SCR_SUMMARY_6_KEY_RANK      ("RANK")
#define SCR_SUMMARY_6_KEY_RANKS     ("RANKS")
#define SCR_SUMMARY_6_KEY_COMPLETE  ("COMPLETE")
#define SCR_SUMMARY_6_KEY_FILE      ("FILE")
#define SCR_SUMMARY_6_KEY_FILES     ("FILES")
#define SCR_SUMMARY_6_KEY_SIZE      ("SIZE")
#define SCR_SUMMARY_6_KEY_CRC       ("CRC")
#define SCR_SUMMARY_6_KEY_NOFETCH   ("NOFETCH")
#define SCR_SUMMARY_6_KEY_CONTAINER ("CTR")
#define SCR_SUMMARY_6_KEY_SEGMENT   ("SEG")
#define SCR_SUMMARY_6_KEY_ID        ("ID")
#define SCR_SUMMARY_6_KEY_LENGTH    ("LENGTH")
#define SCR_SUMMARY_6_KEY_OFFSET    ("OFFSET")

#define SCR_INDEX_KEY_VERSION ("VERSION")

#define SCR_INDEX_FILE_VERSION_1 (1)
#define SCR_INDEX_1_KEY_DIR       ("DIR")
#define SCR_INDEX_1_KEY_CKPT      ("CKPT")
#define SCR_INDEX_1_KEY_DATASET   ("DSET")
#define SCR_INDEX_1_KEY_COMPLETE  ("COMPLETE")
#define SCR_INDEX_1_KEY_FETCHED   ("FETCHED")
#define SCR_INDEX_1_KEY_FLUSHED   ("FLUSHED")
#define SCR_INDEX_1_KEY_FAILED    ("FAILED")
#define SCR_INDEX_1_KEY_CURRENT   ("CURRENT")

/* the rest of these hash keys are only used in memory or in files
 * that live for the life of the job, thus backwards compatibility is not needed */
#define SCR_FLUSH_KEY_DATASET  ("DATASET")
#define SCR_FLUSH_KEY_LOCATION ("LOCATION")
#define SCR_FLUSH_KEY_LOCATION_CACHE    ("CACHE")
#define SCR_FLUSH_KEY_LOCATION_PFS      ("PFS")
#define SCR_FLUSH_KEY_LOCATION_FLUSHING ("FLUSHING")
#define SCR_FLUSH_KEY_LOCATION_SYNC_FLUSHING ("SYNC_FLUSHING")
#define SCR_FLUSH_KEY_DIRECTORY ("DIR")

#define SCR_SCAVENGE_KEY_PRESERVE  ("PRESERVE")
#define SCR_SCAVENGE_KEY_CONTAINER ("CONTAINER")
#define SCR_SCAVENGE_KEY_PARTNER   ("PARTNER")

#define SCR_NODES_KEY_NODES ("NODES")

/* transfer file keys */
#define SCR_TRANSFER_KEY_FILES       ("FILES")
#define SCR_TRANSFER_KEY_DESTINATION ("DESTINATION")
#define SCR_TRANSFER_KEY_SIZE        ("SIZE")
#define SCR_TRANSFER_KEY_WRITTEN     ("WRITTEN")
#define SCR_TRANSFER_KEY_BW          ("BW")
#define SCR_TRANSFER_KEY_PERCENT     ("PERCENT")

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
#define SCR_CONFIG_KEY_GROUPDESC  ("GROUPDESC")
#define SCR_CONFIG_KEY_STOREDESC  ("STOREDESC")
#define SCR_CONFIG_KEY_CACHEDESC  ("CACHEDESC")
#define SCR_CONFIG_KEY_COUNT      ("COUNT")
#define SCR_CONFIG_KEY_NAME       ("NAME")
#define SCR_CONFIG_KEY_BASE       ("BASE")
#define SCR_CONFIG_KEY_SIZE       ("SIZE")
#define SCR_CONFIG_KEY_GROUP      ("GROUP")

#define SCR_CONFIG_KEY_CKPTDESC   ("CKPTDESC")
#define SCR_CONFIG_KEY_ENABLED    ("ENABLED")
#define SCR_CONFIG_KEY_INDEX      ("INDEX")
#define SCR_CONFIG_KEY_INTERVAL   ("INTERVAL")
#define SCR_CONFIG_KEY_DIRECTORY  ("DIRECTORY")
#define SCR_CONFIG_KEY_TYPE       ("TYPE")
#define SCR_CONFIG_KEY_FAIL_GROUP ("FAIL_GROUP")
#define SCR_CONFIG_KEY_SET_SIZE   ("SET_SIZE")
#define SCR_CONFIG_KEY_GROUPS     ("GROUPS")
#define SCR_CONFIG_KEY_GROUP_ID   ("GROUP_ID")
#define SCR_CONFIG_KEY_GROUP_SIZE ("GROUP_SIZE")
#define SCR_CONFIG_KEY_GROUP_RANK ("GROUP_RANK")
#define SCR_CONFIG_KEY_MKDIR      ("MKDIR")

#define SCR_DATASET_KEY_ID       ("ID")
#define SCR_DATASET_KEY_USER     ("USER")
#define SCR_DATASET_KEY_JOBNAME  ("JOBNAME")
#define SCR_DATASET_KEY_NAME     ("NAME")
#define SCR_DATASET_KEY_SIZE     ("SIZE")
#define SCR_DATASET_KEY_FILES    ("FILES")
#define SCR_DATASET_KEY_CREATED  ("CREATED")
#define SCR_DATASET_KEY_JOBID    ("JOBID")
#define SCR_DATASET_KEY_CLUSTER  ("CLUSTER")
#define SCR_DATASET_KEY_CKPT     ("CKPT")
#define SCR_DATASET_KEY_COMPLETE ("COMPLETE")

#define SCR_META_KEY_CKPT     ("CKPT")
#define SCR_META_KEY_RANKS    ("RANKS")
#define SCR_META_KEY_RANK     ("RANK")
#define SCR_META_KEY_ORIG     ("ORIG")
#define SCR_META_KEY_PATH     ("PATH")
#define SCR_META_KEY_NAME     ("NAME")
#define SCR_META_KEY_FILE     ("FILE")
#define SCR_META_KEY_SIZE     ("SIZE")
#define SCR_META_KEY_TYPE     ("TYPE")
#define SCR_META_KEY_TYPE_USER ("USER")
#define SCR_META_KEY_TYPE_XOR  ("XOR")
#define SCR_META_KEY_CRC      ("CRC")
#define SCR_META_KEY_COMPLETE ("COMPLETE")

#define SCR_KEY_COPY_XOR_CHUNK   ("CHUNK")
#define SCR_KEY_COPY_XOR_DATASET ("DSET")
#define SCR_KEY_COPY_XOR_CURRENT ("CURRENT")
#define SCR_KEY_COPY_XOR_PARTNER ("PARTNER")
#define SCR_KEY_COPY_XOR_FILES   ("FILES")
#define SCR_KEY_COPY_XOR_FILE    ("FILE")
#define SCR_KEY_COPY_XOR_RANKS   ("RANKS")
#define SCR_KEY_COPY_XOR_RANK    ("RANK")
#define SCR_KEY_COPY_XOR_GROUP   ("GROUP")
#define SCR_KEY_COPY_XOR_GROUP_RANKS ("RANKS")
#define SCR_KEY_COPY_XOR_GROUP_RANK  ("RANK")

/*
=========================================
Define hash and element structures
=========================================
*/

/* define the structure for the head of a hash */
struct scr_hash_elem_struct;
LIST_HEAD(scr_hash_struct, scr_hash_elem_struct);

/* define the structure for an element of a hash */
struct scr_hash_elem_struct {
  char* key;
  struct scr_hash_struct* hash;
  LIST_ENTRY(scr_hash_elem_struct) pointers;
};

typedef struct scr_hash_struct      scr_hash;
typedef struct scr_hash_elem_struct scr_hash_elem;

/*
=========================================
Allocate and delete hash objects
=========================================
*/

/* allocates a new hash */
scr_hash* scr_hash_new();

/* frees a hash */
int scr_hash_delete(scr_hash** ptr_hash);

/*
=========================================
size, get, set, unset, and merge functions
=========================================
*/

/* return size of hash (number of keys) */
int scr_hash_size(const scr_hash* hash);

/* given a hash and a key, return the hash associated with key, returns NULL if not found */
scr_hash* scr_hash_get(const scr_hash* hash, const char* key);

/* given a hash, a key, and a hash value, set (or reset) the key's hash */
scr_hash* scr_hash_set(scr_hash* hash, const char* key, scr_hash* hash_value);

/* given a hash and a key, extract and return hash for specified key, returns NULL if not found */
scr_hash* scr_hash_extract(scr_hash* hash, const char* key);

/* given a hash and a key, extract and delete any matching element */
int scr_hash_unset(scr_hash* hash, const char* key);

/* unset all values in the hash, but don't delete it */
int scr_hash_unset_all(scr_hash* hash);

/* merges (copies) elements from hash2 into hash1 */
int scr_hash_merge(scr_hash* hash1, const scr_hash* hash2);

/* traverse the given hash using a printf-like format string setting an arbitrary list of keys
 * to set (or reset) the hash associated with the last key */
scr_hash* scr_hash_setf(scr_hash* hash, scr_hash* hash_value, const char* format, ...);

/* same as above, but simply returns the hash associated with the list of keys */
scr_hash* scr_hash_getf(const scr_hash* hash, const char* format, ...);

/* sort the hash assuming the keys are strings */
int scr_hash_sort(scr_hash* hash, int direction);

/* sort the hash assuming the keys are ints */
int scr_hash_sort_int(scr_hash* hash, int direction);

/* return list of keys in hash as integers, caller must free list */
int scr_hash_list_int(const scr_hash* hash, int* num, int** list);

/*
=========================================
get, set, and unset hashes using a key/value pair
=========================================
*/

/* shortcut to create a key and subkey in a hash with one call */
scr_hash* scr_hash_set_kv(scr_hash* hash, const char* key, const char* val);

/* same as scr_hash_set_kv, but with the subkey specified as an int */
scr_hash* scr_hash_set_kv_int(scr_hash* hash, const char* key, int val);

/* shortcut to get hash assocated with the subkey of a key in a hash with one call */
scr_hash* scr_hash_get_kv(const scr_hash* hash, const char* key, const char* val);

/* same as scr_hash_get_kv, but with the subkey specified as an int */
scr_hash* scr_hash_get_kv_int(const scr_hash* hash, const char* key, int val);

/* unset subkey under key, and if that removes the only element for key, unset key as well */
int scr_hash_unset_kv(scr_hash* hash, const char* key, const char* val);

/* same as scr_hash_unset_kv, but with the subkey specified as an int */
int scr_hash_unset_kv_int(scr_hash* hash, const char* key, int val);

/*
=========================================
Hash element functions
=========================================
*/

/* returns the first element for a given hash */
scr_hash_elem* scr_hash_elem_first(const scr_hash* hash);

/* given a hash element, returns the next element */
scr_hash_elem* scr_hash_elem_next(const scr_hash_elem* elem);

/* returns a pointer to the key of the specified element */
char* scr_hash_elem_key(const scr_hash_elem* elem);

/* same as scr_hash_elem_key, but converts the key as an int (returns 0 if key is not defined) */
int scr_hash_elem_key_int(const scr_hash_elem* elem);

/* returns a pointer to the hash of the specified element */
scr_hash* scr_hash_elem_hash(const scr_hash_elem* elem);

/* given a hash and a key, find first matching element and return its address, returns NULL if not found */
scr_hash_elem* scr_hash_elem_get(const scr_hash* hash, const char* key);

/* given a hash and a key, return a pointer to the key of the first element of that key's hash */
char* scr_hash_elem_get_first_val(const scr_hash* hash, const char* key);

/* given a hash and a key, find first matching element, remove it from the hash, and return it */
scr_hash_elem* scr_hash_elem_extract(scr_hash* hash, const char* key);

/* given a hash and a key, find first matching element, remove it from the hash, and return it */
scr_hash_elem* scr_hash_elem_extract_int(scr_hash* hash, int key);

/* extract element from hash given the hash and the address of the element */
scr_hash_elem* scr_hash_elem_extract_by_addr(scr_hash* hash, scr_hash_elem* elem);

char* scr_hash_get_val(const scr_hash* hash, const char* key);

/*
=========================================
Pack and unpack hash and elements into a char buffer
=========================================
*/

/* computes the number of bytes needed to pack the given hash */
size_t scr_hash_pack_size(const scr_hash* hash);

/* packs the given hash into specified buf and returns the number of bytes written */
size_t scr_hash_pack(char* buf, const scr_hash* hash);

/* unpacks hash from specified buffer into given hash object and returns the number of bytes read */
size_t scr_hash_unpack(const char* buf, scr_hash* hash);

/*
=========================================
Read and write hash to a file
=========================================
*/

/* persist hash in newly allocated buffer,
 * return buffer address and size to be freed by caller */
int scr_hash_write_persist(void** ptr_buf, size_t* ptr_size, const scr_hash* hash);

/* executes logic of scr_has_write with opened file descriptor */
ssize_t scr_hash_write_fd(const char* file, int fd, const scr_hash* hash);

/* executes logic of scr_hash_read using an opened file descriptor */
ssize_t scr_hash_read_fd(const char* file, int fd, scr_hash* hash);

/* write the given hash to specified file */
int scr_hash_write(const char* file, const scr_hash* hash);

/* opens specified file and reads in a hash storing its contents in the given hash object */
int scr_hash_read(const char* file, scr_hash* hash);

/* write the given hash to specified file */
int scr_hash_write_path(const scr_path* file, const scr_hash* hash);

/* opens specified file and reads in a hash storing its contents in the given hash object */
int scr_hash_read_path(const scr_path* file, scr_hash* hash);

/* given a filename and hash, lock/open/read/close/unlock the file storing its contents in the hash */
int scr_hash_read_with_lock(const char* file, scr_hash* hash);

/* given a filename and hash, lock the file, open it, and read it into hash, set fd to the opened file descriptor */
int scr_hash_lock_open_read(const char* file, int* fd, scr_hash* hash);

/* given a filename, an opened file descriptor, and a hash, overwrite file with hash, close, and unlock file */
int scr_hash_write_close_unlock(const char* file, int* fd, const scr_hash* hash);

/*
=========================================
Print hash and elements to stdout for debugging
=========================================
*/

/* prints specified hash to stdout for debugging */
int scr_hash_print(const scr_hash* hash, int indent);

#endif
