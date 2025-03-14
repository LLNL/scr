/** \file kvtree_util.h
 *  \ingroup kvtree
 *  \brief Implements some common tasks for operations on kvtrees */

#ifndef KVTREE_UTIL_H
#define KVTREE_UTIL_H

#include "kvtree.h"

/* compute crc32 */
#include <zlib.h>

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
extern "C" {
#endif

/** \name setter functions
 * the set functions all unset the key before setting it to ensure
 * that only one value is ever defined for a given key */
///@{
int kvtree_util_set_bytecount(kvtree* hash, const char* key, unsigned long count);

int kvtree_util_set_crc32(kvtree* hash, const char* key, uLong crc);

int kvtree_util_set_int(kvtree* hash, const char* key, int value);

int kvtree_util_set_unsigned_long(kvtree* hash, const char* key, unsigned long value);

int kvtree_util_set_str(kvtree* hash, const char* key, const char* value);

int kvtree_util_set_int64(kvtree* hash, const char* key, int64_t value);

int kvtree_util_set_double(kvtree* hash, const char* key, double value);

int kvtree_util_set_ptr(kvtree* hash, const char* key, void* ptr);
///@}

/** \name getter functions
 * the get functions all return KVTREE_FAILURE if any of the following hold:
 *  - the input hash or the key is NULL
 *  - the hash has no entry for the specified key
 *  - the specified key does not have a value
 * otherwise, the function returns KVTREE_SUCCESS and the key value is translated
 * into the appropriate type and stored in the specified buffer */
///@{
int kvtree_util_get_bytecount(const kvtree* hash, const char* key, unsigned long* val);

int kvtree_util_get_crc32(const kvtree* hash, const char* key, uLong* val);

int kvtree_util_get_int(const kvtree* hash, const char* key, int* value);

int kvtree_util_get_unsigned_long(const kvtree* hash, const char* key, unsigned long* value);

int kvtree_util_get_str(const kvtree* hash, const char* key, char** value);

int kvtree_util_get_int64(const kvtree* hash, const char* key, int64_t* value);

int kvtree_util_get_double(const kvtree* hash, const char* key, double* value);

int kvtree_util_get_ptr(const kvtree* hash, const char* key, void** value);
///@}

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
} /* extern "C" */
#endif

#endif
