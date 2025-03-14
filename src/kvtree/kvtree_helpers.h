#ifndef KVTREE_HELPERS_H
#define KVTREE_HELPERS_H

#define KVTREE_FAILURE (1)

#include <stdlib.h>
#include <stdint.h>

/** \file kvtree_helpers.h
 *  \ingroup kvtree
 *  \brief Helper functions to pack/unpack kvtree contents
 */

/** given a string, convert it to a double and write that value to val */
int kvtree_atod(char* str, double* val);

/** allocate size bytes, returns NULL if size == 0,
 * calls kvtree_abort if allocation fails */
#define KVTREE_MALLOC(X) kvtree_malloc(X, __FILE__, __LINE__);
void* kvtree_malloc(size_t size, const char* file, int line);

/** pass address of pointer to be freed, frees memory if not NULL and sets pointer to NULL */
void kvtree_free(void* ptr);

/** pack an unsigned 16 bit value to specified buffer in network order */
int kvtree_pack_uint16_t(void* buf, size_t buf_size, size_t* buf_pos, uint16_t val);

/** pack an unsigned 32 bit value to specified buffer in network order */
int kvtree_pack_uint32_t(void* buf, size_t buf_size, size_t* buf_pos, uint32_t val);

/** pack an unsigned 64 bit value to specified buffer in network order */
int kvtree_pack_uint64_t(void* buf, size_t buf_size, size_t* buf_pos, uint64_t val);

/** unpack an unsigned 16 bit value to specified buffer in network order */
int kvtree_unpack_uint16_t(const void* buf, size_t buf_size, size_t* buf_pos, uint16_t* val);

/** unpack an unsigned 32 bit value to specified buffer in network order */
int kvtree_unpack_uint32_t(const void* buf, size_t buf_size, size_t* buf_pos, uint32_t* val);

/** unpack an unsigned 64 bit value to specified buffer in network order */
int kvtree_unpack_uint64_t(const void* buf, size_t buf_size, size_t* buf_pos, uint64_t* val);

#endif
