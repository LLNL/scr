/* Reads parameters from environment and configuration files */

#include "kvtree.h"
#include "kvtree_err.h"
#include "kvtree_io.h" /* for byteswap operations */
#include "kvtree_helpers.h"

#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>
#include <strings.h>

/* variable length args */
#include <errno.h>

/* gettimeofday */
#include <sys/time.h>

/* localtime, asctime */
#include <time.h>

/* pull in things like ULLONG_MAX */
#include <limits.h>

/* TODO: support processing of byte values */

/* given a string, convert it to a double and write that value to val */
int kvtree_atod(char* str, double* val)
{
  /* check that we have a string */
  if (str == NULL) {
    kvtree_err("kvtree_atod: Can't convert NULL string to double @ %s:%d",
            __FILE__, __LINE__
    );
    return KVTREE_FAILURE;
  }

  /* check that we have a value to write to */
  if (val == NULL) {
    kvtree_err("kvtree_atod: NULL address to store value @ %s:%d",
            __FILE__, __LINE__
    );
    return KVTREE_FAILURE;
  }

  /* convert string to double */
  errno = 0;
  double value = strtod(str, NULL);
  if (errno == 0) {
    /* got a valid double, set our output parameter */
    *val = value;
  } else {
    /* could not interpret value */
    kvtree_err("kvtree_atod: Invalid double: %s errno=%d %s @ %s:%d",
            str, errno, strerror(errno), __FILE__, __LINE__
    );
    return KVTREE_FAILURE;
  }

  return KVTREE_SUCCESS;
}

/* allocate size bytes, returns NULL if size == 0,
 * calls kvtree_abort if allocation fails */
void* kvtree_malloc(size_t size, const char* file, int line)
{
  void* ptr = NULL;
  if (size > 0) {
    ptr = malloc(size);
    if (ptr == NULL) {
      kvtree_abort(-1, "Failed to allocate %llu bytes @ %s:%d", file, line);
    }
  }
  return ptr;
}

/* caller really passes in a void**, but we define it as just void* to avoid printing
 * a bunch of warnings */
void kvtree_free(void* p)
{
  /* verify that we got a valid pointer to a pointer */
  if (p != NULL) {
    /* free memory if there is any */
    void* ptr = *(void**)p;
    if (ptr != NULL) {
       free(ptr);
    }

    /* set caller's pointer to NULL */
    *(void**)p = NULL;
  }
}

/* pack an unsigned 16 bit value to specified buffer in network order */
int kvtree_pack_uint16_t(void* buf, size_t buf_size, size_t* buf_pos, uint16_t val)
{
  /* check that we have a valid pointer to a buffer position value */
  if (buf == NULL || buf_pos == NULL) {
    kvtree_err("NULL pointer to buffer or buffer position @ %s:%d",
            __FILE__, __LINE__
    );
    return KVTREE_FAILURE;
  }

  /* get current buffer position */
  size_t pos = *buf_pos;

  /* compute final buffer position */
  size_t pos_final = pos + sizeof(val);

  /* check that we won't overrun the buffer */
  if (pos_final > buf_size) {
    kvtree_err("Attempting to pack too many bytes into buffer @ %s:%d",
            __FILE__, __LINE__
    );
    return KVTREE_FAILURE;
  }

  /* convert value to network order */
  uint16_t val_network = kvtree_hton16(val);

  /* pack value into buffer */
  memcpy(buf + pos, &val_network, sizeof(val_network));

  /* update position */
  *buf_pos = pos_final;

  return KVTREE_SUCCESS;
}

/* pack an unsigned 32 bit value to specified buffer in network order */
int kvtree_pack_uint32_t(void* buf, size_t buf_size, size_t* buf_pos, uint32_t val)
{
  /* check that we have a valid pointer to a buffer position value */
  if (buf == NULL || buf_pos == NULL) {
    kvtree_err("NULL pointer to buffer or buffer position @ %s:%d",
            __FILE__, __LINE__
    );
    return KVTREE_FAILURE;
  }

  /* get current buffer position */
  size_t pos = *buf_pos;

  /* compute final buffer position */
  size_t pos_final = pos + sizeof(val);

  /* check that we won't overrun the buffer */
  if (pos_final > buf_size) {
    kvtree_err("Attempting to pack too many bytes into buffer @ %s:%d",
            __FILE__, __LINE__
    );
    return KVTREE_FAILURE;
  }

  /* convert value to network order */
  uint32_t val_network = kvtree_hton32(val);

  /* pack value into buffer */
  memcpy(buf + pos, &val_network, sizeof(val_network));

  /* update position */
  *buf_pos = pos_final;

  return KVTREE_SUCCESS;
}

/* pack an unsigned 64 bit value to specified buffer in network order */
int kvtree_pack_uint64_t(void* buf, size_t buf_size, size_t* buf_pos, uint64_t val)
{
  /* check that we have a valid pointer to a buffer position value */
  if (buf == NULL || buf_pos == NULL) {
    kvtree_err("NULL pointer to buffer or buffer position @ %s:%d",
            __FILE__, __LINE__
    );
    return KVTREE_FAILURE;
  }

  /* get current buffer position */
  size_t pos = *buf_pos;

  /* compute final buffer position */
  size_t pos_final = pos + sizeof(val);

  /* check that we won't overrun the buffer */
  if (pos_final > buf_size) {
    kvtree_err("Attempting to pack too many bytes into buffer @ %s:%d",
            __FILE__, __LINE__
    );
    return KVTREE_FAILURE;
  }

  /* convert value to network order */
  uint64_t val_network = kvtree_hton64(val);

  /* pack value into buffer */
  memcpy(buf + pos, &val_network, sizeof(val_network));

  /* update position */
  *buf_pos = pos_final;

  return KVTREE_SUCCESS;
}

/* unpack an unsigned 16 bit value to specified buffer in host order */
int kvtree_unpack_uint16_t(const void* buf, size_t buf_size, size_t* buf_pos, uint16_t* val)
{
  /* check that we have a valid pointer to a buffer position value */
  if (buf == NULL || buf_pos == NULL || val == NULL) {
    kvtree_err("NULL pointer to buffer, buffer position, or value @ %s:%d",
            __FILE__, __LINE__
    );
    return KVTREE_FAILURE;
  }

  /* get current buffer position */
  size_t pos = *buf_pos;

  /* compute final buffer position */
  size_t pos_final = pos + sizeof(uint16_t);

  /* check that we won't overrun the buffer */
  if (pos_final > buf_size) {
    kvtree_err("Attempting to unpack too many bytes into buffer @ %s:%d",
            __FILE__, __LINE__
    );
    return KVTREE_FAILURE;
  }

  /* read value from buffer (stored in network order) */
  uint16_t val_network;
  memcpy(&val_network, buf + pos, sizeof(val_network));

  /* conver to host order */
  *val = kvtree_ntoh16(val_network);

  /* update position */
  *buf_pos = pos_final;

  return KVTREE_SUCCESS;
}

/* unpack an unsigned 32 bit value to specified buffer in host order */
int kvtree_unpack_uint32_t(const void* buf, size_t buf_size, size_t* buf_pos, uint32_t* val)
{
  /* check that we have a valid pointer to a buffer position value */
  if (buf == NULL || buf_pos == NULL || val == NULL) {
    kvtree_err("NULL pointer to buffer, buffer position, or value @ %s:%d",
            __FILE__, __LINE__
    );
    return KVTREE_FAILURE;
  }

  /* get current buffer position */
  size_t pos = *buf_pos;

  /* compute final buffer position */
  size_t pos_final = pos + sizeof(uint32_t);

  /* check that we won't overrun the buffer */
  if (pos_final > buf_size) {
    kvtree_err("Attempting to unpack too many bytes into buffer @ %s:%d",
            __FILE__, __LINE__
    );
    return KVTREE_FAILURE;
  }

  /* read value from buffer (stored in network order) */
  uint32_t val_network;
  memcpy(&val_network, buf + pos, sizeof(val_network));

  /* conver to host order */
  *val = kvtree_ntoh32(val_network);

  /* update position */
  *buf_pos = pos_final;

  return KVTREE_SUCCESS;
}

/* unpack an unsigned 64 bit value to specified buffer in host order */
int kvtree_unpack_uint64_t(const void* buf, size_t buf_size, size_t* buf_pos, uint64_t* val)
{
  /* check that we have a valid pointer to a buffer position value */
  if (buf == NULL || buf_pos == NULL || val == NULL) {
    kvtree_err("NULL pointer to buffer, buffer position, or value @ %s:%d",
            __FILE__, __LINE__
    );
    return KVTREE_FAILURE;
  }

  /* get current buffer position */
  size_t pos = *buf_pos;

  /* compute final buffer position */
  size_t pos_final = pos + sizeof(uint64_t);

  /* check that we won't overrun the buffer */
  if (pos_final > buf_size) {
    kvtree_err("Attempting to unpack too many bytes into buffer @ %s:%d",
            __FILE__, __LINE__
    );
    return KVTREE_FAILURE;
  }

  /* read value from buffer (stored in network order) */
  uint64_t val_network;
  memcpy(&val_network, buf + pos, sizeof(val_network));

  /* conver to host order */
  *val = kvtree_ntoh64(val_network);

  /* update position */
  *buf_pos = pos_final;

  return KVTREE_SUCCESS;
}
