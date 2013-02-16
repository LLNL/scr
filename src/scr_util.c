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

/* Reads parameters from environment and configuration files */

#include "scr.h"
#include "scr_err.h"
#include "scr_io.h" /* for byteswap operations */
#include "scr_util.h"

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

/* TODO: support processing of byte values */

unsigned long long kilo = 1024;
unsigned long long mega = 1024*1024;
unsigned long long giga = 1024*1024*1024;

/* given a string, convert it to a double and write that value to val */
int scr_atod(char* str, double* val)
{
  /* check that we have a string */
  if (str == NULL) {
    scr_err("scr_atod: Can't convert NULL string to double @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* check that we have a value to write to */
  if (val == NULL) {
    scr_err("scr_atod: NULL address to store value @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* convert string to double */
  errno = 0;
  double value = strtod(str, NULL);
  if (errno == 0) {
    /* got a valid double, set our output parameter */
    *val = value;
  } else {
    /* could not interpret value */
    scr_err("scr_atod: Invalid double: %s @ %s:%d",
            str, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  return SCR_SUCCESS;
}

/* converts string like 10mb to unsigned long long integer value of 10*1024*1024 */
int scr_abtoull(char* str, unsigned long long* val)
{
  /* check that we have a string */
  if (str == NULL) {
    scr_err("scr_abtoull: Can't convert NULL string to bytes @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* check that we have a value to write to */
  if (val == NULL) {
    scr_err("scr_abtoull: NULL address to store value @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* pull the floating point portion of our byte string off */
  errno = 0;
  char* next = NULL;
  double num = strtod(str, &next);
  if (errno != 0) {
    scr_err("scr_abtoull: Invalid double: %s @ %s:%d",
            str, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* now extract any units, e.g. KB MB GB, etc */
  unsigned long long units = 1;
  if (*next != '\0') {
    switch(*next) {
    case 'k':
    case 'K':
      units = kilo;
      break;
    case 'm':
    case 'M':
      units = mega;
      break;
    case 'g':
    case 'G':
      units = giga;
      break;
    default:
      scr_err("scr_abtoull: Unexpected byte string %s @ %s:%d",
              str, __FILE__, __LINE__
      );
      return SCR_FAILURE;
    }

    next++;

    /* handle optional b or B character, e.g. in 10KB */
    if (*next == 'b' || *next == 'B') {
      next++;
    }

    /* check that we've hit the end of the string */
    if (*next != 0) {
      scr_err("scr_abtoull: Unexpected byte string: %s @ %s:%d",
              str, __FILE__, __LINE__
      );
      return SCR_FAILURE;
    }
  }

  /* check that we got a positive value */
  if (num < 0) {
    scr_err("scr_abtoull: Byte string must be positive: %s @ %s:%d",
            str, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* multiply by our units and set out return value */
  *val = (unsigned long long) (num * (double) units);

  return SCR_SUCCESS;
}

/* caller really passes in a void**, but we define it as just void* to avoid printing
 * a bunch of warnings */
void scr_free(void* p)
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

/* allocates a block of memory and aligns it to specified alignment */
void* scr_align_malloc(size_t size, size_t align)
{
  void* buf = NULL;
  if (posix_memalign(&buf, align, size) != 0) {
    return NULL;
  }
  return buf;

#if 0
  /* allocate size + one block + room to store our starting address */
  size_t bytes = size + align + sizeof(void*);

  /* allocate memory */
  void* start = malloc(bytes);
  if (start == NULL) {
    return NULL;
  }

  /* make room to store our starting address */
  void* buf = start + sizeof(void*);

  /* TODO: Compilers don't like modulo division on pointers */
  /* now align the buffer address to a block boundary */
  unsigned long long mask = (unsigned long long) (align - 1);
  unsigned long long addr = (unsigned long long) buf;
  unsigned long long offset = addr & mask;
  if (offset != 0) {
    buf = buf + (align - offset);
  }

  /* store the starting address in the bytes immediately before the buffer */
  void** tmp = buf - sizeof(void*);
  *tmp = start;

  /* finally, return the buffer address to the user */
  return buf;
#endif
}

/* frees a blocked allocated with a call to scr_align_malloc */
void scr_align_free(void* p)
{
  scr_free(p);

#if 0
  /* first lookup the starting address from the bytes immediately before the buffer */
  void** tmp = buf - sizeof(void*);
  void* start = *tmp;

  /* now free the memory */
  free(start);
#endif
}

/*sprintfs a formatted string into an newly allocated string */
char* scr_strdupf(const char* format, ...)
{
  va_list args;
  char* str = NULL;

  /* check that we have a format string */
  if (format == NULL) {
    return NULL;
  }

  /* compute the size of the string we need to allocate */
  va_start(args, format);
  int size = vsnprintf(NULL, 0, format, args) + 1;
  va_end(args);

  /* allocate and print the string */
  if (size > 0) {
    str = (char*) malloc(size);

    va_start(args, format);
    vsnprintf(str, size, format, args);
    va_end(args);
  }

  return str;
}

/* returns the current linux timestamp */
int64_t scr_time_usecs()
{
  struct timeval tv;
  gettimeofday(&tv, NULL);
  int64_t now = ((int64_t) tv.tv_sec) * 1000000 + ((int64_t) tv.tv_usec);
  return now;
}

/* returns the current linux timestamp (secs + usecs since epoch) as a double */
double scr_seconds()
{
  struct timeval tv;
  gettimeofday(&tv, NULL);
  double secs = (double) tv.tv_sec + (double) tv.tv_usec / (double) 1000000.0;
  return secs;
}

/* pack an unsigned 16 bit value to specified buffer in network order */
int scr_pack_uint16_t(void* buf, size_t buf_size, size_t* buf_pos, uint16_t val)
{
  /* check that we have a valid pointer to a buffer position value */
  if (buf == NULL || buf_pos == NULL) {
    scr_err("NULL pointer to buffer or buffer position @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* get current buffer position */
  size_t pos = *buf_pos;

  /* compute final buffer position */
  size_t pos_final = pos + sizeof(val);

  /* check that we won't overrun the buffer */
  if (pos_final > buf_size) {
    scr_err("Attempting to pack too many bytes into buffer @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* convert value to network order */
  uint16_t val_network = scr_hton16(val);

  /* pack value into buffer */
  memcpy(buf + pos, &val_network, sizeof(val_network));

  /* update position */
  *buf_pos = pos_final;

  return SCR_SUCCESS;
}

/* pack an unsigned 32 bit value to specified buffer in network order */
int scr_pack_uint32_t(void* buf, size_t buf_size, size_t* buf_pos, uint32_t val)
{
  /* check that we have a valid pointer to a buffer position value */
  if (buf == NULL || buf_pos == NULL) {
    scr_err("NULL pointer to buffer or buffer position @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* get current buffer position */
  size_t pos = *buf_pos;

  /* compute final buffer position */
  size_t pos_final = pos + sizeof(val);

  /* check that we won't overrun the buffer */
  if (pos_final > buf_size) {
    scr_err("Attempting to pack too many bytes into buffer @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* convert value to network order */
  uint32_t val_network = scr_hton32(val);

  /* pack value into buffer */
  memcpy(buf + pos, &val_network, sizeof(val_network));

  /* update position */
  *buf_pos = pos_final;

  return SCR_SUCCESS;
}

/* pack an unsigned 64 bit value to specified buffer in network order */
int scr_pack_uint64_t(void* buf, size_t buf_size, size_t* buf_pos, uint64_t val)
{
  /* check that we have a valid pointer to a buffer position value */
  if (buf == NULL || buf_pos == NULL) {
    scr_err("NULL pointer to buffer or buffer position @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* get current buffer position */
  size_t pos = *buf_pos;

  /* compute final buffer position */
  size_t pos_final = pos + sizeof(val);

  /* check that we won't overrun the buffer */
  if (pos_final > buf_size) {
    scr_err("Attempting to pack too many bytes into buffer @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* convert value to network order */
  uint64_t val_network = scr_hton64(val);

  /* pack value into buffer */
  memcpy(buf + pos, &val_network, sizeof(val_network));

  /* update position */
  *buf_pos = pos_final;

  return SCR_SUCCESS;
}

/* unpack an unsigned 16 bit value to specified buffer in host order */
int scr_unpack_uint16_t(const void* buf, size_t buf_size, size_t* buf_pos, uint16_t* val)
{
  /* check that we have a valid pointer to a buffer position value */
  if (buf == NULL || buf_pos == NULL || val == NULL) {
    scr_err("NULL pointer to buffer, buffer position, or value @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* get current buffer position */
  size_t pos = *buf_pos;

  /* compute final buffer position */
  size_t pos_final = pos + sizeof(uint16_t);

  /* check that we won't overrun the buffer */
  if (pos_final > buf_size) {
    scr_err("Attempting to unpack too many bytes into buffer @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* read value from buffer (stored in network order) */
  uint16_t val_network;
  memcpy(&val_network, buf + pos, sizeof(val_network));

  /* conver to host order */
  *val = scr_ntoh16(val_network);

  /* update position */
  *buf_pos = pos_final;

  return SCR_SUCCESS;
}

/* unpack an unsigned 32 bit value to specified buffer in host order */
int scr_unpack_uint32_t(const void* buf, size_t buf_size, size_t* buf_pos, uint32_t* val)
{
  /* check that we have a valid pointer to a buffer position value */
  if (buf == NULL || buf_pos == NULL || val == NULL) {
    scr_err("NULL pointer to buffer, buffer position, or value @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* get current buffer position */
  size_t pos = *buf_pos;

  /* compute final buffer position */
  size_t pos_final = pos + sizeof(uint32_t);

  /* check that we won't overrun the buffer */
  if (pos_final > buf_size) {
    scr_err("Attempting to unpack too many bytes into buffer @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* read value from buffer (stored in network order) */
  uint32_t val_network;
  memcpy(&val_network, buf + pos, sizeof(val_network));

  /* conver to host order */
  *val = scr_ntoh32(val_network);

  /* update position */
  *buf_pos = pos_final;

  return SCR_SUCCESS;
}

/* unpack an unsigned 64 bit value to specified buffer in host order */
int scr_unpack_uint64_t(const void* buf, size_t buf_size, size_t* buf_pos, uint64_t* val)
{
  /* check that we have a valid pointer to a buffer position value */
  if (buf == NULL || buf_pos == NULL || val == NULL) {
    scr_err("NULL pointer to buffer, buffer position, or value @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* get current buffer position */
  size_t pos = *buf_pos;

  /* compute final buffer position */
  size_t pos_final = pos + sizeof(uint64_t);

  /* check that we won't overrun the buffer */
  if (pos_final > buf_size) {
    scr_err("Attempting to unpack too many bytes into buffer @ %s:%d",
            __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* read value from buffer (stored in network order) */
  uint64_t val_network;
  memcpy(&val_network, buf + pos, sizeof(val_network));

  /* conver to host order */
  *val = scr_ntoh64(val_network);

  /* update position */
  *buf_pos = pos_final;

  return SCR_SUCCESS;
}
