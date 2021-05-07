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
#include "scr_io.h"
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

/* pull in things like ULLONG_MAX */
#include <limits.h>

/* TODO: support processing of byte values */

/* given a string, convert it to a double and write that value to val */
int scr_atod(const char* str, double* val)
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
    scr_err("scr_atod: Invalid double: %s errno=%d %s @ %s:%d",
            str, errno, strerror(errno), __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  return SCR_SUCCESS;
}

static unsigned long long kilo  =                1024ULL;
static unsigned long long mega  =             1048576ULL;
static unsigned long long giga  =          1073741824ULL;
static unsigned long long tera  =       1099511627776ULL;
static unsigned long long peta  =    1125899906842624ULL;
static unsigned long long exa   = 1152921504606846976ULL;

/* abtoull ==> ASCII bytes to unsigned long long
 * Converts string like "10mb" to unsigned long long integer value
 * of 10*1024*1024.  Input string should have leading number followed
 * by optional units.  The leading number can be a floating point
 * value (read by strtod).  The trailing units consist of one or two
 * letters which should be attached to the number with no space
 * in between.  The units may be upper or lower case, and the second
 * letter if it exists, must be 'b' or 'B' (short for bytes).
 *
 * Valid units: k,K,m,M,g,G,t,T,p,P,e,E
 *
 * Examples: 2kb, 1.5m, 200GB, 1.4T.
 *
 * Returns SCR_SUCCESS if conversion is successful,
 * and SCR_FAILURE otherwise.
 *
 * Returns converted value in val parameter.  This
 * parameter is only updated if successful. */
int scr_abtoull(const char* str, unsigned long long* val)
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
    /* conversion failed */
    scr_err("scr_abtoull: Invalid double: %s errno=%d %s @ %s:%d",
            str, errno, strerror(errno), __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }
  if (str == next) {
    /* no conversion performed */
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
    case 't':
    case 'T':
      units = tera;
      break;
    case 'p':
    case 'P':
      units = peta;
      break;
    case 'e':
    case 'E':
      units = exa;
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

  /* TODO: double check this overflow calculation */
  /* multiply by our units and check for overflow */
  double units_d = (double) units;
  double val_d = num * units_d;
  double max_d = (double) ULLONG_MAX;
  if (val_d > max_d) {
    /* overflow */
    scr_err("scr_abtoull: Byte string overflowed UNSIGNED LONG LONG type: %s @ %s:%d",
            str, __FILE__, __LINE__
    );
    return SCR_FAILURE;
  }

  /* set return value */
  *val = (unsigned long long) val_d;

  return SCR_SUCCESS;
}

/* allocate size bytes, returns NULL if size == 0,
 * calls scr_abort if allocation fails */
void* scr_malloc(size_t size, const char* file, int line)
{
  void* ptr = NULL;
  if (size > 0) {
    ptr = malloc(size);
    if (ptr == NULL) {
      scr_abort(-1, "Failed to allocate %llu bytes @ %s:%d", file, line);
    }
  }
  return ptr;
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
  void* start = SCR_MALLOC(bytes);
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
    str = (char*) SCR_MALLOC(size);

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

int kvtree_read_path(const spath* path, kvtree* tree)
{
  char* file = spath_strdup(path);
  int rc = kvtree_read_file(file, tree);
  scr_free(&file);
  return rc;
}

int kvtree_write_path(const spath* path, const kvtree* tree)
{
  char* file = spath_strdup(path);
  int rc = kvtree_write_file(file, tree);
  scr_free(&file);
  return rc;
}

/* given a string defining SCR_PREFIX value as given by user
 * return spath of fully qualified path, user should free */
spath* scr_get_prefix(const char* str)
{
  spath* prefix_path = NULL;

  /* start with path given by caller if one is provided */
  if (str != NULL) {
    /* user explicitly set SCR_PREFIX to something, so use that */
    prefix_path = spath_from_str(str);

    /* prepend current working dir if prefix is relative */
    if (! spath_is_absolute(prefix_path)) {
      spath* cwd = spath_cwd();
      spath_prepend(prefix_path, cwd);
      spath_delete(&cwd);
    }
  } else {
    /* user didn't set SCR_PREFIX,
     * use the current working directory as a default */
    prefix_path = spath_cwd();
  }

  /* take out any '.', '..', or extra or trailing '/' */
  spath_reduce(prefix_path);

  return prefix_path;
}
