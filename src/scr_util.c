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

#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>
#include <strings.h>

/* variable length args */
#include <errno.h>

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
