/* Copyright (c) 2012, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory.
 * Written by Adam Moody <moody20@llnl.gov>.
 * LLNL-CODE-557516.
 * All rights reserved.
 * This file is part of the DTCMP library.
 * For details, see https://github.com/hpc/dtcmp
 * Please also read this file: LICENSE.TXT. */

#include <string.h>
#include "mpi.h"
#include "dtcmp_internal.h"
#include "dtcmp_ops.h"

/* initialize operation handle with explicit byte displacement */
void dtcmp_op_hinit(
  dtcmp_op_types type,
  MPI_Datatype key,
  DTCMP_Op_fn fn,
  MPI_Aint cmpdisp,
  MPI_Aint disp,
  DTCMP_Op series,
  DTCMP_Op* cmp)
{
  dtcmp_op_handle_t* c = (dtcmp_op_handle_t*) malloc(sizeof(dtcmp_op_handle_t));
  if (c != NULL) {
    c->magic   = 1;
    c->type    = type;
    MPI_Type_dup(key, &(c->key));
    c->fn      = fn;
    c->cmpdisp = cmpdisp;
    c->disp    = disp;
    c->series  = series;
  }
  *cmp = (DTCMP_Op) c;
}

/* initialize operation handle with extent(key) */
void dtcmp_op_init(
  dtcmp_op_types type,
  MPI_Datatype key,
  DTCMP_Op_fn fn,
  DTCMP_Op series,
  DTCMP_Op* cmp)
{
  /* get extent of key type */
  MPI_Aint lb, extent;
  MPI_Type_get_extent(key, &lb, &extent);

  /* use extent of key type as displacement to second item */
  MPI_Aint cmpdisp = 0;
  dtcmp_op_hinit(type, key, fn, cmpdisp, extent, series, cmp);
}

/* make a copy of src operation and save in dst */
void dtcmp_op_copy(DTCMP_Op* dst, DTCMP_Op src)
{
  dtcmp_op_handle_t* s = (dtcmp_op_handle_t*) src;
  if (s->series == DTCMP_OP_NULL) {
    dtcmp_op_hinit(s->type, s->key, s->fn, s->cmpdisp, s->disp, s->series, dst);
  } else {
    DTCMP_Op copy;
    dtcmp_op_copy(&copy, s->series);
    dtcmp_op_hinit(s->type, s->key, s->fn, s->cmpdisp, s->disp, copy, dst);
  }
}

int dtcmp_op_eval(const void* a, const void* b, DTCMP_Op cmp)
{
  /* get pointer to handle and comparison operation */
  dtcmp_op_handle_t* c = (dtcmp_op_handle_t*) cmp;
  DTCMP_Op_fn compare = c->fn;

  /* invoke comparison function to compare a and b */
  const void* a_cur = (const char*)a + c->cmpdisp;
  const void* b_cur = (const char*)b + c->cmpdisp;
  int rc = (*compare)(a_cur, b_cur);
  if (rc != 0) {
    /* comparison found the two are not equal, just return our result */
    return rc;
  } else {
    /* a and b are equal, see if there is a series comparison */
    if (c->series != DTCMP_OP_NULL) {
      /* advance pointers to point past end of exurrent key */
      const void* a_new = (const char*)a + c->disp;
      const void* b_new = (const char*)b + c->disp;

      /* invoke series comparison function */
      return dtcmp_op_eval(a_new, b_new, c->series);
    } else {
      /* a and b are equal and there is no series comparison,
       * just return our result (equal) */
      return rc;
    }
  }
}

/* -----------------------------------------
 * Comaprison routines for pre-defined ops
 * ----------------------------------------- */

int dtcmp_op_fn_short_ascend(const void* bufa, const void* bufb)
{
  short a = *(short*)bufa;
  short b = *(short*)bufb;
  if (a < b) {
    return -1;
  } else if (b < a) {
    return  1;
  } else {
    return 0;
  }
}

int dtcmp_op_fn_short_descend(const void* bufa, const void* bufb)
{
  short a = *(short*)bufa;
  short b = *(short*)bufb;
  if (a > b) {
    return -1;
  } else if (b > a) {
    return  1;
  } else {
    return 0;
  }
}

int dtcmp_op_fn_int_ascend(const void* bufa, const void* bufb)
{
  int a = *(int*)bufa;
  int b = *(int*)bufb;
  if (a < b) {
    return -1;
  } else if (b < a) {
    return  1;
  } else {
    return 0;
  }
}

int dtcmp_op_fn_int_descend(const void* bufa, const void* bufb)
{
  int a = *(int*)bufa;
  int b = *(int*)bufb;
  if (a > b) {
    return -1;
  } else if (b > a) {
    return  1;
  } else {
    return 0;
  }
}

int dtcmp_op_fn_long_ascend(const void* bufa, const void* bufb)
{
  long a = *(long*)bufa;
  long b = *(long*)bufb;
  if (a < b) {
    return -1;
  } else if (b < a) {
    return  1;
  } else {
    return 0;
  }
}

int dtcmp_op_fn_long_descend(const void* bufa, const void* bufb)
{
  long a = *(long*)bufa;
  long b = *(long*)bufb;
  if (a > b) {
    return -1;
  } else if (b > a) {
    return  1;
  } else {
    return 0;
  }
}

int dtcmp_op_fn_longlong_ascend(const void* bufa, const void* bufb)
{
  long long a = *(long long*)bufa;
  long long b = *(long long*)bufb;
  if (a < b) {
    return -1;
  } else if (b < a) {
    return  1;
  } else {
    return 0;
  }
}

int dtcmp_op_fn_longlong_descend(const void* bufa, const void* bufb)
{
  long long a = *(long long*)bufa;
  long long b = *(long long*)bufb;
  if (a > b) {
    return -1;
  } else if (b > a) {
    return  1;
  } else {
    return 0;
  }
}

int dtcmp_op_fn_unsignedshort_ascend(const void* bufa, const void* bufb)
{
  unsigned short a = *(unsigned short*)bufa;
  unsigned short b = *(unsigned short*)bufb;
  if (a < b) {
    return -1;
  } else if (b < a) {
    return  1;
  } else {
    return 0;
  }
}

int dtcmp_op_fn_unsignedshort_descend(const void* bufa, const void* bufb)
{
  unsigned short a = *(unsigned short*)bufa;
  unsigned short b = *(unsigned short*)bufb;
  if (a > b) {
    return -1;
  } else if (b > a) {
    return  1;
  } else {
    return 0;
  }
}

int dtcmp_op_fn_unsignedint_ascend(const void* bufa, const void* bufb)
{
  unsigned int a = *(unsigned int*)bufa;
  unsigned int b = *(unsigned int*)bufb;
  if (a < b) {
    return -1;
  } else if (b < a) {
    return  1;
  } else {
    return 0;
  }
}

int dtcmp_op_fn_unsignedint_descend(const void* bufa, const void* bufb)
{
  unsigned int a = *(unsigned int*)bufa;
  unsigned int b = *(unsigned int*)bufb;
  if (a > b) {
    return -1;
  } else if (b > a) {
    return  1;
  } else {
    return 0;
  }
}

int dtcmp_op_fn_unsignedlong_ascend(const void* bufa, const void* bufb)
{
  unsigned long a = *(unsigned long*)bufa;
  unsigned long b = *(unsigned long*)bufb;
  if (a < b) {
    return -1;
  } else if (b < a) {
    return  1;
  } else {
    return 0;
  }
}

int dtcmp_op_fn_unsignedlong_descend(const void* bufa, const void* bufb)
{
  unsigned long a = *(unsigned long*)bufa;
  unsigned long b = *(unsigned long*)bufb;
  if (a > b) {
    return -1;
  } else if (b > a) {
    return  1;
  } else {
    return 0;
  }
}

int dtcmp_op_fn_unsignedlonglong_ascend(const void* bufa, const void* bufb)
{
  unsigned long long a = *(unsigned long long*)bufa;
  unsigned long long b = *(unsigned long long*)bufb;
  if (a < b) {
    return -1;
  } else if (b < a) {
    return  1;
  } else {
    return 0;
  }
}

int dtcmp_op_fn_unsignedlonglong_descend(const void* bufa, const void* bufb)
{
  unsigned long long a = *(unsigned long long*)bufa;
  unsigned long long b = *(unsigned long long*)bufb;
  if (a > b) {
    return -1;
  } else if (b > a) {
    return  1;
  } else {
    return 0;
  }
}

int dtcmp_op_fn_int8t_ascend(const void* bufa, const void* bufb)
{
  int8_t a = *(int8_t*)bufa;
  int8_t b = *(int8_t*)bufb;
  if (a < b) {
    return -1;
  } else if (b < a) {
    return  1;
  } else {
    return 0;
  }
}

int dtcmp_op_fn_int8t_descend(const void* bufa, const void* bufb)
{
  int8_t a = *(int8_t*)bufa;
  int8_t b = *(int8_t*)bufb;
  if (a > b) {
    return -1;
  } else if (b > a) {
    return  1;
  } else {
    return 0;
  }
}

int dtcmp_op_fn_int16t_ascend(const void* bufa, const void* bufb)
{
  int16_t a = *(int16_t*)bufa;
  int16_t b = *(int16_t*)bufb;
  if (a < b) {
    return -1;
  } else if (b < a) {
    return  1;
  } else {
    return 0;
  }
}

int dtcmp_op_fn_int16t_descend(const void* bufa, const void* bufb)
{
  int16_t a = *(int16_t*)bufa;
  int16_t b = *(int16_t*)bufb;
  if (a > b) {
    return -1;
  } else if (b > a) {
    return  1;
  } else {
    return 0;
  }
}

int dtcmp_op_fn_int32t_ascend(const void* bufa, const void* bufb)
{
  int32_t a = *(int32_t*)bufa;
  int32_t b = *(int32_t*)bufb;
  if (a < b) {
    return -1;
  } else if (b < a) {
    return  1;
  } else {
    return 0;
  }
}

int dtcmp_op_fn_int32t_descend(const void* bufa, const void* bufb)
{
  int32_t a = *(int32_t*)bufa;
  int32_t b = *(int32_t*)bufb;
  if (a > b) {
    return -1;
  } else if (b > a) {
    return  1;
  } else {
    return 0;
  }
}

int dtcmp_op_fn_int64t_ascend(const void* bufa, const void* bufb)
{
  int64_t a = *(int64_t*)bufa;
  int64_t b = *(int64_t*)bufb;
  if (a < b) {
    return -1;
  } else if (b < a) {
    return  1;
  } else {
    return 0;
  }
}

int dtcmp_op_fn_int64t_descend(const void* bufa, const void* bufb)
{
  int64_t a = *(int64_t*)bufa;
  int64_t b = *(int64_t*)bufb;
  if (a > b) {
    return -1;
  } else if (b > a) {
    return  1;
  } else {
    return 0;
  }
}

int dtcmp_op_fn_uint8t_ascend(const void* bufa, const void* bufb)
{
  uint8_t a = *(uint8_t*)bufa;
  uint8_t b = *(uint8_t*)bufb;
  if (a < b) {
    return -1;
  } else if (b < a) {
    return  1;
  } else {
    return 0;
  }
}

int dtcmp_op_fn_uint8t_descend(const void* bufa, const void* bufb)
{
  uint8_t a = *(uint8_t*)bufa;
  uint8_t b = *(uint8_t*)bufb;
  if (a > b) {
    return -1;
  } else if (b > a) {
    return  1;
  } else {
    return 0;
  }
}

int dtcmp_op_fn_uint16t_ascend(const void* bufa, const void* bufb)
{
  uint16_t a = *(uint16_t*)bufa;
  uint16_t b = *(uint16_t*)bufb;
  if (a < b) {
    return -1;
  } else if (b < a) {
    return  1;
  } else {
    return 0;
  }
}

int dtcmp_op_fn_uint16t_descend(const void* bufa, const void* bufb)
{
  uint16_t a = *(uint16_t*)bufa;
  uint16_t b = *(uint16_t*)bufb;
  if (a > b) {
    return -1;
  } else if (b > a) {
    return  1;
  } else {
    return 0;
  }
}

int dtcmp_op_fn_uint32t_ascend(const void* bufa, const void* bufb)
{
  uint32_t a = *(uint32_t*)bufa;
  uint32_t b = *(uint32_t*)bufb;
  if (a < b) {
    return -1;
  } else if (b < a) {
    return  1;
  } else {
    return 0;
  }
}

int dtcmp_op_fn_uint32t_descend(const void* bufa, const void* bufb)
{
  uint32_t a = *(uint32_t*)bufa;
  uint32_t b = *(uint32_t*)bufb;
  if (a > b) {
    return -1;
  } else if (b > a) {
    return  1;
  } else {
    return 0;
  }
}

int dtcmp_op_fn_uint64t_ascend(const void* bufa, const void* bufb)
{
  uint64_t a = *(uint64_t*)bufa;
  uint64_t b = *(uint64_t*)bufb;
  if (a < b) {
    return -1;
  } else if (b < a) {
    return  1;
  } else {
    return 0;
  }
}

int dtcmp_op_fn_uint64t_descend(const void* bufa, const void* bufb)
{
  uint64_t a = *(uint64_t*)bufa;
  uint64_t b = *(uint64_t*)bufb;
  if (a > b) {
    return -1;
  } else if (b > a) {
    return  1;
  } else {
    return 0;
  }
}

int dtcmp_op_fn_float_ascend(const void* bufa, const void* bufb)
{
  float a = *(float*)bufa;
  float b = *(float*)bufb;
  if (a < b) {
    return -1;
  } else if (b < a) {
    return  1;
  } else {
    return 0;
  }
}

int dtcmp_op_fn_float_descend(const void* bufa, const void* bufb)
{
  float a = *(float*)bufa;
  float b = *(float*)bufb;
  if (a > b) {
    return -1;
  } else if (b > a) {
    return  1;
  } else {
    return 0;
  }
}

int dtcmp_op_fn_double_ascend(const void* bufa, const void* bufb)
{
  double a = *(double*)bufa;
  double b = *(double*)bufb;
  if (a < b) {
    return -1;
  } else if (b < a) {
    return  1;
  } else {
    return 0;
  }
}

int dtcmp_op_fn_double_descend(const void* bufa, const void* bufb)
{
  double a = *(double*)bufa;
  double b = *(double*)bufb;
  if (a > b) {
    return -1;
  } else if (b > a) {
    return  1;
  } else {
    return 0;
  }
}

int dtcmp_op_fn_longdouble_ascend(const void* bufa, const void* bufb)
{
  long double a = *(long double*)bufa;
  long double b = *(long double*)bufb;
  if (a < b) {
    return -1;
  } else if (b < a) {
    return  1;
  } else {
    return 0;
  }
}

int dtcmp_op_fn_longdouble_descend(const void* bufa, const void* bufb)
{
  long double a = *(long double*)bufa;
  long double b = *(long double*)bufb;
  if (a > b) {
    return -1;
  } else if (b > a) {
    return  1;
  } else {
    return 0;
  }
}

/* DTCMP_Op_fn to compare string a to string b */
int dtcmp_op_fn_strcmp_ascend(const void* a, const void* b)
{
  const char* a_str = (const char*) a;
  const char* b_str = (const char*) b;
  int rc = strcmp(a_str, b_str);
  if (rc < 0) {
    return -1;
  } else if (rc > 0) {
    return 1;
  } else {
    return 0;
  }
}

/* DTCMP_Op_fn to compare string a to string b */
int dtcmp_op_fn_strcmp_descend(const void* a, const void* b)
{
  const char* a_str = (const char*) a;
  const char* b_str = (const char*) b;
  int rc = strcmp(a_str, b_str);
  if (rc < 0) {
    return 1;
  } else if (rc > 0) {
    return -1;
  } else {
    return 0;
  }
}
