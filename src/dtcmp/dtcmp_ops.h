/* Copyright (c) 2012, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory.
 * Written by Adam Moody <moody20@llnl.gov>.
 * LLNL-CODE-557516.
 * All rights reserved.
 * This file is part of the DTCMP library.
 * For details, see https://github.com/hpc/dtcmp
 * Please also read this file: LICENSE.TXT. */

#ifndef DTCMP_OPS_H_
#define DTCMP_OPS_H_

#include "dtcmp_internal.h"

typedef enum dtcmp_op_types_enum {
  DTCMP_OP_TYPE_BASIC  = 1,
  DTCMP_OP_TYPE_SERIES = 2,
} dtcmp_op_types;
#if 0
#define DTCMP_OP_TYPE_BASIC  (1)
#define DTCMP_OP_TYPE_SERIES (2)
#endif

void dtcmp_op_hinit(
  dtcmp_op_types type,
  MPI_Datatype key,
  DTCMP_Op_fn fn,
  MPI_Aint cmpdisp,
  MPI_Aint disp,
  DTCMP_Op series,
  DTCMP_Op* cmp
);

void dtcmp_op_init(
  dtcmp_op_types type,
  MPI_Datatype key,
  DTCMP_Op_fn fn,
  DTCMP_Op series,
  DTCMP_Op* cmp
);

void dtcmp_op_copy(DTCMP_Op* dst, DTCMP_Op src);

int dtcmp_op_eval(const void* a, const void* b, DTCMP_Op cmp);

int dtcmp_op_fn_short_ascend(const void*, const void*);
int dtcmp_op_fn_short_descend(const void*, const void*);
int dtcmp_op_fn_int_ascend(const void*, const void*);
int dtcmp_op_fn_int_descend(const void*, const void*);
int dtcmp_op_fn_long_ascend(const void*, const void*);
int dtcmp_op_fn_long_descend(const void*, const void*);
int dtcmp_op_fn_longlong_ascend(const void*, const void*);
int dtcmp_op_fn_longlong_descend(const void*, const void*);
int dtcmp_op_fn_unsignedshort_ascend(const void*, const void*);
int dtcmp_op_fn_unsignedshort_descend(const void*, const void*);
int dtcmp_op_fn_unsignedint_ascend(const void*, const void*);
int dtcmp_op_fn_unsignedint_descend(const void*, const void*);
int dtcmp_op_fn_unsignedlong_ascend(const void*, const void*);
int dtcmp_op_fn_unsignedlong_descend(const void*, const void*);
int dtcmp_op_fn_unsignedlonglong_ascend(const void*, const void*);
int dtcmp_op_fn_unsignedlonglong_descend(const void*, const void*);
int dtcmp_op_fn_int8t_ascend(const void*, const void*);
int dtcmp_op_fn_int8t_descend(const void*, const void*);
int dtcmp_op_fn_int16t_ascend(const void*, const void*);
int dtcmp_op_fn_int16t_descend(const void*, const void*);
int dtcmp_op_fn_int32t_ascend(const void*, const void*);
int dtcmp_op_fn_int32t_descend(const void*, const void*);
int dtcmp_op_fn_int64t_ascend(const void*, const void*);
int dtcmp_op_fn_int64t_descend(const void*, const void*);
int dtcmp_op_fn_uint8t_ascend(const void*, const void*);
int dtcmp_op_fn_uint8t_descend(const void*, const void*);
int dtcmp_op_fn_uint16t_ascend(const void*, const void*);
int dtcmp_op_fn_uint16t_descend(const void*, const void*);
int dtcmp_op_fn_uint32t_ascend(const void*, const void*);
int dtcmp_op_fn_uint32t_descend(const void*, const void*);
int dtcmp_op_fn_uint64t_ascend(const void*, const void*);
int dtcmp_op_fn_uint64t_descend(const void*, const void*);
int dtcmp_op_fn_float_ascend(const void*, const void*);
int dtcmp_op_fn_float_descend(const void*, const void*);
int dtcmp_op_fn_double_ascend(const void*, const void*);
int dtcmp_op_fn_double_descend(const void*, const void*);
int dtcmp_op_fn_longdouble_ascend(const void*, const void*);
int dtcmp_op_fn_longdouble_descend(const void*, const void*);

/* TODO: would like to support variable length data somehow,
 * and strings in particular, but the create function
 * requires that we know the extent, perhaps need a flag
 * to allow for variable length data so that eval function
 * returns extents in addition to flag */
int dtcmp_op_fn_strcmp_ascend(const void*, const void*);
int dtcmp_op_fn_strcmp_descend(const void*, const void*);
/*
int dtcmp_op_fn_strcasecmp_ascend(const void*, const void*);
int dtcmp_op_fn_strcasecmp_descend(const void*, const void*);
*/

#endif /* DTCMP_OPS_H_ */
