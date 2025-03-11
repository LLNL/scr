/* Copyright (c) 2012, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory.
 * Written by Adam Moody <moody20@llnl.gov>.
 * LLNL-CODE-557516.
 * All rights reserved.
 * This file is part of the DTCMP library.
 * For details, see https://github.com/hpc/dtcmp
 * Please also read this file: LICENSE.TXT. */

/* The Datatype Comparison (DTCMP) Library provides methods for sort,
 * search, merge, rank, and select items globally distributed among a
 * communicator of MPI processes using datatype comparison operations.
 * The comparison operations may be predefined or user-defined.
 *
 * Conventions:
 * Function names are often of the form:
 *   DTCMP_<OP><MODIFIER>[_local]
 * where OP specfies the operation like:
 *   "Sort" or "Rank",
 * the MODIFIER may be one of:
 *   v - meaning that different MPI processes may provide different
 *       counts of input elements 
 * the optional _local suffix implies the routine operates only with
 * local data and no MPI communicator is required */

#ifndef DTCMP_H_
#define DTCMP_H_

#include <stdlib.h>
#include <stdint.h>
#include "mpi.h"

/* define a C interface */
#ifdef __cplusplus
extern "C" {
#endif

/* ----------------------------------------------
 * Constants
 * ---------------------------------------------- */

/* API return codes,
 * all user API calls return DTCMP_SUCCESS if successful */
#define DTCMP_SUCCESS (0)
#define DTCMP_FAILURE (1)

/* caller may specify DTCMP_IN_PLACE as input buffer to instruct
 * library to look to output buffer for input data */
extern const void* DTCMP_IN_PLACE;

/* we define a type for our flags just to make the parameter
 * easy to search and replace in case we want to change it,
 * may want to do this via MPI_Info-like object or other
 * functions */
typedef uint32_t DTCMP_Flags;

#define DTCMP_FLAG_NONE   (0x0) /* no hints/assertions provided */
#define DTCMP_FLAG_UNIQUE_LOCAL (0x1)  /* elems known to be locally unique */
#define DTCMP_FLAG_UNIQUE       (0x2)  /* elems known to be globally unique */
#define DTCMP_FLAG_SORTED_LOCAL (0x10) /* elems known to be ordered locally */
#define DTCMP_FLAG_SORTED       (0x20) /* elems known to be ordered globally */
#define DTCMP_FLAG_STABLE (0x100) /* caller requires stable sort */

/* ----------------------------------------------
 * Initialization and Finalization Functions
 * ---------------------------------------------- */

/* initialize the DTCMP library, call after MPI_Init */
int DTCMP_Init(void);

/* shut down the DTCMP library, call before MPI_Finalize */
int DTCMP_Finalize(void);

/* ----------------------------------------------
 * Functions to create types
 * ---------------------------------------------- */

/* concatenates types back-to-back into a single type */
int DTCMP_Type_create_series(
  int num,              /* IN  - number of datatypes in types array (non-negative integer) */
  MPI_Datatype types[], /* IN  - datatypes to combine (array of handles of length num) */
  MPI_Datatype* newtype /* OUT - new datatype (handle) */
);

/* ----------------------------------------------
 * Comparison Operation Handles
 * ---------------------------------------------- */

/* The DTCMP library provides a number of predefined comparison
 * operations, and there are functions to enable applications to
 * construct their own comparison ops. */

/* function prototype for a DTCMP_Op function,
 * compares item at buf1 to item at buf2,
 * must return negative int value if buf1 < bu2,
 * 0 if equal, and positive int value if buf1 > buf2 */
typedef int(*DTCMP_Op_fn)(const void* buf1, const void* buf2);

/* handle to comparison operation */
typedef void* DTCMP_Op;

/* we define a NULL handle */
extern DTCMP_Op DTCMP_OP_NULL;

/* TODO: add more predefined operations */
/* predefined comparison operations */
extern DTCMP_Op DTCMP_OP_SHORT_ASCEND;
extern DTCMP_Op DTCMP_OP_SHORT_DESCEND;
extern DTCMP_Op DTCMP_OP_INT_ASCEND;
extern DTCMP_Op DTCMP_OP_INT_DESCEND;
extern DTCMP_Op DTCMP_OP_LONG_ASCEND;
extern DTCMP_Op DTCMP_OP_LONG_DESCEND;
extern DTCMP_Op DTCMP_OP_LONGLONG_ASCEND;
extern DTCMP_Op DTCMP_OP_LONGLONG_DESCEND;
extern DTCMP_Op DTCMP_OP_UNSIGNEDSHORT_ASCEND;
extern DTCMP_Op DTCMP_OP_UNSIGNEDSHORT_DESCEND;
extern DTCMP_Op DTCMP_OP_UNSIGNEDINT_ASCEND;
extern DTCMP_Op DTCMP_OP_UNSIGNEDINT_DESCEND;
extern DTCMP_Op DTCMP_OP_UNSIGNEDLONG_ASCEND;
extern DTCMP_Op DTCMP_OP_UNSIGNEDLONG_DESCEND;
extern DTCMP_Op DTCMP_OP_UNSIGNEDLONGLONG_ASCEND;
extern DTCMP_Op DTCMP_OP_UNSIGNEDLONGLONG_DESCEND;
extern DTCMP_Op DTCMP_OP_INT8T_ASCEND;
extern DTCMP_Op DTCMP_OP_INT8T_DESCEND;
extern DTCMP_Op DTCMP_OP_INT16T_ASCEND;
extern DTCMP_Op DTCMP_OP_INT16T_DESCEND;
extern DTCMP_Op DTCMP_OP_INT32T_ASCEND;
extern DTCMP_Op DTCMP_OP_INT32T_DESCEND;
extern DTCMP_Op DTCMP_OP_INT64T_ASCEND;
extern DTCMP_Op DTCMP_OP_INT64T_DESCEND;
extern DTCMP_Op DTCMP_OP_UINT8T_ASCEND;
extern DTCMP_Op DTCMP_OP_UINT8T_DESCEND;
extern DTCMP_Op DTCMP_OP_UINT16T_ASCEND;
extern DTCMP_Op DTCMP_OP_UINT16T_DESCEND;
extern DTCMP_Op DTCMP_OP_UINT32T_ASCEND;
extern DTCMP_Op DTCMP_OP_UINT32T_DESCEND;
extern DTCMP_Op DTCMP_OP_UINT64T_ASCEND;
extern DTCMP_Op DTCMP_OP_UINT64T_DESCEND;
extern DTCMP_Op DTCMP_OP_FLOAT_ASCEND;
extern DTCMP_Op DTCMP_OP_FLOAT_DESCEND;
extern DTCMP_Op DTCMP_OP_DOUBLE_ASCEND;
extern DTCMP_Op DTCMP_OP_DOUBLE_DESCEND;
extern DTCMP_Op DTCMP_OP_LONGDOUBLE_ASCEND;
extern DTCMP_Op DTCMP_OP_LONGDOUBLE_DESCEND;

/* make a full copy of a comparison operation */
int DTCMP_Op_dup(
  DTCMP_Op cmp,     /* IN  - comparison op to be copied (handle) */
  DTCMP_Op* newcmp  /* OUT - comparison operation (handle) */
);

/* create a user-defined comparison operation,
 * associate datatype of key and compare function with handle */
int DTCMP_Op_create(
  MPI_Datatype key, /* IN  - datatype of items being compared (handle) */
  DTCMP_Op_fn fn,   /* IN  - function to compare two items */
  DTCMP_Op* cmp     /* OUT - comparison operation (handle) */
);

/* create a series comparison which executes the first comparison
 * operation and then the second if the first evaluates to equal,
 * second key is assumed to be located extent(first) bytes from first */
int DTCMP_Op_create_series2(
  DTCMP_Op first,   /* IN  - first cmp operation (handle) */
  DTCMP_Op second,  /* IN  - second cmp operation if first is
                     *       equal (handle) */
  DTCMP_Op* cmp     /* OUT - comparison operation (handle) */
);

/* create a series comparison which executes the first comparison
 * operation and then the second if the first evaluates to equal,
 * and so on down the chain, each key is assumed to be located
 * immediately following the key before it (uses extent of key
 * datatype to determine length of each key) */
int DTCMP_Op_create_series(
  int num,      /* IN  - number of items in series array
                 *       (non-negative integer) */
  const DTCMP_Op series[],
                /* IN  - comparison operations in series
                 *       (array of handles of length num) */
  DTCMP_Op* cmp /* OUT - comparison operation (handle) */
);

/* create a series comparison which executes the first comparison
 * operation and then the second if the first evaluates to equal,
 * explicit byte displacement is given to go from first to second key */
int DTCMP_Op_create_hseries2(
  DTCMP_Op first,   /* IN  - first cmp operation (handle) */
  MPI_Aint cmpdisp, /* IN  - byte displacement to advance pointer to
                     *       start of first item (integer) */
  MPI_Aint disp,    /* IN  - byte displacement to advance pointer to
                     *       start of second item (integer) */
  DTCMP_Op second,  /* IN  - second cmp operation if first is
                     *       equal (handle) */
  DTCMP_Op* cmp     /* OUT - comparison operation (handle) */
);

/* create a series comparison which executes the first comparison
 * operation and then the second if the first evaluates to equal,
 * explicit byte displacement is given to go from first to second key */
int DTCMP_Op_create_hseries(
  int num,      /* IN  - number of items in series array
                 *       (non-negative integer) */
  const DTCMP_Op series[],
                /* IN  - comparison operations in series
                 *       (array of handles of length num) */
  const MPI_Aint cmpdisp[],
                /* IN  - byte displacement to advance pointer to
                 *       start of first item (array of integer of
                 *       length num) */
  const MPI_Aint disp[],
                /* IN  - byte displacement to advance pointer to
                 *       start of second item (array of integer of
                 *       length num) */
  DTCMP_Op* cmp /* OUT - comparison operation (handle) */
);

/* free object referenced by comparison operation handle,
 * sets cmp to DTCMP_OP_NULL upon return */
int DTCMP_Op_free(
  DTCMP_Op* cmp     /* INOUT - comparison function (handle) */
);

/* compares a to b using cmp op and sets flag to
 *    1 if a > b
 *    0 if a = b
 *   -1 if a < b */
int DTCMP_Op_eval(
  const void* a,    /* IN  - buffer holding item a */
  const void* b,    /* IN  - buffer holding item b */
  DTCMP_Op cmp,     /* IN  - comparison function (handle) */
  int* flag
);

/* ----------------------------------------------
 * String type and op creation
 * ---------------------------------------------- */

/* DTCMP requires fixed length fields, but sorting strings
 * is a common requirement.  This function is a convenience
 * to simplify creating a fixed-length string type and
 * associated comparison operation.  The caller specifies
 * the string length, and the function returns a type and op,
 * which the caller is responsible for freeing with MPI_Type_free
 * and DTCMP_Op_free. The comparison operation uses strcmp(). */
int DTCMP_Str_create_ascend(
  int chars,          /* IN  - number of chars in string */
  MPI_Datatype* type, /* OUT - committed MPI_Type_contiguous */
  DTCMP_Op* cmp       /* OUT - comparison operation */
);

/* same as above but comparison op sorts in reverse order */
int DTCMP_Str_create_descend(
  int chars,          /* IN  - number of chars in string */
  MPI_Datatype* type, /* OUT - committed MPI_Type_contiguous */
  DTCMP_Op* cmp       /* OUT - comparison operation */
);

/* ----------------------------------------------
 * Resource handles
 * ---------------------------------------------- */

/* handle to an object that tracks internal resources allocated during
 * a DTCMP call, active handles must be freed with call to DTCMP_Free
 * to free internal resources, but only after the application is done
 * with the received data the handle refers to */
typedef void* DTCMP_Handle;

/* we define a NULL handle as DTCMP_HANDLE_NULL */
extern DTCMP_Handle DTCMP_HANDLE_NULL;

/* frees resources internally allocated in call to DTCMP_Sortz,
 * sets handle to DTCMP_HANDLE_NULL */
int DTCMP_Free(
  DTCMP_Handle* handle /* INOUT - DTCMP resource (handle) */
);

/* ----------------------------------------------
 * Utility functions
 * ---------------------------------------------- */

/* Copies data from src buffer to dst buffer on the same process using
 * MPI datatypes.  The number of basic elements specified by dstcount
 * and dsttype must be equal to the number of elements specified by
 * srccount and srctype, and both dsttype and srctype must be
 * committed. */
int DTCMP_Memcpy(
  void*        dstbuf,    /* OUT - buffer to copy data to */
  int          dstcount,  /* IN  - number of elements of type dsttype
                           *       to be stored to dstbuf */
  MPI_Datatype dsttype,   /* IN  - datatype of elements in dstbuf */
  const void*  srcbuf,    /* IN  - source buffer to copy data from */ 
  int          srccount,  /* IN  - number of elements of type srctype
                           *       to be copied from srcbuf */
  MPI_Datatype srctype    /* IN  - datatype of elements in srcbuf */
);

/* ----------------------------------------------
 * Search functions
 * ---------------------------------------------- */

/* searches for target within specified range of ordered list,
 * sets flag=1 if target is found, 0 otherwise,
 * and sets index to position within range *at* which target could
 * be inserted such that list is still in order and target would
 * be the first of any duplicates */
int DTCMP_Search_low_local(
  const void* target,  /* IN  - buffer holding target key */
  const void* list,    /* IN  - buffer holding ordered list of
                        *       key/satellite items to search */
  int low,             /* IN  - lowest index to consider */
  int high,            /* IN  - highest index to consider */
  MPI_Datatype key,    /* IN  - datatype of key (handle) */
  MPI_Datatype keysat, /* IN  - datatype of key and satellite (handle) */
  DTCMP_Op cmp,        /* IN  - key comparison function (handle) */
  DTCMP_Flags hints,   /* IN  - hints/assertions (bit flags) */
  int* flag,           /* OUT - set to 1 if target is in list,
                        *       set to 0 otherwise (integer) */
  int* index           /* OUT - lowest index in list where target could
                        *       be inserted (integer) */
);

/* searches for target within specified range of ordered list,
 * sets flag = 1 if target is found, 0 otherwise,
 * and sets index to position within range *after* which target could
 * be inserted such that list is still in order and target would
 * be the last of any duplicates */
int DTCMP_Search_high_local(
  const void* target,  /* IN  - buffer holding target key */
  const void* list,    /* IN  - buffer holding ordered list of
                        *       key/satellite items to search */
  int low,             /* IN  - lowest index to consider */
  int high,            /* IN  - highest index to consider */
  MPI_Datatype key,    /* IN  - datatype of key (handle) */
  MPI_Datatype keysat, /* IN  - datatype of key and satellite (handle) */
  DTCMP_Op cmp,        /* IN  - key comparison function (handle) */
  DTCMP_Flags hints,   /* IN  - hints/assertions (bit flags) */
  int* flag,           /* OUT - set to 1 if target is in list,
                        *       set to 0 otherwise (integer) */
  int* index           /* OUT - highest index in list where target
                        *       could be inserted (integer) */
);

/* given an ordered list of targets, internally calls DTCMP_Search_low
 * and returns index corresponding to each target */
int DTCMP_Search_low_list_local(
  int num,             /* IN  - number of targets (integer) */
  const void* targets, /* IN  - array holding target key */
  const void* list,    /* IN  - array holding ordered list of
                        *       key/satellite items to search */
  int low,             /* IN  - number of items in list (non-negative
                        *       integer) */
  int high,            /* IN  - number of items in list (non-negative
                        *       integer) */
  MPI_Datatype key,    /* IN  - datatype of key (handle) */
  MPI_Datatype keysat, /* IN  - datatype of key and satellite (handle) */
  DTCMP_Op cmp,        /* IN  - key comparison function (handle) */
  DTCMP_Flags hints,   /* IN  - hints/assertions (bit flags) */
  int flags[],         /* OUT - flag set to 1 if corresponding target
                        *       is found, 0 otherwise (integer array
                        *       of length num) */
  int indicies[]       /* OUT - lowest index in list where target could
                        *       be inserted (integer array of length num) */
);

/* ----------------------------------------------
 * Partition functions
 * ---------------------------------------------- */

/* given an index to a pivot value, partition values in buf to either
 * side of pivot, less than will be before pivot, greater than will be
 * after pivot, and equal values are evenly scattered to either side */
int DTCMP_Partition_local(
  void* buf,           /* INOUT - buffer holding elements to be
                        *         partitioned */
  int count,           /* IN    - number of input items on the calling
                        *         process (non-negative integer) */
  int inpivot,         /* IN    - index of pivot value within input
                        *         values (non-negative integer) */
  int* outpivot,       /* OUT   - position of pivot element after
                        *         partitioning (non-negative integer) */
  MPI_Datatype key,    /* IN    - datatype of key (handle) */
  MPI_Datatype keysat, /* IN    - datatype of key and satellite (handle) */
  DTCMP_Op cmp,        /* IN    - key comparison function (handle) */
  DTCMP_Flags hints    /* IN  - hints/assertions (bit flags) */
);

/* TODO: add partition_list functions like search list */

/* TODO: add partition_target functions like search list */

#if 0
/* TODO: change comm to lwgrp, since common use will be to recursively
 * divide comm into smaller groups as part of sort */

/* Given a list of items on each process, globally partition items at
 * specified item rank, and send smaller items to lower half of ranks
 * and larger items to upper half of ranks.  Item rank should be in
 * range of [1,sum(count)].  Lower items go to [0,dividerank-1] and
 * higher items go to [dividerank,ranks-1].  Evenly divides items among
 * lower and upper ranges of ranks as best as possible, and returns
 * partitioned items in newly allocate memory (outbuf, outcount, handle) */
int DTCMP_Partitionz(
  const void* buf,     /* IN  - buffer holding elements to be
                        *       partitioned */
  int count,           /* IN  - number of input items on the calling
                        *       process (non-negative integer) */
  uint64_t k,          /* IN  - rank of item to identify in range from
                        *       0 to sum(count)-1 inclusive */
  int dividerank,      /* IN  - lowest MPI rank to receive elements
                        *       ranked at k or higher */
  void** outbuf,       /* OUT - start of buffer to hold output
                        *       key/satellite items after partition */
  int* outcount,       /* OUT - number of items in output buffer
                        *       (non-negative integer) */
  MPI_Datatype key,    /* IN  - datatype of key (handle) */
  MPI_Datatype keysat, /* IN  - datatype of key and satellite (handle) */
  DTCMP_Op cmp,        /* IN  - key comparison function (handle) */
  DTCMP_Flags hints    /* IN  - hints/assertions (bit flags) */
  MPI_Comm comm,       /* IN  - communicator on which to execute
                        *       partition (handle) */
  DTCMP_Handle* handle /* OUT - handle to resources associated with
                        *       outbuf (handle) */
);
#endif

/* ----------------------------------------------
 * Merge functions
 * ---------------------------------------------- */

/* merge num ordered lists pointed to by inbufs into
 * a single ordered list in outbuf */
int DTCMP_Merge_local(
  int num,              /* IN  - number of input buffers (non-negative
                         *       integer) */
  const void* inbufs[], /* IN  - start of each input buffer (array of
                         *       length num) */ 
  int counts[],         /* IN  - number of items in each buffer (array
                         *       of non-negative integers of length num) */
  void* outbuf,         /* OUT - output buffer (large enough to hold
                         *       sum of counts) */
  MPI_Datatype key,     /* IN  - datatype of key (handle) */
  MPI_Datatype keysat,  /* IN  - datatype of key and satellite (handle) */
  DTCMP_Op cmp,         /* IN  - key comparison function (handle) */
  DTCMP_Flags hints    /* IN  - hints/assertions (bit flags) */
);

/* ----------------------------------------------
 * Selection functions
 * ---------------------------------------------- */

/* given an array of items and a rank k, identify and return the
 * location of the kth largest item in the array */
int DTCMP_Select_local(
  const void* buf,     /* IN  - buffer holding elements in which kth
                        *       item is to be identified */
  int count,           /* IN  - number of input items on the calling
                        *       process (non-negative integer) */
  int k,               /* IN  - rank of item to identify in range from
                        *       0 to (count-1) inclusive */
  void* item,          /* OUT - buffer to hold copy of kth largest key */
  MPI_Datatype key,    /* IN  - datatype of key (handle) */
  MPI_Datatype keysat, /* IN  - datatype of key and satellite (handle) */
  DTCMP_Op cmp,        /* IN  - key comparison function (handle) */
  DTCMP_Flags hints    /* IN  - hints/assertions (bit flags) */
);

/* given an array of items and a rank k, identify and return the
 * location of the kth largest item from all procs */
int DTCMP_Selectv(
  const void* buf,     /* IN  - buffer holding elements in which kth
                        *       item is to be identified */
  int count,           /* IN  - number of input items on the calling
                        *       process (non-negative integer) */
  uint64_t k,          /* IN  - rank of item to identify in range from
                        *       0 to sum(count)-1 inclusive */
  void* item,          /* OUT - buffer to hold copy of kth largest key */
  MPI_Datatype key,    /* IN  - datatype of key (handle) */
  MPI_Datatype keysat, /* IN  - datatype of key and satellite (handle) */
  DTCMP_Op cmp,        /* IN  - key comparison function (handle) */
  DTCMP_Flags hints,   /* IN  - hints/assertions (bit flags) */
  MPI_Comm comm        /* IN  - communicator on which to execute
                        *       selection (handle) */
);

/* ----------------------------------------------
 * Sort functions
 * ---------------------------------------------- */

/* TODO: could implement as Sort with comm == MPI_COMM_SELF */
/* sort a local set of elements */
int DTCMP_Sort_local(
  const void* inbuf,   /* IN  - start of buffer containing input
                        *       key/satellite items */
  void* outbuf,        /* OUT - start of buffer to hold output
                        *       key/satellite items after sort */
  int count,           /* IN  - number of input items on the calling
                        *       process (non-negative integer) */
  MPI_Datatype key,    /* IN  - datatype of key (handle) */
  MPI_Datatype keysat, /* IN  - datatype of key and satellite (handle) */
  DTCMP_Op cmp,        /* IN  - key comparison operation (handle) */
  DTCMP_Flags hints    /* IN  - hints/assertions (bit flags) */
);

/* all processes contribute the same number of elements to the sort,
 * and after sorting, ith process receives elements (i-1)*count to
 * i*count */
int DTCMP_Sort(
  const void* inbuf,   /* IN  - start of buffer containing input
                        *       key/satellite items */
  void* outbuf,        /* OUT - start of buffer to hold output
                        *       key/satellite items after sort */
  int count,           /* IN  - number of input items on the calling
                        *       process (non-negative integer) */
  MPI_Datatype key,    /* IN  - datatype of key (handle) */
  MPI_Datatype keysat, /* IN  - datatype of key and satellite (handle) */
  DTCMP_Op cmp,        /* IN  - key comparison operation (handle) */
  DTCMP_Flags hints,   /* IN  - hints/assertions (bit flags) */
  MPI_Comm comm        /* IN  - communicator on which to execute sort
                        *       (handle) */
);

/* each process may specify zero or more elements in inbuf to be sorted
 * given by count, and after sorting, ith process receives elements
 * SUM(count_(1)..count(i-1)) to SUM(count(1)..count(i)) */
int DTCMP_Sortv(
  const void* inbuf,   /* IN  - start of buffer containing input
                        *       key/satellite items */
  void* outbuf,        /* OUT - start of buffer to hold output
                        *       key/satellite items after sort */
  int count,           /* IN  - number of input items on the calling
                        *       process (non-negative integer) */
  MPI_Datatype key,    /* IN  - datatype of key (handle) */
  MPI_Datatype keysat, /* IN  - datatype of key and satellite (handle) */
  DTCMP_Op cmp,        /* IN  - key comparison operation (handle) */
  DTCMP_Flags hints,   /* IN  - hints/assertions (bit flags) */
  MPI_Comm comm        /* IN  - communicator on which to execute sort
                        *       (handle) */
);

/* each process may specify zero or more elements in inbuf to be sorted
 * give by count, and function allocates and returns memory to hold
 * output data, fills outbuf with pointer to this buffer, fills
 * outcount with the number of elements in outbuf, and returns a handle
 * which refers to the memory associated with outbuf, this handle must
 * be freed with a call to DTCMP_Free when the called is done accessing
 * outbuf */
int DTCMP_Sortz(
  const void* inbuf,   /* IN  - start of buffer containing input
                        *       key/satellite items */
  int count,           /* IN  - number of input items on the calling
                        *       process (non-negative integer) */
  void** outbuf,       /* OUT - start of buffer to hold output
                        *       key/satellite items after sort */
  int* outcount,       /* OUT - number of items in output buffer
                        *       (non-negative integer) */
  MPI_Datatype key,    /* IN  - datatype of key (handle) */
  MPI_Datatype keysat, /* IN  - datatype of key and satellite (handle) */
  DTCMP_Op cmp,        /* IN  - key comparison operation (handle) */
  DTCMP_Flags hints,   /* IN  - hints/assertions (bit flags) */
  MPI_Comm comm,       /* IN  - communicator on which to execute sort
                        *       (handle) */
  DTCMP_Handle* handle /* OUT - handle to resources associated with
                        *       outbuf (handle) */
);

/* check whether all items in buf are already in sorted order */
int DTCMP_Is_sorted(
  const void* buf,     /* IN  - start of buffer containing input
                        *       key/satellite items */
  int count,           /* IN  - number of input items on the calling
                        *       process (non-negative integer) */
  MPI_Datatype key,    /* IN  - datatype of key (handle) */
  MPI_Datatype keysat, /* IN  - datatype of key and satellite (handle) */
  DTCMP_Op cmp,        /* IN  - key comparison operation (handle) */
  DTCMP_Flags hints,   /* IN  - hints/assertions (bit flags) */
  MPI_Comm comm,       /* IN  - communicator on which to execute sort
                        *       (handle) */
  int* flag            /* OUT - flag is set to 1 if items are in sorted
                        *       order, 0 otherwise (integer) */
);

/* ----------------------------------------------
 * Rank functions
 * ---------------------------------------------- */

/* assigns globally unique rank ids to items in buf,
 * returns number of globally distinct items in groups,
 * and sets group_id, group_ranks, group_rank for each item in buf,
 * group_id[i] is set from 0 to groups-1 and specifies to which group
 * the ith item (in buf) belongs, group_ranks[i] returns the global
 * number of items in that group, group_rank[i] is set from 0 to
 * group_ranks[i]-1 and it specifies the item's rank within its group,
 * with any ties broken first by MPI rank and then by the item's index
 * within buf */
int DTCMP_Rank_local(
  int count,              /* IN  - number of input items on the calling
                           *       process (non-negative integer) */
  const void* buf,        /* IN  - start of buffer containing input
                           *       key/satellite items */
  uint64_t* groups,       /* OUT - number of distinct items
                           *       (non-negative integer) */
  uint64_t group_id[],    /* OUT - group identifier corresponding to
                           *       each input item (array of
                           *       non-negative integer of length count) */
  uint64_t group_ranks[], /* OUT - number of items within group of
                           *       each input item (array of
                           *       non-negative integers of length count) */
  uint64_t group_rank[],  /* OUT - rank of each input item within its
                           *       group (array of non-negative
                           *       integers of length count) */
  MPI_Datatype key,       /* IN  - datatype of key (handle) */
  MPI_Datatype keysat,    /* IN  - datatype of key and satellite (handle) */
  DTCMP_Op cmp,           /* IN  - key comparison operation (handle) */
  DTCMP_Flags hints       /* IN  - hints/assertions (bit flags) */
);

/* conveniece function to rank variable length, NUL-terminated C
 * strings */
int DTCMP_Rank_strings_local(
  int count,              /* IN  - number of strings on calling
                           *       process (non-negative integer) */
  const char* strings[],  /* IN  - array of pointers to each string
                           *       (array of length count) */
  uint64_t* groups,       /* OUT - number of distinct strings
                           *       (non-negative integer) */
  uint64_t group_id[],    /* OUT - group identifier corresponding to
                           *       each input item (array of
                           *       non-negative integer of length count) */
  uint64_t group_ranks[], /* OUT - number of items within group of
                           *       each input item (array of
                           *       non-negative integers of length count) */
  uint64_t group_rank[],  /* OUT - rank of each input item within its
                           *       group (array of non-negative
                           *       integers of length count) */
  DTCMP_Flags hints       /* IN  - hints/assertions (bit flags) */
);

int DTCMP_Rank(
  int count,              /* IN  - number of input items on the calling
                           *       process (non-negative integer) */
  const void* buf,        /* IN  - start of buffer containing input
                           *       key/satellite items */
  uint64_t* groups,       /* OUT - number of distinct items
                           *       (non-negative integer) */
  uint64_t  group_id[],   /* OUT - group identifier corresponding to
                           *       each input item (array of
                           *       non-negative integer of length count) */
  uint64_t  group_ranks[],/* OUT - number of items within group of
                           *       each input item (array of
                           *       non-negative integers of length count) */
  uint64_t  group_rank[], /* OUT - rank of each input item within its
                           *       group (array of non-negative
                           *       integers of length count) */
  MPI_Datatype key,       /* IN  - datatype of key (handle) */
  MPI_Datatype keysat,    /* IN  - datatype of key and satellite (handle) */
  DTCMP_Op cmp,           /* IN  - key comparison operation (handle) */
  DTCMP_Flags hints,      /* IN  - hints/assertions (bit flags) */
  MPI_Comm comm           /* IN  - communicator on which to execute
                           *       rank (handle) */
);

/* conveniece function to rank variable length, NUL-terminated C
 * strings */
int DTCMP_Rank_strings(
  int count,              /* IN  - number of strings on calling
                           *       process (non-negative integer) */
  const char* strings[],  /* IN  - array of pointers to each string
                           *       (array of length count) */
  uint64_t* groups,       /* OUT - number of distinct strings
                           *       (non-negative integer) */
  uint64_t group_id[],    /* OUT - group identifier corresponding to
                           *       each input item (array of
                           *       non-negative integer of length count) */
  uint64_t group_ranks[], /* OUT - number of items within group of
                           *       each input item (array of
                           *       non-negative integers of length count) */
  uint64_t group_rank[],  /* OUT - rank of each input item within its
                           *       group (array of non-negative
                           *       integers of length count) */
  DTCMP_Flags hints,      /* IN  - hints/assertions (bit flags) */
  MPI_Comm comm           /* IN  - communicator on which to execute
                           *       rank (handle) */
);

/* like DTCMP_Rank but each process can contribute different number of
 * elements */
int DTCMP_Rankv(
  int count,              /* IN  - number of input items on the calling
                           *       process (non-negative integer) */
  const void* buf,        /* IN  - start of buffer containing input
                           *       key/satellite items */
  uint64_t* groups,       /* OUT - number of distinct items
                           *       (non-negative integer) */
  uint64_t group_id[],    /* OUT - group identifier corresponding to
                           *       each input item (array of
                           *       non-negative integer of length count) */
  uint64_t group_ranks[], /* OUT - number of items within group of
                           *       each input item (array of
                           *       non-negative integers of length count) */
  uint64_t group_rank[],  /* OUT - rank of each input item within its
                           *       group (array of non-negative
                           *       integers of length count) */
  MPI_Datatype key,       /* IN  - datatype of key (handle) */
  MPI_Datatype keysat,    /* IN  - datatype of key and satellite (handle) */
  DTCMP_Op cmp,           /* IN  - key comparison operation (handle) */
  DTCMP_Flags hints,      /* IN  - hints/assertions (bit flags) */
  MPI_Comm comm           /* IN  - communicator on which to execute
                           *       rank (handle) */
);

/* conveniece function to rank variable length, NUL-terminated C
 * strings */
int DTCMP_Rankv_strings(
  int count,              /* IN  - number of strings on calling
                           *       process (non-negative integer) */
  const char* strings[],  /* IN  - array of pointers to each string
                           *       (array of length count) */
  uint64_t* groups,       /* OUT - number of distinct strings
                           *       (non-negative integer) */
  uint64_t group_id[],    /* OUT - group identifier corresponding to
                           *       each input item (array of
                           *       non-negative integer of length count) */
  uint64_t group_ranks[], /* OUT - number of items within group of
                           *       each input item (array of
                           *       non-negative integers of length count) */
  uint64_t group_rank[],  /* OUT - rank of each input item within its
                           *       group (array of non-negative
                           *       integers of length count) */
  DTCMP_Flags hints,      /* IN  - hints/assertions (bit flags) */
  MPI_Comm comm           /* IN  - communicator on which to execute
                           *       rank (handle) */
);

/* ----------------------------------------------
 * Segmented scan
 * ---------------------------------------------- */

/* Executes left-to-right and right-to-left segmented exclusive scans,
 * applying the MPI reduction operator to the input values across
 * contiguous segments whose keys are equal.  The result of the
 * left-to-right scan is output in ltrbuf, and the result of the
 * right-to-left scan is output in rtlbuf.  Output buffer elements
 * corresponding to the start of a segment are left unmodified. */
int DTCMP_Segmented_exscanv(
  int count,           /* IN  - number of input items on the calling
                        *       process (non-negative integer) */
  const void* keybuf,  /* IN  - start of buffer containing keys */
  MPI_Datatype key,    /* IN  - datatype of key (handle) */
  DTCMP_Op cmp,        /* IN  - key comparison operation (handle) */
  const void* valbuf,  /* IN  - start of buffer containing values */
  void* ltrbuf,        /* OUT - start of buffer for left-to-right result */
  void* rtlbuf,        /* OUT - start of buffer for right-to-left result */
  MPI_Datatype val,    /* IN  - datatype of value (handle) */
  MPI_Op op,           /* IN  - MPI reduction operator (handle) */
  DTCMP_Flags hints,   /* IN  - hints/assertions (bit flags) */
  MPI_Comm comm        /* IN  - communicator on which to execute scan (handle) */
);

/* Executes left-to-right and right-to-left segmented inclusive scans,
 * applying the MPI reduction operator to the input values across
 * contiguous segments whose keys are equal.  The result of the
 * left-to-right scan is output in ltrbuf, and the result of the
 * right-to-left scan is output in rtlbuf.  Output buffer elements
 * corresponding to the start of a segment are initialized with
 * value from input buffer. */
int DTCMP_Segmented_scanv(
  int count,           /* IN  - number of input items on the calling
                        *       process (non-negative integer) */
  const void* keybuf,  /* IN  - start of buffer containing keys */
  MPI_Datatype key,    /* IN  - datatype of key (handle) */
  DTCMP_Op cmp,        /* IN  - key comparison operation (handle) */
  const void* valbuf,  /* IN  - start of buffer containing values */
  void* ltrbuf,        /* OUT - start of buffer for left-to-right result */
  void* rtlbuf,        /* OUT - start of buffer for right-to-left result */
  MPI_Datatype val,    /* IN  - datatype of value (handle) */
  MPI_Op op,           /* IN  - MPI reduction operator (handle) */
  DTCMP_Flags hints,   /* IN  - hints/assertions (bit flags) */
  MPI_Comm comm        /* IN  - communicator on which to execute scan (handle) */
);

/* Executes left-to-right segmented exclusive scan,
 * applying the MPI reduction operator to the input values across
 * contiguous segments whose keys are equal.  The result of the
 * scan is output in outbuf.  Output buffer elements
 * corresponding to the start of a segment are left unmodified. */
int DTCMP_Segmented_exscanv_ltr(
  int count,           /* IN  - number of input items on the calling
                        *       process (non-negative integer) */
  const void* keybuf,  /* IN  - start of buffer containing keys */
  MPI_Datatype key,    /* IN  - datatype of key (handle) */
  DTCMP_Op cmp,        /* IN  - key comparison operation (handle) */
  const void* valbuf,  /* IN  - start of buffer containing values */
  void* outbuf,        /* OUT - start of buffer for left-to-right result */
  MPI_Datatype val,    /* IN  - datatype of value (handle) */
  MPI_Op op,           /* IN  - MPI reduction operator (handle) */
  DTCMP_Flags hints,   /* IN  - hints/assertions (bit flags) */
  MPI_Comm comm        /* IN  - communicator on which to execute scan (handle) */
);

/* Executes left-to-right segmented inclusive scan,
 * applying the MPI reduction operator to the input values across
 * contiguous segments whose keys are equal.  The result of the
 * scan is output in outbuf,  Output buffer elements
 * corresponding to the start of a segment are initialized with
 * value from input buffer. */
int DTCMP_Segmented_scanv_ltr(
  int count,           /* IN  - number of input items on the calling
                        *       process (non-negative integer) */
  const void* keybuf,  /* IN  - start of buffer containing keys */
  MPI_Datatype key,    /* IN  - datatype of key (handle) */
  DTCMP_Op cmp,        /* IN  - key comparison operation (handle) */
  const void* valbuf,  /* IN  - start of buffer containing values */
  void* outbuf,        /* OUT - start of buffer for left-to-right result */
  MPI_Datatype val,    /* IN  - datatype of value (handle) */
  MPI_Op op,           /* IN  - MPI reduction operator (handle) */
  DTCMP_Flags hints,   /* IN  - hints/assertions (bit flags) */
  MPI_Comm comm        /* IN  - communicator on which to execute scan (handle) */
);

/* Executes a segmented exclusive scan on items in buf.
 * Executes specified MPI operation on value data
 * for items whose keys are equal.  Input values are assumed
 * to start at the first byte of the satellite data.  Stores result
 * in left-to-right scan in ltrbuf and result of right-to-left
 * scan in rtlbuf.  Items must be in sorted order. */
int DTCMP_Segmented_exscanv_fused(
  int count,           /* IN  - number of input items on the calling
                        *       process (non-negative integer) */
  const void* buf,     /* IN  - start of buffer containing keys and values */
  MPI_Datatype key,    /* IN  - datatype of key (handle) */
  MPI_Datatype keysat, /* IN  - datatype of key and satellite (handle) */
  DTCMP_Op cmp,        /* IN  - key comparison operation (handle) */
  void* ltrbuf,        /* OUT - start of buffer for left-to-right result */
  void* rtlbuf,        /* OUT - start of buffer for right-to-left result */
  MPI_Datatype val,    /* IN  - datatype of value (handle) */
  MPI_Op op,           /* IN  - MPI reduction operator (handle) */
  DTCMP_Flags hints,   /* IN  - hints/assertions (bit flags) */
  MPI_Comm comm        /* IN  - communicator on which to execute scan (handle) */
);

/* Executes a segmented inclusive scan on items in buf.
 * Executes specified MPI operation on value data
 * for items whose keys are equal.  Input values are assumed
 * to start at the first byte of the satellite data.  Stores result
 * in left-to-right scan in ltrbuf and result of right-to-left
 * scan in rtlbuf.  Items must be in sorted order. */
int DTCMP_Segmented_scanv_fused(
  int count,           /* IN  - number of input items on the calling
                        *       process (non-negative integer) */
  const void* buf,     /* IN  - start of buffer containing keys and values */
  MPI_Datatype key,    /* IN  - datatype of key (handle) */
  MPI_Datatype keysat, /* IN  - datatype of key and satellite (handle) */
  DTCMP_Op cmp,        /* IN  - key comparison operation (handle) */
  void* ltrbuf,        /* OUT - start of buffer for left-to-right result */
  void* rtlbuf,        /* OUT - start of buffer for right-to-left result */
  MPI_Datatype val,    /* IN  - datatype of value (handle) */
  MPI_Op op,           /* IN  - MPI reduction operator (handle) */
  DTCMP_Flags hints,   /* IN  - hints/assertions (bit flags) */
  MPI_Comm comm        /* IN  - communicator on which to execute scan (handle) */
);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* DTCMP_H_ */
