/* Copyright (c) 2012, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory.
 * Written by Adam Moody <moody20@llnl.gov>.
 * LLNL-CODE-557516.
 * All rights reserved.
 * This file is part of the DTCMP library.
 * For details, see https://github.com/hpc/dtcmp
 * Please also read this file: LICENSE.TXT. */

#ifndef DTCMP_INTERNAL_H_
#define DTCMP_INTERNAL_H_

#include <stdlib.h>
#include <stdint.h>
#include "mpi.h"
#include "dtcmp.h"
#include "dtcmp_ops.h"
#include "lwgrp.h"

/* ---------------------------------------
 * Internal types
 * --------------------------------------- */

/* we allocate one of these structs and return a pointer to it as the
 * value for our DTCMP_Op operation handle */
typedef struct {
  uint32_t magic;   /* special integer value which we can use to verify
                     * that handle appears valid */
  dtcmp_op_types type; /* type of DTCMP handle, currently only used to
                        * know whether we can call qsort */
  MPI_Datatype key; /* datatype of items being compared */
  DTCMP_Op_fn fn;   /* comparison function pointer */
  MPI_Aint cmpdisp; /* byte displacement from current pointer to first
                     * byte to be passed to comparison op */
  MPI_Aint disp;    /* byte displacement from current pointer to start
                     * of next type */
  DTCMP_Op series;  /* second comparison handle to be applied if first
                     * evaluates to equal */
} dtcmp_op_handle_t;

/* ---------------------------------------
 * Globals
 * --------------------------------------- */

/* dup of MPI_COMM_SELF that we need for DTCMP_Memcpy */
extern MPI_Comm dtcmp_comm_self;

/* we create a type of 3 consecutive uint64_t for computing min/max/sum
 * reduction */
extern MPI_Datatype dtcmp_type_3uint64t;

/* op for computing min/max/sum reduction */
extern MPI_Op dtcmp_reduceop_mms_3uint64t;

/* we create a type of 3 consecutive int for computing max rand/count/rank reduction */
extern MPI_Datatype dtcmp_type_3int;

/* op for computing max rand/count/rank reduction */
extern MPI_Op dtcmp_reduceop_randroot;

/* we call rand_r() to acquire random numbers,
 * and this keeps track of the seed between calls */
extern unsigned dtcmp_rand_seed;

/* ---------------------------------------
 * Utility functions
 * --------------------------------------- */

void* dtcmp_malloc(size_t size, size_t alignment, const char* file, int line);

void dtcmp_free(void*);

/* function pointer to a DTCMP_Free implementation that takes a pointer
 * to a handle */
typedef int(*dtcmp_handle_free_fn)(DTCMP_Handle*);

/* allocate a handle object of specified size and set it up to be freed
 * with dtcmp_handle_free_single, return pointer to start of buffer and
 * set handle value */
int dtcmp_handle_alloc_single(size_t size, void** buf, DTCMP_Handle* handle);

/* assumes that handle just points to one big block of memory that must
 * be freed */
int dtcmp_handle_free_single(DTCMP_Handle* handle);

/* user-defined reduction operation to compute min/max/sum */
#define MMS_MIN (0)
#define MMS_MAX (1)
#define MMS_SUM (2)
int dtcmp_get_uint64t_min_max_sum(int count, uint64_t* min, uint64_t* max, uint64_t* sum, MPI_Comm comm);

/* user-defined reduction to randomly select a root */
#define RANDROOT_COUNT (0)
#define RANDROOT_RAND  (1)
#define RANDROOT_RANK  (2)
int dtcmp_get_randroot(int count, int* flag, int* root, MPI_Comm comm);

/* randomly choose element from all tasks as bcast element,
 * a weight of 0 disqualifies a process from contributing,
 * flag is set to 1 if output buffer is valid, 0 otherwise */
int dtcmp_randbcast(
  const void* inbuf,
  int weight,
  void* outbuf,
  int* flag,
  int count,
  MPI_Datatype type,
  MPI_Comm comm
);

int dtcmp_get_lt_eq_gt(
  const void* target,
  const void* buf,
  int count,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints,
  uint64_t* lt,
  uint64_t* eq,
  uint64_t* gt,
  MPI_Comm comm
);

/* builds and commits a new datatype that is the concatenation of the
 * list of old types, each oldtype should have no holes */
int dtcmp_type_concat(int num, const MPI_Datatype oldtypes[], MPI_Datatype* newtype);

/* same as above but a shortcut when using just two input types */
int dtcmp_type_concat2(MPI_Datatype type1, MPI_Datatype type2, MPI_Datatype* newtype);

/* distribute a sorted set of elements acquired from a sortz back to
 * ranks requiring a sort */
int dtcmp_sortz_to_sort(
  const void* inbuf,
  int incount,
  void* outbuf,
  int outcount,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints,
  MPI_Comm comm
);

/* ---------------------------------------
 * Uniqify functions - ensure every element is unique for stable sorts
 * --------------------------------------- */

/* the dtcmp_uniqify function takes an input buffer with count, key,
 * keysat, and comparison operation and allocates a new buffer ensuring
 * each element is unique by copying original elements with rank and
 * original index, returns new buffer, new key and ketsat types, and
 * new comparison operation.  When done, the associated handle must be
 * passed to DTCMP_Handle_free to free the buffer and newly created
 * types. */
int dtcmp_uniqify(
  const void* buf, int count, MPI_Datatype key, MPI_Datatype keysat, DTCMP_Op cmp, DTCMP_Flags hints,
  void** outbuf, MPI_Datatype* outkey, MPI_Datatype* outkeysat, DTCMP_Op* outcmp, DTCMP_Flags* outhints,
  int rank, DTCMP_Handle* uniqhandle
);

int dtcmp_deuniqify(
  const void* buf, int count, MPI_Datatype key, MPI_Datatype keysat,
  void* outbuf, MPI_Datatype outkey, MPI_Datatype outkeysat,
  DTCMP_Handle* uniqhandle
);

int dtcmp_deuniqifyz(
  const void* buf, int count, MPI_Datatype key, MPI_Datatype keysat,
  void** outbuf, MPI_Datatype outkey, MPI_Datatype outkeysat,
  DTCMP_Handle* uniqhandle, DTCMP_Handle* handle
);

typedef struct {
  MPI_Datatype key;
  MPI_Datatype keysat;
  DTCMP_Op cmp;
  void* buf;
} dtcmp_handle_uniqify_t;

/* allocates a block of memory like handle_alloc_single, but also
 * includes room for a dtcmp_handle_uniqify_t struct right after
 * the function pointer to the free function */
int dtcmp_handle_alloc_uniqify(size_t size, dtcmp_handle_uniqify_t** vals, DTCMP_Handle* handle);

/* frees the types and op associated with internal dtcmp_handle_uniqify_t
 * struct and then frees block of memory associated with handle */
int dtcmp_handle_free_uniqify(DTCMP_Handle* handle);

/* ---------------------------------------
 * Seach implementations
 * --------------------------------------- */

int DTCMP_Search_low_local_binary(
  const void* target,
  const void* list,
  int low,
  int high,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints,
  int* flag,
  int* index
);

int DTCMP_Search_high_local_binary(
  const void* target,
  const void* list,
  int low,
  int high,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints,
  int* flag,
  int* index
);

int DTCMP_Search_low_list_local_binary(
  int num,
  const void* targets,
  const void* list,
  int low,
  int high,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints,
  int flags[],
  int indicies[]
);

/* ---------------------------------------
 * Parition implementations
 * --------------------------------------- */

int dtcmp_partition_local_memcpy(
  void* buf,
  void* scratch,
  int pivot,
  int num,
  size_t size,
  DTCMP_Op cmp,
  DTCMP_Flags hints
);

int DTCMP_Partition_local_dtcpy(
  void* buf,
  int count,
  int inpivot,
  int* outpivot,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints
);

int DTCMP_Partition_local_target_dtcpy(
  void* buf,
  int count,
  const void* target,
  int* outindex,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints
);

int DTCMP_Partition_local_target_list_dtcpy(
  void* buf,
  int count,
  int offset,
  int num,
  const void* targets,
  int indicies[],
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints
);

/* ---------------------------------------
 * Merge implementations
 * --------------------------------------- */

int dtcmp_merge_local_2way_memcpy(
  int num,
  const void* inbufs[],
  int counts[],
  void* outbuf,
  size_t size,
  DTCMP_Op cmp,
  DTCMP_Flags hints
);

int DTCMP_Merge_local_2way(
  int num,
  const void* inbufs[],
  int counts[],
  void* outbuf,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints
);

int DTCMP_Merge_local_kway_heap(
  int k,
  const void* inbufs[],
  int counts[],
  void* outbuf,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints
);

/* ---------------------------------------
 * Local select implementations
 * --------------------------------------- */

int dtcmp_select_local_ends(
  void* buf,
  int num,
  int k,
  void* item,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints
);

int DTCMP_Select_local_ends(
  const void* buf,
  int num,
  int k,
  void* item,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints
);

int DTCMP_Select_local_randpartition(
  const void* buf,
  int num,
  int k,
  void* item,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints
);

/* ---------------------------------------
 * Select implementations
 * --------------------------------------- */

int DTCMP_Selectv_rand(
  const void* buf,
  int num,
  uint64_t k,
  void* item,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints,
  MPI_Comm comm
);

int DTCMP_Selectv_medianofmedians(
  const void* buf,
  int num,
  uint64_t k,
  void* item,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints,
  MPI_Comm comm
);

/* ---------------------------------------
 * Local sort implementations
 * --------------------------------------- */

int DTCMP_Sort_local_insertionsort(
  const void* inbuf,
  void* outbuf,
  int count,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints
);

int DTCMP_Sort_local_randquicksort(
  const void* inbuf, 
  void* outbuf,
  int count,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints
);

int DTCMP_Sort_local_mergesort(
  const void* inbuf,
  void* outbuf,
  int count,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints
);

int DTCMP_Sort_local_qsort(
  const void* inbuf,
  void* outbuf,
  int count,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints
);

/* ---------------------------------------
 * Sort implementations
 * --------------------------------------- */

int DTCMP_Sort_allgather(
  const void* inbuf,
  void* outbuf,
  int count,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints,
  MPI_Comm comm
);

int DTCMP_Sort_bitonic(
  const void* inbuf,
  void* outbuf,
  int count,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints,
  MPI_Comm comm
);

int DTCMP_Sort_samplesort(
  const void* inbuf,
  void* outbuf,
  int count,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints,
  MPI_Comm comm
);

/* ---------------------------------------
 * Sortv implementations
 * --------------------------------------- */

int DTCMP_Sortv_allgather(
  const void* inbuf,
  void* outbuf,
  int count,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints,
  MPI_Comm comm
);

int DTCMP_Sortv_sortgather_scatter(
  const void* inbuf,
  void* outbuf,
  int count,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints,
  MPI_Comm comm
);

int DTCMP_Sortv_cheng(
  const void* inbuf,
  void* outbuf,
  int count,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints,
  MPI_Comm comm
);

int DTCMP_Sortv_cheng_lwgrp(
  const void* inbuf,
  void* outbuf,
  int count,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints,
  const lwgrp_comm* comm
);

/* ---------------------------------------
 * Sortz implementations
 * --------------------------------------- */

int DTCMP_Sortz_samplesort(
  const void* inbuf,
  int count,
  void** outbuf,
  int* outcount,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints,
  MPI_Comm comm,
  DTCMP_Handle* handle
);

/* ---------------------------------------
 * Rankv implementations
 * --------------------------------------- */

int DTCMP_Rankv_sort(
  int count,
  const void* buf,
  uint64_t* groups,
  uint64_t  group_id[],
  uint64_t  group_ranks[],
  uint64_t  group_rank[],
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints,
  MPI_Comm comm
);

int DTCMP_Rankv_strings_sort(
  int count,
  const char* strings[],
  uint64_t* groups,
  uint64_t  group_id[],
  uint64_t  group_ranks[],
  uint64_t  group_rank[],
  DTCMP_Flags hints,
  MPI_Comm comm
);

#endif /* DTCMP_INTERNAL_H_ */
