/* Copyright (c) 2012, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory.
 * Written by Adam Moody <moody20@llnl.gov>.
 * LLNL-CODE-557516.
 * All rights reserved.
 * This file is part of the DTCMP library.
 * For details, see https://github.com/hpc/dtcmp
 * Please also read this file: LICENSE.TXT. */

#include <stdlib.h>
#include <stdio.h>

#include "mpi.h"
#include "dtcmp.h"
#include "dtcmp_internal.h"

#define SIZE (50)

typedef int(*selectv_fn)(const void* inbuf, int size, uint64_t k, void* outbuf, MPI_Datatype key, MPI_Datatype keysat, DTCMP_Op op, DTCMP_Flags hints, MPI_Comm comm);
typedef int(*sort_local_fn)(const void* inbuf, void* outbuf, int size, MPI_Datatype key, MPI_Datatype keysat, DTCMP_Op op, DTCMP_Flags hints);
typedef int(*sort_fn)(const void* inbuf, void* outbuf, int size, MPI_Datatype key, MPI_Datatype keysat, DTCMP_Op op, DTCMP_Flags hints, MPI_Comm comm);
typedef int(*sortv_fn)(const void* inbuf, void* outbuf, int size, MPI_Datatype key, MPI_Datatype keysat, DTCMP_Op op, DTCMP_Flags hints, MPI_Comm comm);
typedef int(*sortz_fn)(const void* inbuf, int incount, void** outbuf, int* outcount, MPI_Datatype key, MPI_Datatype keysat, DTCMP_Op op, DTCMP_Flags hints, MPI_Comm comm, DTCMP_Handle* handle);

#define NUM_SELECTV_FNS (3)
selectv_fn selectv_fns[NUM_SELECTV_FNS] = {
  DTCMP_Selectv,
  DTCMP_Selectv_rand,
  DTCMP_Selectv_medianofmedians,
};
char* selectv_names[NUM_SELECTV_FNS] = {
  "DTCMP_Selectv",
  "DTCMP_Selectv_rand",
  "DTCMP_Selectv_medianofmedians",
};

#define NUM_SORT_LOCAL_FNS (5)
sort_local_fn sort_local_fns[NUM_SORT_LOCAL_FNS] = {
  DTCMP_Sort_local,
  DTCMP_Sort_local_insertionsort,
  DTCMP_Sort_local_randquicksort,
  DTCMP_Sort_local_mergesort,
  DTCMP_Sort_local_qsort, /* this can only be used for basic types */
};
char* sort_local_names[NUM_SORT_LOCAL_FNS] = {
  "DTCMP_Sort_local",
  "DTCMP_Sort_local_insertionsort",
  "DTCMP_Sort_local_randquicksort",
  "DTCMP_Sort_local_mergesort",
  "DTCMP_Sort_local_qsort",
};

#define NUM_SORT_FNS (4)
sort_fn sort_fns[NUM_SORT_FNS] = {
  DTCMP_Sort,
  DTCMP_Sort_allgather,
  DTCMP_Sort_bitonic,
  DTCMP_Sort_samplesort,
};
char* sort_names[NUM_SORT_FNS] = {
  "DTCMP_Sort",
  "DTCMP_Sort_allgather",
  "DTCMP_Sort_bitonic",
  "DTCMP_Sort_samplesort",
};

#define NUM_SORTV_FNS (4)
sortv_fn sortv_fns[NUM_SORTV_FNS] = {
  DTCMP_Sortv,
  DTCMP_Sortv_allgather,
  DTCMP_Sortv_cheng,
  DTCMP_Sortv_sortgather_scatter,
};
char* sortv_names[NUM_SORTV_FNS] = {
  "DTCMP_Sortv",
  "DTCMP_Sortv_allgather",
  "DTCMP_Sortv_cheng",
  "DTCMP_Sortv_sortgather_scatter",
};

#define NUM_SORTZ_FNS (2)
sortz_fn sortz_fns[NUM_SORTZ_FNS] = {
  DTCMP_Sortz,
  DTCMP_Sortz_samplesort,
};
char* sortz_names[NUM_SORTZ_FNS] = {
  "DTCMP_Sortz",
  "DTCMP_Sortz_samplesort",
};

int test_selectv(
  const char* test,
  selectv_fn fn,
  const char* name,
  const void* inbuf,
  int size,
  uint64_t k,
  void* outbuf,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints,
  MPI_Comm comm)
{
  (*fn)(inbuf, size, k, outbuf, key, keysat, cmp, hints, comm);

  uint64_t lt, eq, gt;
  dtcmp_get_lt_eq_gt(outbuf, inbuf, size, key, keysat, cmp, hints, &lt, &eq, &gt, comm);

  if (k <= lt || k > (lt + eq)) {
    int rank;
    MPI_Comm_rank(comm, &rank);
    if (rank == 0) {
      printf("ERROR DETECTED rank=%d test=%s routine=%s\n", rank, test, name);
    }
  }
  return 0;
}

int test_all_selectv(
  const char* test,
  uint64_t k,
  void* outbuf,
  const void* inbuf,
  int size,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints,
  MPI_Comm comm)
{
  int i;

  for (i = 0; i < NUM_SELECTV_FNS; i++) {
    test_selectv(test, selectv_fns[i], selectv_names[i], inbuf, size, k, outbuf, key, keysat, cmp, hints, comm);
  }

  return 0;
}

int test_sort_local(
  const char* test,
  sort_local_fn fn,
  const char* name,
  const void* inbuf,
  void* outbuf,
  int size,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints)
{
  (*fn)(inbuf, outbuf, size, key, keysat, cmp, hints);

  int flag;
  DTCMP_Is_sorted(outbuf, size, key, keysat, cmp, hints, MPI_COMM_SELF, &flag);
  if (flag != 1) {
    int rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    printf("ERROR DETECTED rank=%d test=%s routine=%s\n", rank, test, name);
  }
  return flag;
}

int test_sort(
  const char* test,
  sort_fn fn,
  const char* name,
  const void* inbuf,
  void* outbuf,
  int size,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints,
  MPI_Comm comm)
{
  (*fn)(inbuf, outbuf, size, key, keysat, cmp, hints, comm);

  int flag;
  DTCMP_Is_sorted(outbuf, size, key, keysat, cmp, hints, comm, &flag);
  if (flag != 1) {
    int rank;
    MPI_Comm_rank(comm, &rank);
    if (rank == 0) {
      printf("ERROR DETECTED rank=%d test=%s routine=%s\n", rank, test, name);
    }
  }
  return flag;
}

int test_sortv(
  const char* test,
  sortv_fn fn,
  const char* name,
  const void* inbuf,
  void* outbuf,
  int size,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints,
  MPI_Comm comm)
{
  (*fn)(inbuf, outbuf, size, key, keysat, cmp, hints, comm);

  int flag;
  DTCMP_Is_sorted(outbuf, size, key, keysat, cmp, hints, comm, &flag);
  if (flag != 1) {
    int rank;
    MPI_Comm_rank(comm, &rank);
    if (rank == 0) {
      printf("ERROR DETECTED rank=%d test=%s routine=%s\n", rank, test, name);
    }
  }
  return flag;
}

int test_sortz(
  const char* test,
  sortz_fn fn,
  const char* name,
  const void* inbuf,
  int size,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints,
  MPI_Comm comm)
{
  void* outbuf;
  int outcount;
  DTCMP_Handle handle;
  (*fn)(inbuf, size, &outbuf, &outcount, key, keysat, cmp, hints, comm, &handle);

  int flag;
  DTCMP_Is_sorted(outbuf, outcount, key, keysat, cmp, hints, comm, &flag);
  if (flag != 1) {
    int rank;
    MPI_Comm_rank(comm, &rank);
    if (rank == 0) {
      printf("ERROR DETECTED rank=%d test=%s routine=%s\n", rank, test, name);
    }
  }

  DTCMP_Free(&handle);
  return flag;
}

int test_all_sorts(
  const char* test,
  const void* inbuf,
  void* outbuf,
  int size,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints,
  MPI_Comm comm)
{
  int i;

  for (i = 0; i < NUM_SORT_LOCAL_FNS; i++) {
    test_sort_local(test, sort_local_fns[i], sort_local_names[i], inbuf, outbuf, size, key, keysat, cmp, hints);
  }

  for (i = 0; i < NUM_SORT_FNS; i++) {
    test_sort(test, sort_fns[i], sort_names[i], inbuf, outbuf, size, key, keysat, cmp, hints, comm);
  }

  for (i = 0; i < NUM_SORTV_FNS; i++) {
    test_sortv(test, sortv_fns[i], sortv_names[i], inbuf, outbuf, size, key, keysat, cmp, hints, comm);
  }

  for (i = 0; i < NUM_SORTZ_FNS; i++) {
    test_sortz(test, sortz_fns[i], sortz_names[i], inbuf, size, key, keysat, cmp, hints, comm);
  }

  return 0;
}

int test_variable_sorts(
  const char* test,
  const void* inbuf,
  void* outbuf,
  int size,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints,
  MPI_Comm comm)
{
  int i;

  for (i = 0; i < NUM_SORTV_FNS; i++) {
    test_sortv(test, sortv_fns[i], sortv_names[i], inbuf, outbuf, size, key, keysat, cmp, hints, comm);
  }

  for (i = 0; i < NUM_SORTZ_FNS; i++) {
    test_sortz(test, sortz_fns[i], sortz_names[i], inbuf, size, key, keysat, cmp, hints, comm);
  }

  return 0;
}

int main(int argc, char* argv[])
{
  int i;

  MPI_Init(NULL, NULL);
  DTCMP_Init();

  int rank, ranks;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &ranks);

/* sorting tests, check result of each with is_sorted, which is assumed to be correct code
 *   all procs contribute zero items
 *   all procs contribute one item
 *   all procs contribute same number of items > 1
 *   all procs contribute diff number of items, but at least one
 *   only rank 0 has zero items
 *   only rank N-1 has zero items
 *   every other proc has zero items
 *   ** randomly select procs with zero items
 *
 *   ** repeat all of above with duplicates
 *
 *   items already in order
 *   items in reverse order
 *   items in random order
 *
 */

  char* test;
  void* inbuf;
  void* outbuf;
  int size;
  MPI_Datatype key, keysat;
  DTCMP_Op op;
  DTCMP_Flags hints = DTCMP_FLAG_NONE;
  MPI_Comm comm;
  uint64_t kth;

DTCMP_Op series1, series2, series3;
DTCMP_Op_create_series2(DTCMP_OP_INT_ASCEND, DTCMP_OP_LONG_ASCEND, &series1);
DTCMP_Op_create_series2(DTCMP_OP_FLOAT_ASCEND, DTCMP_OP_DOUBLE_ASCEND, &series2);
DTCMP_Op_create_series2(series1, series2, &series3);

DTCMP_Op series4;
DTCMP_Op series[4];
series[0] = DTCMP_OP_INT_ASCEND;
series[1] = DTCMP_OP_LONG_ASCEND;
series[2] = DTCMP_OP_FLOAT_ASCEND;
series[3] = DTCMP_OP_DOUBLE_ASCEND;
DTCMP_Op_create_series(4, series, &series4);

  int in_1int[SIZE], out_1int[SIZE];
  inbuf  = (void*) in_1int;
  outbuf = (void*) out_1int;

  for (i = 0; i < SIZE; i++) {
    in_1int[i] = -(i*10 + rank);
    out_1int[i] = 0;
  }

  key    = MPI_INT;
  keysat = MPI_INT;
  comm   = MPI_COMM_WORLD;

  /* test that all sorts work with count=0 on all procs */
  size = 0;
  test = "0 INT ASCEND";
  op = DTCMP_OP_INT_ASCEND;
  test_all_sorts(test, inbuf, outbuf, size, key, keysat, op, hints, comm);

  /* test that all sorts work with count=1 on all procs */
  size = 1;
  test = "1 INT/INT ASCEND";
  op = DTCMP_OP_INT_ASCEND;
  test_all_sorts(test, inbuf, outbuf, size, key, keysat, op, hints, comm);
  test = "1 INT/INT DESCEND";
  op = DTCMP_OP_INT_DESCEND;
  test_all_sorts(test, inbuf, outbuf, size, key, keysat, op, hints, comm);

kth = 1;
test_all_selectv(test, kth, outbuf, inbuf, size, key, keysat, op, hints, comm);
kth = (ranks*size/2)+1;
test_all_selectv(test, kth, outbuf, inbuf, size, key, keysat, op, hints, comm);
kth = ranks*size;
test_all_selectv(test, kth, outbuf, inbuf, size, key, keysat, op, hints, comm);

  /* test that all sorts work with count>1 on all procs */
  size = SIZE;
  test = "SIZE INT/INT ASCEND";
  op = DTCMP_OP_INT_ASCEND;
  test_all_sorts(test, inbuf, outbuf, size, key, keysat, op, hints, comm);
  test = "SIZE INT/INT DESCEND";
  op = DTCMP_OP_INT_DESCEND;
  test_all_sorts(test, inbuf, outbuf, size, key, keysat, op, hints, comm);

kth = 1;
test_all_selectv(test, kth, outbuf, inbuf, size, key, keysat, op, hints, comm);
kth = (ranks*size/2)+1;
test_all_selectv(test, kth, outbuf, inbuf, size, key, keysat, op, hints, comm);
kth = ranks*size;
test_all_selectv(test, kth, outbuf, inbuf, size, key, keysat, op, hints, comm);

  for (i = 0; i < SIZE; i++) {
    in_1int[i]  = 1;
    out_1int[i] = 0;
  }

  /* test that all sorts work with count=0 on all procs */
  size = 0;
  test = "0 INT ASCEND DUP";
  op = DTCMP_OP_INT_ASCEND;
  test_all_sorts(test, inbuf, outbuf, size, key, keysat, op, hints, comm);

  /* test that all sorts work with count=1 on all procs */
  size = 1;
  test = "1 INT/INT ASCEND DUP";
  op = DTCMP_OP_INT_ASCEND;
  test_all_sorts(test, inbuf, outbuf, size, key, keysat, op, hints, comm);
  test = "1 INT/INT DESCEND DUP";
  op = DTCMP_OP_INT_DESCEND;
  test_all_sorts(test, inbuf, outbuf, size, key, keysat, op, hints, comm);

DTCMP_Selectv(inbuf, size,              1, outbuf, key, keysat, op, hints, comm);
DTCMP_Selectv(inbuf, size, (ranks*size/2), outbuf, key, keysat, op, hints, comm);
DTCMP_Selectv(inbuf, size, (ranks*size/1), outbuf, key, keysat, op, hints, comm);

  /* test that all sorts work with count>1 on all procs */
  size = SIZE;
  test = "SIZE INT/INT ASCEND DUP";
  op = DTCMP_OP_INT_ASCEND;
  test_all_sorts(test, inbuf, outbuf, size, key, keysat, op, hints, comm);
  test = "SIZE INT/INT DESCEND DUP";
  op = DTCMP_OP_INT_DESCEND;
  test_all_sorts(test, inbuf, outbuf, size, key, keysat, op, hints, comm);


  MPI_Datatype type_2int;
  MPI_Type_contiguous(2, MPI_INT, &type_2int);
  MPI_Type_commit(&type_2int);

  DTCMP_Op cmp_updown, cmp_downdown;
  DTCMP_Op_create_series2(DTCMP_OP_INT_ASCEND, DTCMP_OP_INT_DESCEND, &cmp_updown);
  DTCMP_Op_create_series2(DTCMP_OP_INT_DESCEND, DTCMP_OP_INT_DESCEND, &cmp_downdown);

  int in_2int[SIZE*2], out_2int[SIZE*2];
  inbuf  = (void*) in_2int;
  outbuf = (void*) out_2int;

  for (i = 0; i < SIZE; i++) {
    in_2int[i*2+0]  = -(i*10 + rank);
    in_2int[i*2+1]  = +(i*10 + rank);
    out_2int[i*2+0] = 0;
    out_2int[i*2+1] = 0;
  }

  key    = MPI_INT;
  keysat = type_2int;
  comm   = MPI_COMM_WORLD;

  /* test that all sorts work with count=0 on all procs */
  size = 0;
  test = "0 INT/2INT ASCEND";
  op = DTCMP_OP_INT_ASCEND;
  test_all_sorts(test, inbuf, outbuf, size, key, keysat, op, hints, comm);

  /* test that all sorts work with count=1 on all procs */
  size = 1;
  test = "1 INT/2INT ASCEND";
  op = DTCMP_OP_INT_ASCEND;
  test_all_sorts(test, inbuf, outbuf, size, key, keysat, op, hints, comm);
  test = "1 INT/2INT DESCEND";
  op = DTCMP_OP_INT_DESCEND;
  test_all_sorts(test, inbuf, outbuf, size, key, keysat, op, hints, comm);

void* partbuf;
int partcount;
DTCMP_Handle handle;
  /* test that all sorts work with count>1 on all procs */
  size = SIZE;
  test = "SIZE INT/2INT ASCEND";
  op = DTCMP_OP_INT_ASCEND;
  test_all_sorts(test, inbuf, outbuf, size, key, keysat, op, hints, comm);
DTCMP_Partitionz(inbuf, size, size*ranks/2+1, ranks/2, &partbuf, &partcount, key, keysat, op, hints, comm, &handle);
DTCMP_Free(&handle);
uint64_t splitters[2];
splitters[0] = (uint64_t) (size * ranks * 1 / 3);
splitters[1] = (uint64_t) (size * ranks * 2 / 3);
int divideranks[2];
divideranks[0] = ranks * 1 / 3;
divideranks[1] = ranks * 2 / 3;
DTCMP_Partitionz_list(inbuf, size, 2, splitters, divideranks, &partbuf, &partcount, key, keysat, op, hints, comm, &handle);
DTCMP_Free(&handle);
  test = "SIZE INT/2INT DESCEND";
  op = DTCMP_OP_INT_DESCEND;
  test_all_sorts(test, inbuf, outbuf, size, key, keysat, op, hints, comm);

  key    = type_2int;
  keysat = type_2int;
  comm   = MPI_COMM_WORLD;

  /* test that all sorts work with count=0 on all procs */
  size = 0;
  test = "0 2INT/2INT ASCEND";
  op = cmp_updown;
  test_all_sorts(test, inbuf, outbuf, size, key, keysat, op, hints, comm);

  /* test that all sorts work with count=1 on all procs */
  size = 1;
  test = "1 2INT/2INT ASCEND";
  op = cmp_updown;
  test_all_sorts(test, inbuf, outbuf, size, key, keysat, op, hints, comm);
  test = "1 2INT/2INT DESCEND";
  op = cmp_downdown;
  test_all_sorts(test, inbuf, outbuf, size, key, keysat, op, hints, comm);

  /* test that all sorts work with count>1 on all procs */
  size = SIZE;
  test = "SIZE 2INT/2INT ASCEND";
  op = cmp_updown;
  test_all_sorts(test, inbuf, outbuf, size, key, keysat, op, hints, comm);
  test = "SIZE 2INT/2INT DESCEND";
  op = cmp_downdown;
  test_all_sorts(test, inbuf, outbuf, size, key, keysat, op, hints, comm);

  key    = MPI_INT;
  keysat = type_2int;
  comm   = MPI_COMM_WORLD;

  /* test variable counts across procs where 1 <= count <= 10 */
  size = rank % 10 + 1;
  if (size > SIZE) {
    printf("Invalid size %d limit %d\n", size, SIZE);
    return 1;
  }
  test = "1-10 INT/2INT ASCEND";
  op = DTCMP_OP_INT_ASCEND;
  test_variable_sorts(test, inbuf, outbuf, size, key, keysat, op, hints, comm);
DTCMP_Partitionz(inbuf, size, 6, ranks/2, &partbuf, &partcount, key, keysat, op, hints, comm, &handle);
DTCMP_Free(&handle);
  test = "1-10 INT/2INT DESCEND";
  op = DTCMP_OP_INT_DESCEND;
  test_variable_sorts(test, inbuf, outbuf, size, key, keysat, op, hints, comm);

  /* test variable counts where count=0 on rank=0, 1 <= count <= 10 elsewhere */
  size = rank % 10 + 1;
  if (rank == 0) {
    size = 0;
  }
  if (size > SIZE) {
    printf("Invalid size %d limit %d\n", size, SIZE);
    return 1;
  }
  test = "0,1-10 INT/2INT ASCEND";
  op = DTCMP_OP_INT_ASCEND;
  test_variable_sorts(test, inbuf, outbuf, size, key, keysat, op, hints, comm);
  test = "0,1-10 INT/2INT DESCEND";
  op = DTCMP_OP_INT_DESCEND;
  test_variable_sorts(test, inbuf, outbuf, size, key, keysat, op, hints, comm);

  /* test variable counts where count=0 on rank=N-1, 1 <= count <= 10 elsewhere */
  size = rank % 10 + 1;
  if (rank == ranks-1) {
    size = 0;
  }
  if (size > SIZE) {
    printf("Invalid size %d limit %d\n", size, SIZE);
    return 1;
  }
  test = "1-10,0 INT/2INT ASCEND";
  op = DTCMP_OP_INT_ASCEND;
  test_variable_sorts(test, inbuf, outbuf, size, key, keysat, op, hints, comm);
  test = "1-10,0 INT/2INT DESCEND";
  op = DTCMP_OP_INT_DESCEND;
  test_variable_sorts(test, inbuf, outbuf, size, key, keysat, op, hints, comm);

  /* test variable counts where count=0 on even ranks, 1 <= count <= 10 elsewhere */
  size = rank % 10 + 1;
  if (rank % 2 == 0) {
    size = 0;
  }
  if (size > SIZE) {
    printf("Invalid size %d limit %d\n", size, SIZE);
    return 1;
  }
  test = "1-10,0 even INT/2INT ASCEND";
  op = DTCMP_OP_INT_ASCEND;
  test_variable_sorts(test, inbuf, outbuf, size, key, keysat, op, hints, comm);
  test = "1-10,0 even INT/2INT DESCEND";
  op = DTCMP_OP_INT_DESCEND;
  test_variable_sorts(test, inbuf, outbuf, size, key, keysat, op, hints, comm);

  /* create a bunch of items guaranteed to be unique */
  for (i = 0; i < SIZE; i++) {
    in_2int[i*2+0]  = -(i + rank*SIZE);
    in_2int[i*2+1]  = +(i + rank*SIZE);
    out_2int[i*2+0] = 0;
    out_2int[i*2+1] = 0;
  }
  hints  = DTCMP_FLAG_UNIQUE;

  key    = MPI_INT;
  keysat = type_2int;
  comm   = MPI_COMM_WORLD;

  /* test that all sorts work with count=0 on all procs */
  size = 0;
  test = "0 INT/2INT ASCEND";
  op = DTCMP_OP_INT_ASCEND;
  test_all_sorts(test, inbuf, outbuf, size, key, keysat, op, hints, comm);

  /* test that all sorts work with count=1 on all procs */
  size = 1;
  test = "1 INT/2INT ASCEND";
  op = DTCMP_OP_INT_ASCEND;
  test_all_sorts(test, inbuf, outbuf, size, key, keysat, op, hints, comm);
  test = "1 INT/2INT DESCEND";
  op = DTCMP_OP_INT_DESCEND;
  test_all_sorts(test, inbuf, outbuf, size, key, keysat, op, hints, comm);

  /* test that all sorts work with count>1 on all procs */
  size = SIZE;
  test = "SIZE INT/2INT ASCEND";
  op = DTCMP_OP_INT_ASCEND;
  test_all_sorts(test, inbuf, outbuf, size, key, keysat, op, hints, comm);
  test = "SIZE INT/2INT DESCEND";
  op = DTCMP_OP_INT_DESCEND;
  test_all_sorts(test, inbuf, outbuf, size, key, keysat, op, hints, comm);

  /* create a bunch of identical items */
  for (i = 0; i < SIZE; i++) {
    in_2int[i*2+0]  = 1;
    in_2int[i*2+1]  = 2;
    out_2int[i*2+0] = 0;
    out_2int[i*2+1] = 0;
  }
  hints  = DTCMP_FLAG_NONE;

  key    = MPI_INT;
  keysat = type_2int;
  comm   = MPI_COMM_WORLD;

  /* test that all sorts work with count=0 on all procs */
  size = 0;
  test = "0 INT/2INT ASCEND DUP";
  op = DTCMP_OP_INT_ASCEND;
  test_all_sorts(test, inbuf, outbuf, size, key, keysat, op, hints, comm);

  /* test that all sorts work with count=1 on all procs */
  size = 1;
  test = "1 INT/2INT ASCEND DUP";
  op = DTCMP_OP_INT_ASCEND;
  test_all_sorts(test, inbuf, outbuf, size, key, keysat, op, hints, comm);
  test = "1 INT/2INT DESCEND DUP";
  op = DTCMP_OP_INT_DESCEND;
  test_all_sorts(test, inbuf, outbuf, size, key, keysat, op, hints, comm);

  /* test that all sorts work with count>1 on all procs */
  size = SIZE;
  test = "SIZE INT/2INT ASCEND DUP";
  op = DTCMP_OP_INT_ASCEND;
  test_all_sorts(test, inbuf, outbuf, size, key, keysat, op, hints, comm);
  test = "SIZE INT/2INT DESCEND DUP";
  op = DTCMP_OP_INT_DESCEND;
  test_all_sorts(test, inbuf, outbuf, size, key, keysat, op, hints, comm);

  key    = type_2int;
  keysat = type_2int;
  comm   = MPI_COMM_WORLD;

  /* test that all sorts work with count=0 on all procs */
  size = 0;
  test = "0 2INT/2INT ASCEND DUP";
  op = cmp_updown;
  test_all_sorts(test, inbuf, outbuf, size, key, keysat, op, hints, comm);

  /* test that all sorts work with count=1 on all procs */
  size = 1;
  test = "1 2INT/2INT ASCEND DUP";
  op = cmp_updown;
  test_all_sorts(test, inbuf, outbuf, size, key, keysat, op, hints, comm);
  test = "1 2INT/2INT DESCEND DUP";
  op = cmp_downdown;
  test_all_sorts(test, inbuf, outbuf, size, key, keysat, op, hints, comm);

  /* test that all sorts work with count>1 on all procs */
  size = SIZE;
  test = "SIZE 2INT/2INT ASCEND DUP";
  op = cmp_updown;
  test_all_sorts(test, inbuf, outbuf, size, key, keysat, op, hints, comm);
  test = "SIZE 2INT/2INT DESCEND DUP";
  op = cmp_downdown;
  test_all_sorts(test, inbuf, outbuf, size, key, keysat, op, hints, comm);

  key    = MPI_INT;
  keysat = type_2int;
  comm   = MPI_COMM_WORLD;

  /* test variable counts across procs where 1 <= count <= 10 */
  size = rank % 10 + 1;
  if (size > SIZE) {
    printf("Invalid size %d limit %d\n", size, SIZE);
    return 1;
  }
  test = "1-10 INT/2INT ASCEND DUP";
  op = DTCMP_OP_INT_ASCEND;
  test_variable_sorts(test, inbuf, outbuf, size, key, keysat, op, hints, comm);
  test = "1-10 INT/2INT DESCEND DUP";
  op = DTCMP_OP_INT_DESCEND;
  test_variable_sorts(test, inbuf, outbuf, size, key, keysat, op, hints, comm);

  /* test variable counts where count=0 on rank=0, 1 <= count <= 10 elsewhere */
  size = rank % 10 + 1;
  if (rank == 0) {
    size = 0;
  }
  if (size > SIZE) {
    printf("Invalid size %d limit %d\n", size, SIZE);
    return 1;
  }
  test = "0,1-10 INT/2INT ASCEND DUP";
  op = DTCMP_OP_INT_ASCEND;
  test_variable_sorts(test, inbuf, outbuf, size, key, keysat, op, hints, comm);
  test = "0,1-10 INT/2INT DESCEND DUP";
  op = DTCMP_OP_INT_DESCEND;
  test_variable_sorts(test, inbuf, outbuf, size, key, keysat, op, hints, comm);

  /* test variable counts where count=0 on rank=N-1, 1 <= count <= 10 elsewhere */
  size = rank % 10 + 1;
  if (rank == ranks-1) {
    size = 0;
  }
  if (size > SIZE) {
    printf("Invalid size %d limit %d\n", size, SIZE);
    return 1;
  }
  test = "1-10,0 INT/2INT ASCEND DUP";
  op = DTCMP_OP_INT_ASCEND;
  test_variable_sorts(test, inbuf, outbuf, size, key, keysat, op, hints, comm);
  test = "1-10,0 INT/2INT DESCEND DUP";
  op = DTCMP_OP_INT_DESCEND;
  test_variable_sorts(test, inbuf, outbuf, size, key, keysat, op, hints, comm);

  /* test variable counts where count=0 on even ranks, 1 <= count <= 10 elsewhere */
  size = rank % 10 + 1;
  if (rank % 2 == 0) {
    size = 0;
  }
  if (size > SIZE) {
    printf("Invalid size %d limit %d\n", size, SIZE);
    return 1;
  }
  test = "1-10,0 even INT/2INT ASCEND DUP";
  op = DTCMP_OP_INT_ASCEND;
  test_variable_sorts(test, inbuf, outbuf, size, key, keysat, op, hints, comm);
  test = "1-10,0 even INT/2INT DESCEND DUP";
  op = DTCMP_OP_INT_DESCEND;
  test_variable_sorts(test, inbuf, outbuf, size, key, keysat, op, hints, comm);

  MPI_Type_free(&type_2int);
  DTCMP_Op_free(&cmp_downdown);
  DTCMP_Op_free(&cmp_updown);

  DTCMP_Finalize();
  MPI_Finalize();

  return 0;
}
