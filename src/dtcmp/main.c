/* Copyright (c) 2012, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory.
 * Written by Adam Moody <moody20@llnl.gov>.
 * LLNL-CODE-557516.
 * All rights reserved.
 * This file is part of the DTCMP library.
 * For details, see https://github.com/hpc/dtcmp
 * Please also read this file: LICENSE.TXT. */

//  use mvapich2-gnu-1.9
//  setenv LD_LIBRARY_PATH "../../lwgrp.git/install_mvapich2/lib:../install/lib"
//  mpicc -g -O0 -o main main.c -I../../lwgrp.git/install_mvapich2/include -L../../lwgrp.git/install_mvapich2/lib -L../install/lib -ldtcmp -llwgrp

#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>

#include "mpi.h"
#include "dtcmp.h"
#include "dtcmp_internal.h"

//#define SIZE (50)
//#define SIZE (5)
#define SIZE (12)

int main(int argc, char* argv[])
{
  int i;
  int flag;

  MPI_Init(NULL, NULL);
  DTCMP_Init();

  int rank, ranks;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &ranks);

  int inbuf[SIZE], outbuf[SIZE];
  for (i = 0; i < SIZE; i++) {
    inbuf[i] = -(i*10 + rank);
    //inbuf[i] = -(i*10);
    outbuf[i] = 0;
  }

  int size = SIZE;
#if 0
  if (rank % 2 == 1) {
    size = 3;
  }
  size = rank;
#endif

  unsigned long long int scanbuf[SIZE];
  int valbuf[SIZE];
  int ltrbuf[SIZE];
  int rtlbuf[SIZE];
  for (i = 0; i < SIZE; i++) {
    //scanbuf[i] = (rank * SIZE + i) / 10;
    if (rank == 25) {
        if (i < SIZE/2) scanbuf[i] = 1;
        else scanbuf[i] = 2;
    } else if (rank == 32) {
        if (i < SIZE/2) scanbuf[i] = 2;
        else scanbuf[i] = 3;
    }
    valbuf[i]  = 1;
    ltrbuf[i]  = 0;
    rtlbuf[i]  = 0;
  }

  int scansize = 0;
  if (rank == 25) scansize = 12;
  else if (rank == 32) scansize = 8;
  DTCMP_Segmented_exscan(
    scansize, scanbuf, MPI_UNSIGNED_LONG_LONG,
    valbuf, ltrbuf, rtlbuf, MPI_INT,
    DTCMP_OP_UINT64T_ASCEND, DTCMP_FLAG_NONE,
    MPI_SUM, MPI_COMM_WORLD
  );

  for (i = 0; i < scansize; i++) {
    printf("%d: item %d = key %d, ltr %d, rtl %d\n", rank, i, (int)scanbuf[i], ltrbuf[i], rtlbuf[i]);
  }

#if 0
//  DTCMP_Sortv(inbuf, outbuf, size, MPI_INT, MPI_INT, DTCMP_OP_INT_DESCEND, MPI_COMM_WORLD);
  //DTCMP_Sort_local(inbuf, outbuf, size, MPI_INT, MPI_INT, DTCMP_OP_INT_DESCEND);
//  DTCMP_Sort_bitonic(inbuf, outbuf, size, MPI_INT, MPI_INT, DTCMP_OP_INT_ASCEND, MPI_COMM_WORLD);
  for (i = 0; i < size; i++) {
    printf("%d: item %d = %d\n", rank, i, outbuf[i]);
  }

  uint64_t num_groups, group_ids[SIZE], group_ranks[SIZE], group_rank[SIZE];
  DTCMP_Rankv(
    size, inbuf,
    &num_groups, group_ids, group_ranks, group_rank,
    MPI_INT, MPI_INT, DTCMP_OP_INT_DESCEND, DTCMP_FLAG_NONE, MPI_COMM_WORLD
  );
  for (i = 0; i < size; i++) {
    printf("%d: item %d = %d, groups=%lu, group=%lu, ranks=%lu, rank=%lu\n",
      rank, i, inbuf[i], num_groups, group_ids[i], group_ranks[i], group_rank[i]
    );
  }

  const char* strings[4] = {"hello", "my", "mean", "world!"};
  DTCMP_Rankv_strings(
    size, strings,
    &num_groups, group_ids, group_ranks, group_rank,
    DTCMP_FLAG_NONE, MPI_COMM_WORLD
  );
  for (i = 0; i < size; i++) {
    printf("%d: item %d = %s, groups=%lu, group=%lu, ranks=%lu, rank=%lu\n",
      rank, i, strings[i], num_groups, group_ids[i], group_ranks[i], group_rank[i]
    );
  }

  DTCMP_Is_sorted(inbuf, size, MPI_INT, MPI_INT, DTCMP_OP_INT_ASCEND, DTCMP_FLAG_NONE, MPI_COMM_SELF, &flag);
  printf("%d: flag = %d\n", rank, flag);

  int insatbuf[SIZE*2], outsatbuf[SIZE*2];
  for (i = 0; i < SIZE; i++) {
    insatbuf[i*2+0] = -(i*10 + rank);
//    insatbuf[i*2+0] = 35 - (i%2);
    insatbuf[i*2+1] = +(i*10 + rank);
    outsatbuf[i*2+0] = 0;
    outsatbuf[i*2+1] = 0;
  }

  DTCMP_Op cmp_2int;
  DTCMP_Op_create_series2(DTCMP_OP_INT_ASCEND, DTCMP_OP_INT_DESCEND, &cmp_2int);

  MPI_Datatype type_2int;
  MPI_Type_contiguous(2, MPI_INT, &type_2int);
//  MPI_Type_vector(2, 1, 2, MPI_INT, &type_2int);
  MPI_Type_commit(&type_2int);

  DTCMP_Sortv(insatbuf, outsatbuf, size, type_2int, type_2int, cmp_2int, DTCMP_FLAG_NONE, MPI_COMM_WORLD);
//  DTCMP_Sortv(insatbuf, outsatbuf, size, MPI_INT, type_2int, DTCMP_OP_INT_ASCEND, MPI_COMM_WORLD);
//  DTCMP_Sortv_allgather(insatbuf, outsatbuf, size, MPI_INT, type_2int, DTCMP_OP_INT_ASCEND, MPI_COMM_WORLD);
//  DTCMP_Sortv_sortgather_scatter(insatbuf, outsatbuf, size, MPI_INT, type_2int, DTCMP_OP_INT_ASCEND, MPI_COMM_WORLD);
//  DTCMP_Sortv_sortgather_scatter(insatbuf, outsatbuf, size, type_2int, type_2int, cmp_2int, MPI_COMM_WORLD);
  int sortz_outcount;
  void* sortz_outbuf;
  DTCMP_Handle handle;
  DTCMP_Sortz(insatbuf, size, &sortz_outbuf, &sortz_outcount, type_2int, type_2int, cmp_2int, DTCMP_FLAG_NONE, MPI_COMM_WORLD, &handle);
  DTCMP_Free(&handle);

  for (i = 0; i < size; i++) {
    printf("%d: item %d = %d(%d)\n", rank, i, outsatbuf[i*2+0], outsatbuf[i*2+1]);
  }

  DTCMP_Is_sorted(outsatbuf, size, type_2int, type_2int, cmp_2int, DTCMP_FLAG_NONE, MPI_COMM_WORLD, &flag);
//  DTCMP_Is_sorted(outsatbuf, size, type_2int, type_2int, DTCMP_OP_INT_ASCEND, MPI_COMM_WORLD, &flag);
  if (rank == 0) {
    printf("%d: flag = %d\n", rank, flag);
  }

  int index;
  int target[2];
  target[0] = 35 - (4%2);
  target[1] = +(4*10 + rank);

  DTCMP_Search_low_local(target, outsatbuf, 0, size-1, MPI_INT, type_2int, DTCMP_OP_INT_DESCEND, DTCMP_FLAG_NONE, &flag, &index);
//  DTCMP_Search_local_low(target, outsatbuf, 0, size-1, type_2int, type_2int, cmp_2int, &flag, &index);
  printf("%d: flag %d, index %d\n", rank, flag, index);

  DTCMP_Search_high_local(target, outsatbuf, 0, size-1, MPI_INT, type_2int, DTCMP_OP_INT_DESCEND, DTCMP_FLAG_NONE, &flag, &index);
//  DTCMP_Search_local_high(target, outsatbuf, 0, size-1, type_2int, type_2int, cmp_2int, &flag, &index);
  printf("%d: flag %d, index %d\n", rank, flag, index);

  int item[2];
  DTCMP_Select_local(outsatbuf, size, 0, &item, MPI_INT, type_2int, DTCMP_OP_INT_ASCEND, DTCMP_FLAG_NONE);
  printf("%d: rank %d = %d\n", rank, 0, item[0]);

  DTCMP_Select_local(outsatbuf, size, size-1, &item, MPI_INT, type_2int, DTCMP_OP_INT_ASCEND, DTCMP_FLAG_NONE);
  printf("%d: rank %d = %d\n", rank, size-1, item[0]);

  DTCMP_Select_local(outsatbuf, size, size/2, &item, MPI_INT, type_2int, DTCMP_OP_INT_ASCEND, DTCMP_FLAG_NONE);
  printf("%d: rank %d = %d\n", rank, size/2, item[0]);

/* sorting tests, check result of each with is_sorted, which is assumed to be correct code
 *   all procs contribute one item
 *   all procs contribute same number of items > 1
 *   all procs contribute diff number of items, but at least one
 *   one proc has zero items
 *   only rank 0 has zero items
 *   only rank N-1 has zero items
 *   every other proc has zero items
 *   randomly select procs with zero items
 *   all procs have zero items
 */

  MPI_Type_free(&type_2int);

  DTCMP_Op_free(&cmp_2int);
#endif

  DTCMP_Finalize();
  MPI_Finalize();

  return 0;
}
