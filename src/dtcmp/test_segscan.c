/* Copyright (c) 2012, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory.
 * Written by Adam Moody <moody20@llnl.gov>.
 * LLNL-CODE-557516.
 * All rights reserved.
 * This file is part of the DTCMP library.
 * For details, see https://github.com/hpc/dtcmp
 * Please also read this file: LICENSE.TXT. */

// mpicc -g -O0 -o test_segscan test_segscan.c -I./install/include -Wl,-rpath,`pwd`/install/lib -L./install/lib -ldtcmp
// srun -n4 ./test_segscan
//
// This runs a few tests with Segmented scan/exscan operations and
// verifies the results.  The program prints a message to stdout,
// and returns with an exit code of 1 if an error is detected,
// it returns 0 otherwise.

#include <stdlib.h>
#include <stdio.h>
#include "mpi.h"
#include "dtcmp.h"

int test_exscanv(int count, int segment_length)
{
  int rc = 0;

  int rank, ranks;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &ranks);

  int* keys = (int*) malloc(count * sizeof(int));
  int* vals = (int*) malloc(count * sizeof(int));
  int* ltr  = (int*) malloc(count * sizeof(int));
  int* rtl  = (int*) malloc(count * sizeof(int));

  int i;
  for (i = 0; i < count; i++) {
    keys[i] = (rank*count + i) / segment_length;
    vals[i] = 1;
    ltr[i] = -1;
    rtl[i] = -1;
  }

  DTCMP_Segmented_exscanv(
    count, keys, MPI_INT, DTCMP_OP_INT_ASCEND,
    vals, ltr, rtl, MPI_INT, MPI_SUM,
    DTCMP_FLAG_NONE, MPI_COMM_WORLD
  );

#if 0
  printf("exscanv ltr: %d ", rank);
  for (i = 0; i < count; i++) {
    printf("(%d,%d)=%d, ", keys[i], vals[i], ltr[i]);
  }
  printf("\n");

  printf("exscanv rtl: %d ", rank);
  for (i = 0; i < count; i++) {
    printf("(%d, %d)=%d, ", keys[i], vals[i], rtl[i]);
  }
  printf("\n");
#endif

  // check ltr results
  for (i = 0; i < count; i++) {
    int val = (rank*count + i) % segment_length;
    int actual = ltr[i];
    if (val == 0 && actual != -1) {
      rc = 1;
      printf("ERROR: Segmented_exscanv rank=%d ltr[%d]=%d, expected -1 @ %s:%d\n", rank, i, actual, __FILE__, __LINE__);
    } else if (val > 0 && actual != val) {
      rc = 1;
      printf("ERROR: Segmented_exscanv rank=%d ltr[%d]=%d, expected -1 @ %s:%d\n", rank, i, actual, val, __FILE__, __LINE__);
    }
  }

  // check rtl results
  int last_length = (ranks * count) % segment_length;
  int length = segment_length;
  for (i = 0; i < count; i++) {
    // if we're in the last segment, it may not be full length
    int offset = rank * count + i;
    int remainder = ranks * count - offset;
    if (remainder <= last_length) {
      length = last_length;
    }
    int val = offset % segment_length;
    int actual = rtl[i];
    if (val == (length - 1) && actual != -1) {
      rc = 1;
      printf("ERROR: Segmented_exscanv rank=%d rtl[%d]=%d, expected -1 @ %s:%d\n", rank, i, actual, __FILE__, __LINE__);
    } else if (val < (length - 1) && actual != (length - val - 1)) {
      rc = 1;
      printf("ERROR: Segmented_exscanv rank=%d rtl[%d]=%d, expected %d @ %s:%d\n", rank, i, actual, length - val - 1, __FILE__, __LINE__);
    }
  }

  MPI_Barrier(MPI_COMM_WORLD);

  free(rtl);
  free(ltr);
  free(vals);
  free(keys);

  return rc;
}

int test_exscanv_even0(int count, int segment_length)
{
  int rc = 0;

  int rank, ranks;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &ranks);

  int* keys = (int*) malloc(count * sizeof(int));
  int* vals = (int*) malloc(count * sizeof(int));
  int* ltr  = (int*) malloc(count * sizeof(int));
  int* rtl  = (int*) malloc(count * sizeof(int));

  int i;
  for (i = 0; i < count; i++) {
    keys[i] = (rank/2 * count + i) / segment_length;
    vals[i] = 1;
    ltr[i] = -1;
    rtl[i] = -1;
  }

  // set input count for even ranks to be 0
  int in_count = count;
  if (rank % 2 == 0) {
    in_count = 0;
  }

  DTCMP_Segmented_exscanv(
    in_count, keys, MPI_INT, DTCMP_OP_INT_ASCEND,
    vals, ltr, rtl, MPI_INT, MPI_SUM,
    DTCMP_FLAG_NONE, MPI_COMM_WORLD
  );

#if 0
  printf("exscanv ltr: %d ", rank);
  for (i = 0; i < count; i++) {
    printf("(%d,%d)=%d, ", keys[i], vals[i], ltr[i]);
  }
  printf("\n");

  printf("exscanv rtl: %d ", rank);
  for (i = 0; i < count; i++) {
    printf("(%d, %d)=%d, ", keys[i], vals[i], rtl[i]);
  }
  printf("\n");
#endif

  if (rank % 2 == 0) {
    // even ranks, output buffers should not be changed
    for (i = 0; i < count; i++) {
      if (ltr[i] != -1) {
        rc = 1;
        printf("ERROR: Segmented_exscanv rank=%d ltr[%d]=%d, expected -1 @ %s:%d\n", rank, i, ltr[i], __FILE__, __LINE__);
      }
      if (rtl[i] != -1) {
        rc = 1;
        printf("ERROR: Segmented_exscanv rank=%d rtl[%d]=%d, expected -1 @ %s:%d\n", rank, i, rtl[i], __FILE__, __LINE__);
      }
    }
  } else {
    // odd ranks

    // check ltr results
    for (i = 0; i < count; i++) {
      int val = (rank/2 * count + i) % segment_length;
      int actual = ltr[i];
      if (val == 0 && actual != -1) {
        rc = 1;
        printf("ERROR: Segmented_exscanv rank=%d ltr[%d]=%d, expected -1 @ %s:%d\n", rank, i, actual, __FILE__, __LINE__);
      } else if (val > 0 && actual != val) {
        rc = 1;
        printf("ERROR: Segmented_exscanv rank=%d ltr[%d]=%d, expected -1 @ %s:%d\n", rank, i, actual, val, __FILE__, __LINE__);
      }
    }

    // check rtl results
    int last_length = (ranks/2 * count) % segment_length;
    int length = segment_length;
    for (i = 0; i < count; i++) {
      // if we're in the last segment, it may not be full length
      int offset = rank/2 * count + i;
      int remainder = ranks/2 * count - offset;
      if (remainder <= last_length) {
        length = last_length;
      }
      int val = offset % segment_length;
      int actual = rtl[i];
      if (val == (length - 1) && actual != -1) {
        rc = 1;
        printf("ERROR: Segmented_exscanv rank=%d rtl[%d]=%d, expected -1 @ %s:%d\n", rank, i, actual, __FILE__, __LINE__);
      } else if (val < (length - 1) && actual != (length - val - 1)) {
        rc = 1;
        printf("ERROR: Segmented_exscanv rank=%d rtl[%d]=%d, expected %d @ %s:%d\n", rank, i, actual, length - val - 1, __FILE__, __LINE__);
      }
    }
  }

  MPI_Barrier(MPI_COMM_WORLD);

  free(rtl);
  free(ltr);
  free(vals);
  free(keys);

  return rc;
}

int test_exscanv_firsthalf0(int count, int segment_length)
{
  int rc = 0;

  int rank, ranks;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &ranks);

  int* keys = (int*) malloc(count * sizeof(int));
  int* vals = (int*) malloc(count * sizeof(int));
  int* ltr  = (int*) malloc(count * sizeof(int));
  int* rtl  = (int*) malloc(count * sizeof(int));

  int i;

  int rank_offset = ranks / 2;
  int ranks_shifted = ranks - rank_offset;
  int rank_shifted = rank - rank_offset;
  for (i = 0; i < count; i++) {
    if (rank < rank_offset) {
      keys[i] = 0;
    } else {
      keys[i] = (rank_shifted * count + i) / segment_length;
    }
    vals[i] = 1;
    ltr[i] = -1;
    rtl[i] = -1;
  }

  // set input count for even ranks to be 0
  int in_count = count;
  if (rank < rank_offset) {
    in_count = 0;
  }

  DTCMP_Segmented_exscanv(
    in_count, keys, MPI_INT, DTCMP_OP_INT_ASCEND,
    vals, ltr, rtl, MPI_INT, MPI_SUM,
    DTCMP_FLAG_NONE, MPI_COMM_WORLD
  );

#if 0
  printf("exscanv ltr: %d ", rank);
  for (i = 0; i < count; i++) {
    printf("(%d,%d)=%d, ", keys[i], vals[i], ltr[i]);
  }
  printf("\n");

  printf("exscanv rtl: %d ", rank);
  for (i = 0; i < count; i++) {
    printf("(%d, %d)=%d, ", keys[i], vals[i], rtl[i]);
  }
  printf("\n");
#endif

  if (rank < rank_offset) {
    // first half of ranks, output buffers should not be changed
    for (i = 0; i < count; i++) {
      if (ltr[i] != -1) {
        rc = 1;
        printf("ERROR: Segmented_exscanv rank=%d ltr[%d]=%d, expected -1 @ %s:%d\n", rank, i, ltr[i], __FILE__, __LINE__);
      }
      if (rtl[i] != -1) {
        rc = 1;
        printf("ERROR: Segmented_exscanv rank=%d rtl[%d]=%d, expected -1 @ %s:%d\n", rank, i, rtl[i], __FILE__, __LINE__);
      }
    }
  } else {
    // second half of ranks

    // check ltr results
    for (i = 0; i < count; i++) {
      int val = (rank_shifted * count + i) % segment_length;
      int actual = ltr[i];
      if (val == 0 && actual != -1) {
        rc = 1;
        printf("ERROR: Segmented_exscanv rank=%d ltr[%d]=%d, expected -1 @ %s:%d\n", rank, i, actual, __FILE__, __LINE__);
      } else if (val > 0 && actual != val) {
        rc = 1;
        printf("ERROR: Segmented_exscanv rank=%d ltr[%d]=%d, expected -1 @ %s:%d\n", rank, i, actual, val, __FILE__, __LINE__);
      }
    }

    // check rtl results
    int last_length = (ranks_shifted * count) % segment_length;
    int length = segment_length;
    for (i = 0; i < count; i++) {
      // if we're in the last segment, it may not be full length
      int offset = rank_shifted * count + i;
      int remainder = ranks_shifted * count - offset;
      if (remainder <= last_length) {
        length = last_length;
      }
      int val = offset % segment_length;
      int actual = rtl[i];
      if (val == (length - 1) && actual != -1) {
        rc = 1;
        printf("ERROR: Segmented_exscanv rank=%d rtl[%d]=%d, expected -1 @ %s:%d\n", rank, i, actual, __FILE__, __LINE__);
      } else if (val < (length - 1) && actual != (length - val - 1)) {
        rc = 1;
        printf("ERROR: Segmented_exscanv rank=%d rtl[%d]=%d, expected %d @ %s:%d\n", rank, i, actual, length - val - 1, __FILE__, __LINE__);
      }
    }
  }

  MPI_Barrier(MPI_COMM_WORLD);

  free(rtl);
  free(ltr);
  free(vals);
  free(keys);

  return rc;
}

int test_scanv(int count, int segment_length)
{
  int rc = 0;

  int rank, ranks;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &ranks);

  int* keys = (int*) malloc(count * sizeof(int));
  int* vals = (int*) malloc(count * sizeof(int));
  int* ltr  = (int*) malloc(count * sizeof(int));
  int* rtl  = (int*) malloc(count * sizeof(int));

  int i;
  for (i = 0; i < count; i++) {
    keys[i] = (rank*count + i) / segment_length;
    vals[i] = 1;
    ltr[i] = -1;
    rtl[i] = -1;
  }

  DTCMP_Segmented_scanv(
    count, keys, MPI_INT, DTCMP_OP_INT_ASCEND,
    vals, ltr, rtl, MPI_INT, MPI_SUM,
    DTCMP_FLAG_NONE, MPI_COMM_WORLD
  );

#if 0
  printf("scanv ltr: %d ", rank);
  for (i = 0; i < count; i++) {
    printf("(%d,%d)=%d, ", keys[i], vals[i], ltr[i]);
  }
  printf("\n");

  printf("scanv rtl: %d ", rank);
  for (i = 0; i < count; i++) {
    printf("(%d, %d)=%d, ", keys[i], vals[i], rtl[i]);
  }
  printf("\n");
#endif

  // check ltr results
  for (i = 0; i < count; i++) {
    int val = (rank*count + i) % segment_length;
    if (ltr[i] != val + 1) {
      rc = 1;
      printf("ERROR: Segmented_scanv rank=%d ltr[%d]=%d, expected -1 @ %s:%d\n", rank, i, ltr[i], val + 1, __FILE__, __LINE__);
    }
  }

  // check rtl results
  int last_length = (ranks * count) % segment_length;
  int length = segment_length;
  for (i = 0; i < count; i++) {
    // if we're in the last segment, it may not be full length
    int offset = rank * count + i;
    int remainder = ranks * count - offset;
    if (remainder <= last_length) {
      length = last_length;
    }
    int val = offset % segment_length;
    int actual = rtl[i];
    int expected = length - val;
    if (actual != expected) {
      rc = 1;
      printf("ERROR: Segmented_scanv rank=%d rtl[%d]=%d, expected %d @ %s:%d\n", rank, i, actual, expected, __FILE__, __LINE__);
    }
  }

  MPI_Barrier(MPI_COMM_WORLD);

  free(rtl);
  free(ltr);
  free(vals);
  free(keys);

  return rc;
}

int test_exscanv_ltr(int count, int segment_length)
{
  int rc = 0;

  int rank, ranks;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &ranks);

  int* keys = (int*) malloc(count * sizeof(int));
  int* vals = (int*) malloc(count * sizeof(int));
  int* ltr  = (int*) malloc(count * sizeof(int));
  int* rtl  = (int*) malloc(count * sizeof(int));

  int i;
  for (i = 0; i < count; i++) {
    keys[i] = (rank*count + i) / segment_length;
    vals[i] = 1;
    ltr[i] = -1;
    rtl[i] = -1;
  }

  DTCMP_Segmented_exscanv_ltr(
    count, keys, MPI_INT, DTCMP_OP_INT_ASCEND,
    vals, ltr, MPI_INT, MPI_SUM,
    DTCMP_FLAG_NONE, MPI_COMM_WORLD
  );

#if 0
  printf("exscanv_ltr: %d ", rank);
  for (i = 0; i < count; i++) {
    printf("(%d,%d)=%d, ", keys[i], vals[i], ltr[i]);
  }
  printf("\n");
#endif

  MPI_Barrier(MPI_COMM_WORLD);

  free(rtl);
  free(ltr);
  free(vals);
  free(keys);

  return rc;
}

int test_scanv_ltr(int count, int segment_length)
{
  int rc = 0;

  int rank, ranks;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &ranks);

  int* keys = (int*) malloc(count * sizeof(int));
  int* vals = (int*) malloc(count * sizeof(int));
  int* ltr  = (int*) malloc(count * sizeof(int));
  int* rtl  = (int*) malloc(count * sizeof(int));

  int i;
  for (i = 0; i < count; i++) {
    keys[i] = (rank*count + i) / segment_length;
    vals[i] = 1;
    ltr[i] = -1;
    rtl[i] = -1;
  }

  DTCMP_Segmented_scanv_ltr(
    count, keys, MPI_INT, DTCMP_OP_INT_ASCEND,
    vals, ltr, MPI_INT, MPI_SUM,
    DTCMP_FLAG_NONE, MPI_COMM_WORLD
  );

#if 0
  printf("scanv_ltr: %d ", rank);
  for (i = 0; i < count; i++) {
    printf("(%d,%d)=%d, ", keys[i], vals[i], ltr[i]);
  }
  printf("\n");
#endif

  MPI_Barrier(MPI_COMM_WORLD);

  free(rtl);
  free(ltr);
  free(vals);
  free(keys);

  return rc;
}

int test_exscanv_fused(int count, int segment_length)
{
  int rc = 0;

  int rank, ranks;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &ranks);

  int* keys = (int*) malloc(count * 2 * sizeof(int));
  int* ltr  = (int*) malloc(count * sizeof(int));
  int* rtl  = (int*) malloc(count * sizeof(int));

  int i;
  for (i = 0; i < count; i++) {
    keys[i*2] = (rank*count + i) / segment_length; // key
    keys[i*2+1] = 1; // value
    ltr[i] = -1;
    rtl[i] = -1;
  }

  MPI_Datatype keysat;
  MPI_Type_contiguous(2, MPI_INT, &keysat);
  MPI_Type_commit(&keysat);

  DTCMP_Segmented_exscanv_fused(
    count, keys, MPI_INT, keysat, DTCMP_OP_INT_ASCEND,
    ltr, rtl, MPI_INT, MPI_SUM,
    DTCMP_FLAG_NONE, MPI_COMM_WORLD
  );

  // check ltr results
  for (i = 0; i < count; i++) {
    int val = (rank*count + i) % segment_length;
    int actual = ltr[i];
    if (val == 0 && actual != -1) {
      rc = 1;
      printf("ERROR: Segmented_exscanv rank=%d ltr[%d]=%d, expected -1 @ %s:%d\n", rank, i, actual, __FILE__, __LINE__);
    } else if (val > 0 && actual != val) {
      rc = 1;
      printf("ERROR: Segmented_exscanv rank=%d ltr[%d]=%d, expected -1 @ %s:%d\n", rank, i, actual, val, __FILE__, __LINE__);
    }
  }

  // check rtl results
  int last_length = (ranks * count) % segment_length;
  int length = segment_length;
  for (i = 0; i < count; i++) {
    // if we're in the last segment, it may not be full length
    int offset = rank * count + i;
    int remainder = ranks * count - offset;
    if (remainder <= last_length) {
      length = last_length;
    }
    int val = offset % segment_length;
    int actual = rtl[i];
    if (val == (length - 1) && actual != -1) {
      rc = 1;
      printf("ERROR: Segmented_exscanv rank=%d rtl[%d]=%d, expected -1 @ %s:%d\n", rank, i, actual, __FILE__, __LINE__);
    } else if (val < (length - 1) && actual != (length - val - 1)) {
      rc = 1;
      printf("ERROR: Segmented_exscanv rank=%d rtl[%d]=%d, expected %d @ %s:%d\n", rank, i, actual, length - val - 1, __FILE__, __LINE__);
    }
  }

  MPI_Barrier(MPI_COMM_WORLD);

  MPI_Type_free(&keysat);
  free(rtl);
  free(ltr);
  free(keys);

  return rc;
}

int test_scanv_fused(int count, int segment_length)
{
  int rc = 0;

  int rank, ranks;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  MPI_Comm_size(MPI_COMM_WORLD, &ranks);

  int* keys = (int*) malloc(count * 2 * sizeof(int));
  int* vals = (int*) malloc(count * sizeof(int));
  int* ltr  = (int*) malloc(count * sizeof(int));
  int* rtl  = (int*) malloc(count * sizeof(int));

  int i;
  for (i = 0; i < count; i++) {
    keys[i*2] = (rank*count + i) / segment_length; // key
    keys[i*2+1] = 1; // value
    ltr[i] = -1;
    rtl[i] = -1;
  }

  MPI_Datatype keysat;
  MPI_Type_contiguous(2, MPI_INT, &keysat);
  MPI_Type_commit(&keysat);

  DTCMP_Segmented_scanv_fused(
    count, keys, MPI_INT, keysat, DTCMP_OP_INT_ASCEND,
    ltr, rtl, MPI_INT, MPI_SUM,
    DTCMP_FLAG_NONE, MPI_COMM_WORLD
  );

#if 0
  printf("scanv ltr: %d ", rank);
  for (i = 0; i < count; i++) {
    printf("(%d,%d)=%d, ", keys[i], vals[i], ltr[i]);
  }
  printf("\n");

  printf("scanv rtl: %d ", rank);
  for (i = 0; i < count; i++) {
    printf("(%d, %d)=%d, ", keys[i], vals[i], rtl[i]);
  }
  printf("\n");
#endif

  // check ltr results
  for (i = 0; i < count; i++) {
    int val = (rank*count + i) % segment_length;
    if (ltr[i] != val + 1) {
      rc = 1;
      printf("ERROR: Segmented_scanv rank=%d ltr[%d]=%d, expected -1 @ %s:%d\n", rank, i, ltr[i], val + 1, __FILE__, __LINE__);
    }
  }

  // check rtl results
  int last_length = (ranks * count) % segment_length;
  int length = segment_length;
  for (i = 0; i < count; i++) {
    // if we're in the last segment, it may not be full length
    int offset = rank * count + i;
    int remainder = ranks * count - offset;
    if (remainder <= last_length) {
      length = last_length;
    }
    int val = offset % segment_length;
    int actual = rtl[i];
    int expected = length - val;
    if (actual != expected) {
      rc = 1;
      printf("ERROR: Segmented_scanv rank=%d rtl[%d]=%d, expected %d @ %s:%d\n", rank, i, actual, expected, __FILE__, __LINE__);
    }
  }

  MPI_Barrier(MPI_COMM_WORLD);

  MPI_Type_free(&keysat);
  free(rtl);
  free(ltr);
  free(vals);
  free(keys);

  return rc;
}

int main(int argc, char* argv[])
{
  int rc = 0;
  int tmp_rc;

  MPI_Init(&argc, &argv);
  DTCMP_Init();

  // ----------------------------------------
  // Check exscanv
  // ----------------------------------------
  tmp_rc = test_exscanv(10, 7);
  if (tmp_rc != 0) {
    rc = tmp_rc;
  }

  tmp_rc = test_exscanv(1, 7);
  if (tmp_rc != 0) {
    rc = tmp_rc;
  }

  tmp_rc = test_exscanv(10, 1);
  if (tmp_rc != 0) {
    rc = tmp_rc;
  }

  tmp_rc = test_exscanv(1, 1);
  if (tmp_rc != 0) {
    rc = tmp_rc;
  }

  tmp_rc = test_exscanv(0, 1);
  if (tmp_rc != 0) {
    rc = tmp_rc;
  }

  // ----------------------------------------
  // Check exscanv with count=0 on even ranks
  // ----------------------------------------
  tmp_rc = test_exscanv_even0(10, 7);
  if (tmp_rc != 0) {
    rc = tmp_rc;
  }

  tmp_rc = test_exscanv_even0(1, 7);
  if (tmp_rc != 0) {
    rc = tmp_rc;
  }

  tmp_rc = test_exscanv_even0(10, 1);
  if (tmp_rc != 0) {
    rc = tmp_rc;
  }

  tmp_rc = test_exscanv_even0(1, 1);
  if (tmp_rc != 0) {
    rc = tmp_rc;
  }

  tmp_rc = test_exscanv_even0(0, 1);
  if (tmp_rc != 0) {
    rc = tmp_rc;
  }

  // ----------------------------------------
  // Check exscanv with count=0 on first half of ranks
  // ----------------------------------------
  tmp_rc = test_exscanv_firsthalf0(10, 7);
  if (tmp_rc != 0) {
    rc = tmp_rc;
  }

  tmp_rc = test_exscanv_firsthalf0(1, 7);
  if (tmp_rc != 0) {
    rc = tmp_rc;
  }

  tmp_rc = test_exscanv_firsthalf0(10, 1);
  if (tmp_rc != 0) {
    rc = tmp_rc;
  }

  tmp_rc = test_exscanv_firsthalf0(1, 1);
  if (tmp_rc != 0) {
    rc = tmp_rc;
  }

  tmp_rc = test_exscanv_firsthalf0(0, 1);
  if (tmp_rc != 0) {
    rc = tmp_rc;
  }

  // ----------------------------------------
  // Check scanv
  // ----------------------------------------
  tmp_rc = test_scanv(10, 7);
  if (tmp_rc != 0) {
    rc = tmp_rc;
  }

  tmp_rc = test_scanv(1, 7);
  if (tmp_rc != 0) {
    rc = tmp_rc;
  }

  tmp_rc = test_scanv(10, 1);
  if (tmp_rc != 0) {
    rc = tmp_rc;
  }

  tmp_rc = test_scanv(1, 1);
  if (tmp_rc != 0) {
    rc = tmp_rc;
  }

  tmp_rc = test_scanv(0, 1);
  if (tmp_rc != 0) {
    rc = tmp_rc;
  }

  // ----------------------------------------
  // Check that exscanv_ltr works
  // ----------------------------------------
  tmp_rc = test_exscanv_ltr(10, 7);
  if (tmp_rc != 0) {
    rc = tmp_rc;
  }

  // ----------------------------------------
  // Check that scanv_ltr works
  // ----------------------------------------
  tmp_rc = test_scanv_ltr(10, 7);
  if (tmp_rc != 0) {
    rc = tmp_rc;
  }

  // ----------------------------------------
  // Check exscanv_fused
  // ----------------------------------------
  tmp_rc = test_exscanv_fused(10, 7);
  if (tmp_rc != 0) {
    rc = tmp_rc;
  }

  tmp_rc = test_exscanv_fused(1, 7);
  if (tmp_rc != 0) {
    rc = tmp_rc;
  }

  tmp_rc = test_exscanv_fused(10, 1);
  if (tmp_rc != 0) {
    rc = tmp_rc;
  }

  tmp_rc = test_exscanv_fused(1, 1);
  if (tmp_rc != 0) {
    rc = tmp_rc;
  }

  tmp_rc = test_exscanv_fused(0, 1);
  if (tmp_rc != 0) {
    rc = tmp_rc;
  }

  // ----------------------------------------
  // Check scanv_fused
  // ----------------------------------------
  tmp_rc = test_scanv_fused(10, 7);
  if (tmp_rc != 0) {
    rc = tmp_rc;
  }

  tmp_rc = test_scanv_fused(1, 7);
  if (tmp_rc != 0) {
    rc = tmp_rc;
  }

  tmp_rc = test_scanv_fused(10, 1);
  if (tmp_rc != 0) {
    rc = tmp_rc;
  }

  tmp_rc = test_scanv_fused(1, 1);
  if (tmp_rc != 0) {
    rc = tmp_rc;
  }

  tmp_rc = test_scanv_fused(0, 1);
  if (tmp_rc != 0) {
    rc = tmp_rc;
  }

  DTCMP_Finalize();
  MPI_Finalize();

  return rc;
}
