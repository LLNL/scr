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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "mpi.h"
#include "scr_globals.h"

/* Based on "Exascale Algorithms for Generalized MPI_Comm_split",
 * EuroMPI 2011, Adam Moody, Dong H. Ahn, and Bronis R. de Supinkski
 *
 * Executes an MPI_Comm_split operation using bitonic sort, a double
 * inclusive scan to find color boundaries and left and right group
 * neighbors, a recv from ANY_SOURCE, and a barrier, returns the
 * output group as a chain. */

/* compares first int,
 *   - used to sort data back to its originating rank if ANY_SOURCE
 *     isn't to be used */
static int scr_cmp_int(const void* a_void, const void* b_void, size_t offset)
{
  /* get pointers to the integer array, offset is dummy in this case */
  const int* a = (const int*) a_void;
  const int* b = (const int*) b_void;
  if (a[0] != b[0]) {
    if (a[0] > b[0]) {
      return 1;
    }
    return -1;
  }

  /* all values are equal if we make it here */
  return 0;
}

/* compares a string
 *   - used to compare strings after sorting */
static int scr_cmp_str(const void* a, const void* b, size_t offset)
{
  /* compare string values first */
  const char* a_str = (char*) a;
  const char* b_str = (char*) b;
  int rc = strcmp(a_str, b_str);
  if (rc != 0) {
    if (rc > 0) {
      return 1;
    }
    return -1;
  }

  /* all values are equal if we make it here */
  return 0;
}

/* compares a (string,rank) tuple, first by string,
 * then rank which is offset bytes from start of buffer
 *   - used to sort (strink,rank) tuples */
static int scr_cmp_str_int(const void* a, const void* b, size_t offset)
{
  /* compare string values first */
  const char* a_str = (char*) a;
  const char* b_str = (char*) b;
  int rc = strcmp(a_str, b_str);
  if (rc != 0) {
    if (rc > 0) {
      return 1;
    }
    return -1;
  }

  /* then compare int values, stored offset bytes from start of buffer */
  int a_int = *(int*)(a_str + offset);
  int b_int = *(int*)(b_str + offset);
  if (a_int != b_int) {
    if (a_int > b_int) {
      return 1;
    }
    return -1;
  }

  /* all values are equal if we make it here */
  return 0;
}

static int scr_sort_bitonic_merge(
  void* value,
  void* scratch,
  MPI_Datatype type,
  size_t type_size,
  size_t offset,
  int (*compare)(const void*, const void*, size_t),
  int start,
  int num,
  int direction,
  MPI_Comm comm,
  int tag)
{
  if (num > 1) {
    /* get our rank within the group */
    int rank;
    MPI_Comm_rank(comm, &rank);

    /* determine largest power of two that is smaller than num */
    int count = 1;
    int index = 0;
    while (count < num) {
      count <<= 1;
      index++;
    }
    count >>= 1;
    index--;

    /* divide range into two chunks, execute bitonic half-clean step,
     * then recursively merge each half */
    MPI_Status status[2];
    if (rank < start + count) {
      /* we are in the lower half, find a partner in the upper half */
      int dst_rank = rank + count;
      if (dst_rank < start + num) {
        /* exchange data with our partner rank */
        MPI_Sendrecv(
          value,   1, type, dst_rank, tag,
          scratch, 1, type, dst_rank, tag,
          comm, status
        );

        /* select the appropriate value,
         * depedning on the sort direction */
        int cmp = (*compare)(scratch, value, offset);
        if ((direction && cmp < 0) || (!direction && cmp > 0)) {
          /* we keep the value if
           *   direction is ascending and new value is smaller,
           *   direction is descending and new value is greater */
          memcpy(value, scratch, type_size);
        }
      }

      /* recursively merge our half */
      scr_sort_bitonic_merge(
        value, scratch, type, type_size, offset, compare,
        start, count, direction,
        comm, tag
      );
    } else {
      /* we are in the upper half, find a partner in the lower half */
      int dst_rank = rank - count;
      if (dst_rank >= start) {
        /* exchange data with our partner rank */
        MPI_Sendrecv(
          value,   1, type, dst_rank, tag,
          scratch, 1, type, dst_rank, tag,
          comm, status
        );

        /* select the appropriate value,
         * depedning on the sort direction */
        int cmp = (*compare)(scratch, value, offset);
        if ((direction && cmp > 0) || (!direction && cmp < 0)) {
          /* we keep the value if
           *   direction is ascending and new value is bigger,
           *   direction is descending and new value is smaller */
          memcpy(value, scratch, type_size);
        }
      }

      /* recursively merge our half */
      int new_start = start + count;
      int new_num   = num - count;
      scr_sort_bitonic_merge(
        value, scratch, type, type_size, offset, compare,
        new_start, new_num, direction,
        comm, tag
      );
    }
  }

  return 0;
}

static int scr_sort_bitonic_sort(
  void* value,
  void* scratch,
  MPI_Datatype type,
  size_t type_size,
  size_t offset,
  int (*compare)(const void*, const void*, size_t),
  int start,
  int num,
  int direction,
  MPI_Comm comm,
  int tag)
{
  if (num > 1) {
    /* get our rank in our group */
    int rank;
    MPI_Comm_rank(comm, &rank);

    /* recursively divide and sort each half */
    int mid = num / 2;
    if (rank < start + mid) {
      /* sort first half in one direction */
      scr_sort_bitonic_sort(
        value, scratch, type, type_size, offset, compare,
        start, mid, !direction,
        comm, tag
      );
    } else {
      /* sort the second half in the other direction */
      int new_start = start + mid;
      int new_num   = num - mid;
      scr_sort_bitonic_sort(
        value, scratch, type, type_size, offset, compare,
        new_start, new_num, direction,
        comm, tag
      );
    }

    /* merge the two sorted halves */
    scr_sort_bitonic_merge(
      value, scratch, type, type_size, offset, compare,
      start, num, direction,
      comm, tag
    );
  }

  return 0;
}

/* globally sort items across processes in group,
 * each process provides its tuple as item on input,
 * on output item is overwritten with a new item
 * such that if rank_i < rank_j, item_i < item_j for all i and j */
static int scr_sort_bitonic(
  void* value,
  MPI_Datatype type,
  size_t type_size,
  size_t data_offset,
  int (*compare)(const void*, const void*, size_t),
  MPI_Comm comm,
  int tag)
{
  /* allocate a scratch buffer to hold received type during sort */
  void* scratch = malloc(type_size);
  if (scratch == NULL) {
    /* TODO: error */
  }

  /* conduct the bitonic sort on our values */
  int ranks;
  MPI_Comm_size(comm, &ranks);
  int rc = scr_sort_bitonic_sort(
    value, scratch, type, type_size, data_offset, compare,
    0, ranks, 1,
    comm, tag
  );

  /* free the buffer */
  if (scratch != NULL) {
    free(scratch);
    scratch = NULL;
  }

  return rc;
}

enum scan_fields {
  SCAN_COLOR = 0, /* running count of number of groups */
  SCAN_FLAG  = 1, /* set flag to 1 when we should stop accumulating */
  SCAN_COUNT = 2, /* running count of ranks within segmented group */
  SCAN_NEXT  = 3, /* address of next process to talk to */
};

enum chain_fields {
  CHAIN_SRC   = 0, /* address of originating rank */
  CHAIN_LEFT  = 1, /* address of left rank */
  CHAIN_RIGHT = 2, /* address of right rank */
  CHAIN_RANK  = 3, /* rank of originating process within its new group */
  CHAIN_SIZE  = 4, /* size of new group */
  CHAIN_ID    = 5, /* id of new group */
  CHAIN_COUNT = 6, /* number of new groups */
};

/* assumes that color/key/rank tuples have been globally sorted
 * across ranks of in chain, computes corresponding group
 * information for val and passes that back to originating rank:
 *   1) determines group boundaries and left and right neighbors
 *      by sending pt2pt msgs to left and right neighbors and
 *      comparing color values
 *   2) executes left-to-right and right-to-left (double) inclusive
 *      segmented scan to compute number of ranks to left and right
 *      sides of host value */
static int scr_split_sorted(
  const void* value,
  MPI_Datatype type,
  size_t type_size,
  size_t data_offset,
  int (*compare)(const void*, const void*, size_t),
  MPI_Comm comm,
  int tag1,
  int tag2,
  int recv_ints[7])
{
  int k;
  MPI_Request request[4];
  MPI_Status  status[4];

  /* we will fill in 7 integer values (src, left, right, rank, size, groupid, groups)
   * representing the chain data structure for the the globally
   * ordered color/key/rank tuple that we hold, which we'll later
   * send back to the rank that contributed our item */
  int send_ints[7];

  /* record address of process that contributed this item */
  int* orig_ptr = (int*)((char*)value + data_offset);
  send_ints[CHAIN_SRC] = *orig_ptr;

  /* allocate a scratch buffer to receive value from left neighbor */
  void* left_buf = malloc(type_size);
  if (left_buf == NULL) {
    /* TODO: error */
  }

  /* allocate a scratch buffer to receive value from right neighbor */
  void* right_buf = malloc(type_size);
  if (right_buf == NULL) {
    /* TODO: error */
  }

  /* get our rank in communicator */
  int rank, ranks;
  MPI_Comm_rank(comm, &rank);
  MPI_Comm_size(comm, &ranks);

  /* set left rank */
  int left_rank = rank - 1;
  if (left_rank < 0) {
    left_rank = MPI_PROC_NULL;
  }

  /* set right rank */
  int right_rank = rank + 1;
  if (right_rank >= ranks) {
    right_rank = MPI_PROC_NULL;
  }

  /* exchange data with left and right neighbors to find
   * boundaries of group */
  k = 0;
  if (left_rank != MPI_PROC_NULL) {
    MPI_Isend(
      (void*)value, 1, type, left_rank, tag1, comm, &request[k]
    );
    k++;

    MPI_Irecv(
      left_buf, 1, type, left_rank, tag1, comm, &request[k]
    );
    k++;
  }
  if (right_rank != MPI_PROC_NULL) {
    MPI_Isend(
      (void*)value, 1, type, right_rank, tag1, comm, &request[k]
    );
    k++;

    MPI_Irecv(
      right_buf, 1, type, right_rank, tag1, comm, &request[k]
    );
    k++;
  }
  if (k > 0) {
    MPI_Waitall(k, request, status);
  }

  /* if we have a left neighbor, and if his color value matches ours,
   * then our element is part of his group, otherwise we are the first
   * rank of a new group */
  int first_in_group = 1;
  send_ints[CHAIN_LEFT] = MPI_PROC_NULL;
  if (left_rank != MPI_PROC_NULL) {
    int left_cmp = (*compare)(left_buf, value, 0);
    if (left_cmp == 0) {
      /* we are not the first in the group,
       * record the rank of the item from our left neighbor */
      first_in_group = 0;
      int* left_ptr = (int*)((char*)left_buf + data_offset);
      send_ints[CHAIN_LEFT] = *left_ptr;
    }
  }

  /* if we have a right neighbor, and if his color value matches ours,
   * then our element is part of his group, otherwise we are the last
   * rank of our group */
  int last_in_group = 1;
  send_ints[CHAIN_RIGHT] = MPI_PROC_NULL;
  if (right_rank != MPI_PROC_NULL) {
    int right_cmp = (*compare)(right_buf, value, 0);
    if (right_cmp == 0) {
      /* we are not the last in our group,
       * record the rank of the item from our right neighbor */
      last_in_group = 0;
      int* right_ptr = (int*)((char*)right_buf + data_offset);
      send_ints[CHAIN_RIGHT] = *right_ptr;
    }
  }

  /* prepare buffers for our scan operations:
   * group count, flag, rank count, next proc */
  int send_left_ints[4]  = {0,0,1,MPI_PROC_NULL};
  int send_right_ints[4] = {0,0,1,MPI_PROC_NULL};
  int recv_left_ints[4]  = {0,0,0,MPI_PROC_NULL};
  int recv_right_ints[4] = {0,0,0,MPI_PROC_NULL};
  if (first_in_group) {
    send_right_ints[SCAN_COLOR] = 1;
    send_right_ints[SCAN_FLAG] = 1;
  }
  if (last_in_group) {
    send_left_ints[SCAN_COLOR] = 1;
    send_left_ints[SCAN_FLAG] = 1;
  }

  /* execute inclusive scan in both directions to count number of
   * ranks in our group to our left and right sides */
  while (left_rank != MPI_PROC_NULL || right_rank != MPI_PROC_NULL) {
    /* select our left and right partners for this iteration */
    k = 0;

    /* send and receive data with left partner */
    if (left_rank != MPI_PROC_NULL) {
      MPI_Irecv(
        recv_left_ints, 4, MPI_INT, left_rank, tag1, comm, &request[k]
      );
      k++;

      /* send the rank of our right neighbor to our left,
       * since it will be his right neighbor in the next step */
      send_left_ints[SCAN_NEXT] = right_rank;
      MPI_Isend(
        send_left_ints, 4, MPI_INT, left_rank, tag1, comm, &request[k]
      );
      k++;
    }

    /* send and receive data with right partner */
    if (right_rank != MPI_PROC_NULL) {
      MPI_Irecv(
        recv_right_ints, 4, MPI_INT, right_rank, tag1, comm, &request[k]
      );
      k++;

      /* send the rank of our left neighbor to our right,
       * since it will be his left neighbor in the next step */
      send_right_ints[SCAN_NEXT] = left_rank;
      MPI_Isend(
        send_right_ints, 4, MPI_INT, right_rank, tag1, comm, &request[k]
      );
      k++;
    }

    /* wait for communication to finsih */
    if (k > 0) {
      MPI_Waitall(k, request, status);
    }

    /* reduce data from left partner */
    if (left_rank != MPI_PROC_NULL) {
      /* count the number of groups to our left */
      send_right_ints[SCAN_COLOR] += recv_left_ints[SCAN_COLOR];

      /* continue accumulating the count in our right-going data
       * if our flag has not already been set */
      if (send_right_ints[SCAN_FLAG] != 1) {
        send_right_ints[SCAN_FLAG]   = recv_left_ints[SCAN_FLAG];
        send_right_ints[SCAN_COUNT] += recv_left_ints[SCAN_COUNT];
      }

      /* get the next rank on our left */
      left_rank = recv_left_ints[SCAN_NEXT];
    }

    /* reduce data from right partner */
    if (right_rank != MPI_PROC_NULL) {
      /* count the number of groups to our right */
      send_left_ints[SCAN_COLOR] += recv_right_ints[SCAN_COLOR];

      /* continue accumulating the count in our left-going data
       * if our flag has not already been set */
      if (send_left_ints[SCAN_FLAG] != 1) {
        send_left_ints[SCAN_FLAG]   = recv_right_ints[SCAN_FLAG];
        send_left_ints[SCAN_COUNT] += recv_right_ints[SCAN_COUNT];
      }

      /* get the next rank on our right */
      right_rank = recv_right_ints[SCAN_NEXT];
    }
  }

  /* Now we can set our rank and the number of ranks in our group.
   * At this point, our right-going count is the number of ranks to our
   * left including ourself, and the left-going count is the number of
   * ranks to our right including ourself.
   * Our rank is the number of ranks to our left (right-going count
   * minus 1), and the group size is the sum of right-going and
   * left-going counts minus 1 so we don't double counts ourself. */
  send_ints[CHAIN_RANK] = send_right_ints[SCAN_COUNT] - 1;
  send_ints[CHAIN_SIZE] = send_right_ints[SCAN_COUNT] + 
                          send_left_ints[SCAN_COUNT] - 1;
  send_ints[CHAIN_ID]    = send_right_ints[SCAN_COLOR] - 1;
  send_ints[CHAIN_COUNT] = send_right_ints[SCAN_COLOR] +
                           send_left_ints[SCAN_COLOR] - 1;

  /* send group info back to originating rank */
#ifdef SCR_USE_ANYSOURCE
  /* send group info back to originating rank,
   * receive our own from someone else
   * (don't know who so use ANY_SOURCE) */
  MPI_Isend(
    send_ints, 7, MPI_INT, send_ints[CHAIN_SRC], tag2,
    comm, &request[0]
  );
  MPI_Irecv(
    recv_ints, 7, MPI_INT, MPI_ANY_SOURCE, tag2,
    comm, &request[1]
  );
  MPI_Waitall(2, request, status);

  /* barrier to ensure processes don't send another
   * message with tag2 until all have been received */
  MPI_Barrier(comm);
#else
  /* if we can't use MPI_ANY_SOURCE, then sort item back to its
   * destination */
  MPI_Datatype result_type;
  MPI_Type_contiguous(7, MPI_INT, &result_type);
  MPI_Type_commit(&result_type);
  size_t result_type_size = 7 * sizeof(int);

  scr_sort_bitonic(
    send_ints, result_type, result_type_size, 0, scr_cmp_int,
    comm, tag1
  );
  memcpy(recv_ints, send_ints, result_type_size);

  MPI_Type_free(&result_type);
#endif

  return SCR_SUCCESS;
}

/* int scr_rank_str(MPI_Comm comm, const void* str, int* groups, int* groupid)
 *   IN  comm    - input communicator (handle)
 *   IN  str     - string (NUL-terminated string)
 *   OUT groups  - number of unique strings in comm (non-negative integer)
 *   OUT groupid - id for specified string (non-negative integer)
 *
 * Given an arbitrary-length string on each process, return the number
 * of unique strings, and assign a unique id to each.
 *
 * rank str    groups groupid
 * 0    hello  2      0
 * 1    world  2      1
 * 2    world  2      1
 * 3    world  2      1
 * 4    hello  2      0
 * 5    world  2      1
 * 6    hello  2      0
 * 7    hello  2      0
 *
 * This function computes the total number of unique strings
 * when taking the union of the strings from all processes in comm.
 * Each string is assigned a unique id from 0 to M-1 in groupid,
 * where M is the number of unique strings.
 * The groupid value is the same on two different processes
 * if and only if both processes specify the same string.
 * This groupid can be used as a color value in MPI_COMM_SPLIT. */
int scr_rank_str(MPI_Comm comm, const char* str, int* groups, int* groupid)
{
  int tag1 = 0;
  int tag2 = 1;

  /* require str not be NULL */
  if (str == NULL) {
    /* TODO: error */
  }

  /* get maximum str length */
  int max_len;
  int len = strlen(str) + 1;
  MPI_Allreduce(&len, &max_len, 1, MPI_INT, MPI_MAX, comm);

  /* allocate space to hold a copy of the string (plus rank) */
  size_t buf_size = max_len + sizeof(int);
  void* buf = malloc(buf_size);
  if (buf == NULL) {
    /* TODO: error */
  }

  /* TODO: if we want items with equal strings to be ordered by rank
   * in comm, then we need to include chain.group_rank as second key */

  /* get our rank in comm */
  int rank;
  MPI_Comm_rank(comm, &rank);

  /* Prepare buffer, copy in string and then rank after max_len characters.
   * This rank serves two purposes. First by sorting on string and then rank,
   * it ensures that every item is unique since the ranks are distinct.
   * Second, it is used as a return address to send the result back. */
  strcpy((char*)buf, str);
  int* ptr = (int*) ((char*)buf + max_len);
  *ptr = rank;

  /* create MPI datatype of offset chars followed by one int */
  MPI_Datatype type;
  int blocklens[2]      = {max_len, 1};
  MPI_Aint displs[2]    = {0, max_len};
  MPI_Datatype types[2] = {MPI_CHAR, MPI_INT};
#if MPI_VERSION >=2
  MPI_Type_create_struct(2, blocklens, displs, types, &type);
#else
  MPI_Type_struct(2, blocklens, displs, types, &type);
#endif
  MPI_Type_commit(&type);

  /* compute type size and offset to original rank */
  size_t type_size = max_len + sizeof(int);
  size_t data_offset = max_len;

  /* sort our values using bitonic sort algorithm -- 
   * O(log^2 N) communication */
  scr_sort_bitonic(
    buf, type, type_size, data_offset, scr_cmp_str_int,
    comm, tag1
  );

  /* now split our sorted values by comparing our value with our
   * left and right neighbors to determine group boundaries --
   * O(log N) communication */
  int recv_ints[7];
  scr_split_sorted(
    buf, type, type_size, data_offset, scr_cmp_str, 
    comm, tag1, tag2, recv_ints
  );

  /* fill in group info */
  *groups  = recv_ints[CHAIN_COUNT];
  *groupid = recv_ints[CHAIN_ID];

  /* free MPI datatype */
  MPI_Type_free(&type);

  /* free memory allocated for buffer */
  if (buf != NULL) {
    free(buf);
    buf = NULL;
  }

  return SCR_SUCCESS;
}
