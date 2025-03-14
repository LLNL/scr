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

#define DETECT_NEXT  (0)
#define DETECT_VALID (1)

#define ASSIGN_FLAG   (0)
#define ASSIGN_GROUPS (1)
#define ASSIGN_RANKS  (2)
#define ASSIGN_NEXT   (3)

/* computes first-in-group and last-in-group flags for each local item,
 * disregarding first-in-group for first item and last-in-group for
 * last item since those can be affected by items on other procs,
 * we do this separately from detect_edges to (eventually) reuse code
 * for Rank_local */
static int detect_edges_interior(
  const void* buf,
  int num,
  MPI_Datatype item,
  DTCMP_Op cmp,
  DTCMP_Flags hints,
  int first_in_group[],
  int last_in_group[])
{
  /* get true extent of item */
  MPI_Aint lb, extent;
  MPI_Type_get_extent(item, &lb, &extent);

  /* we can directly compute the edges for all elements but the endpoints */
  int i;
  const char* left_data  = (const char*)buf;
  const char* right_data = (const char*)buf + extent;
  for (i = 1; i < num; i++) {
    int result = dtcmp_op_eval(left_data, right_data, cmp);
    if (result != 0) {
      /* the left item is different from the right item,
       * mark the right item as the leader of a new group,
       * and mark the left as the last of its group */
      first_in_group[i]  = 1;
      last_in_group[i-1] = 1;
    } else {
      /* the items are equal, so the right is not the start of a group,
       * nor is the left the end of a group */
      first_in_group[i]  = 0;
      last_in_group[i-1] = 0;
    }

    /* point to the next element */
    left_data = right_data;
    right_data += extent;
  }

  return DTCMP_SUCCESS;
}

static int detect_edges(
  const void* buf,
  int num,
  MPI_Datatype item,
  DTCMP_Op cmp,
  DTCMP_Flags hints,
  int first_in_group[],
  int last_in_group[],
  MPI_Comm comm)
{
  /* if every process has at least one item, we'd just need to send values
   * to the ranks to our left and right sides O(1), but since
   * some ranks may not have any items, we must execute a double scan
   * instead O(log N) */

  /* get our rank and number of ranks in the communicator */
  int rank, ranks;
  MPI_Comm_rank(comm, &rank);
  MPI_Comm_size(comm, &ranks);

  /* get extent of item */
  MPI_Aint lb, extent;
  MPI_Type_get_extent(item, &lb, &extent);

  /* get true extent of item */
  MPI_Aint true_lb, true_extent;
  MPI_Type_get_true_extent(item, &true_lb, &true_extent);

  /* compute the size of the scan item, we'll send the rank the
   * process should exchange data with in the next round,
   * a flag to indicate whether the item is valid, and the item itself */
  size_t scan_buf_size = 2 * sizeof(int) + true_extent;

  /* build type for exchange */
  MPI_Datatype type_item;
  MPI_Datatype types[3];
  types[0] = MPI_INT;
  types[1] = MPI_INT;
  types[2] = item;
  dtcmp_type_concat(3, types, &type_item);

  /* declare pointers to our scan buffers */
  char* recv_left_buf  = dtcmp_malloc(scan_buf_size, 0, __FILE__, __LINE__);
  char* recv_right_buf = dtcmp_malloc(scan_buf_size, 0, __FILE__, __LINE__);
  char* send_left_buf  = dtcmp_malloc(scan_buf_size, 0, __FILE__, __LINE__);
  char* send_right_buf = dtcmp_malloc(scan_buf_size, 0, __FILE__, __LINE__);

  /* we can directly compute the edges for all elements but the endpoints */
  detect_edges_interior(buf, num, item, cmp, hints, first_in_group, last_in_group);

  /* to compute the edges for our endpoints we use left-to-right and
   * right-to-left scan operations -- we can't just do point-to-point
   * with our left and right neighbors because our left and/or right
   * neighbor may not have any elements */

  /* for the scan element, we specify:
   *   (int)  a flag indicating whether the element value is valid,
   *   (size) a copy of the element
   * Note that the next rank with the element and the next rank for
   * communication may be different since some ranks may not have a value. */

  /* to compute the leader flag for the leftmost element,
   * we execute a left-to-right scan */
  int* send_left_ints  = (int*) (send_left_buf);
  int* send_right_ints = (int*) (send_right_buf);
  int* recv_left_ints  = (int*) (recv_left_buf);
  int* recv_right_ints = (int*) (recv_right_buf);

  /* get pointers to first and last items in buffer */
  char* buf_first = (char*)buf;
  char* buf_last  = (char*)buf + (num-1) * extent;

  /* copy the value from our rightmost element into our left-to-right
   * scan buffer assume that we don't have a valid value */
  send_left_ints[DETECT_VALID]  = 0;
  send_right_ints[DETECT_VALID] = 0;
  if (true_extent > 0 && num > 0) {
    /* copy value from our leftmost element into right-to-left scan buffer */
    send_left_ints[DETECT_VALID] = 1;
    DTCMP_Memcpy(send_left_buf + 2 * sizeof(int), 1, item, buf_first, 1, item);

    /* copy value from our rightmost element into left-to-right scan buffer */
    send_right_ints[DETECT_VALID] = 1;
    DTCMP_Memcpy(send_right_buf + 2 * sizeof(int), 1, item, buf_last, 1, item);
  }

  int left_rank = rank - 1;
  if (left_rank < 0) {
    left_rank = MPI_PROC_NULL;
  }
  int right_rank = rank + 1;
  if (right_rank >= ranks) {
    right_rank = MPI_PROC_NULL;
  }

  /* execute left-to-right and right-to-left scans */
  MPI_Request request[4];
  MPI_Status  status[4];
  int made_left_comparison  = 0;
  int made_right_comparison = 0;
  while (left_rank != MPI_PROC_NULL || right_rank != MPI_PROC_NULL) {
    int k = 0;

    /* if we have a left partner, send it our left-going data
     * and recv its right-going data */
    if (left_rank != MPI_PROC_NULL) {
      /* receive right-going data from the left */
      MPI_Irecv(recv_left_buf, 1, type_item, left_rank, 0, comm, &request[k]);
      k++;

      /* inform rank to our left of the rank on our right,
       * and send it our data */
      send_left_ints[DETECT_NEXT] = right_rank;
      MPI_Isend(send_left_buf, 1, type_item, left_rank, 0, comm, &request[k]);
      k++;
    }

    /* if we have a right partner, send it our right-going data
     * and recv its left-going data */
    if (right_rank != MPI_PROC_NULL) {
      /* receive left-going data from the right */
      MPI_Irecv(recv_right_buf, 1, type_item, right_rank, 0, comm, &request[k]);
      k++;

      /* inform rank to our right of the rank on our left,
       * and send it our data */
      send_right_ints[DETECT_NEXT] = left_rank;
      MPI_Isend(send_right_buf, 1, type_item, right_rank, 0, comm, &request[k]);
      k++;
    }

    /* wait for all communication to complete */
    if (k > 0) {
      MPI_Waitall(k, request, status);
    }

    /* if we have a left partner, merge its data with our right-going data */
    if (left_rank != MPI_PROC_NULL) {
      if (true_extent > 0 && num > 0 && !made_left_comparison && recv_left_ints[DETECT_VALID]) {
        /* compare our leftmost value with the value we receive from the left,
         * if our value is different, mark it as the start of a new group */
        int result = dtcmp_op_eval(recv_left_buf + 2 * sizeof(int), buf_first, cmp);
        if (result != 0) {
          first_in_group[0] = 1;
        } else {
          first_in_group[0] = 0;
        }

        /* after we make this comparison, we don't need to again,
         * we just need to check with the first rank to our left
         * that has a valid value */
        made_left_comparison = 1;
      }

      /* get the next rank to send to on our left */
      left_rank = recv_left_ints[DETECT_NEXT];
    }

    /* if we have a right partner, merge its data with our left-going data */
    if (right_rank != MPI_PROC_NULL) {
      if (true_extent > 0 && num > 0 && !made_right_comparison && recv_right_ints[DETECT_VALID]) {
        /* compare our rightmost value with the value we receive from the right,
         * if our value is different, mark it as the end of a group */
        int result = dtcmp_op_eval(recv_right_buf + 2 * sizeof(int), buf_last, cmp);
        if (result != 0) {
          last_in_group[num-1] = 1;
        } else {
          last_in_group[num-1] = 0;
        }
        made_right_comparison = 1;
      }

      /* get the next rank to send to on our right */
      right_rank = recv_right_ints[DETECT_NEXT];
    }
  }

  /* if we have an element but never made a comparison on one side,
   * our element is the first or last in its group */
  if (num > 0) {
    /* if we never made a comparison on the left,
     * there is no value to our left, so our leftmost item is the first */
    if (! made_left_comparison) {
      first_in_group[0] = 1;
    }

    /* if we never made a comparison on the right, 
     * there is no value to our right, so our rightmost item is the last */
    if (! made_right_comparison) {
      last_in_group[num-1] = 1;
    }
  }

  /* free our temporary data */
  dtcmp_free(&send_right_buf);
  dtcmp_free(&send_left_buf);
  dtcmp_free(&recv_right_buf);
  dtcmp_free(&recv_left_buf);

  MPI_Type_free(&type_item);

  return DTCMP_SUCCESS;
}

static int assign_ids(
  const int leading[],
  int num,
  uint64_t groups_and_ranks[],
  uint64_t* num_groups,
  MPI_Comm comm)
{
  int i;

  /* get our rank and number of ranks in the communicator */
  int rank, ranks;
  MPI_Comm_rank(comm, &rank);
  MPI_Comm_size(comm, &ranks);

  /* since we pack integer ranks (including MPI_PROC_NULL, often < 0)
   * into an unsigned 64bit integer, we use 0 to represent NULL and
   * shift all MPI ranks up by one */
  uint64_t SHIFTED_MPI_PROC_NULL = 0;

  /* declare buffers for our scan operations */
  uint64_t send_left_ints[4]   = {0,0,0,SHIFTED_MPI_PROC_NULL};
  uint64_t send_right_ints[4]  = {0,0,0,SHIFTED_MPI_PROC_NULL};
  uint64_t recv_left_ints[4]   = {0,0,0,SHIFTED_MPI_PROC_NULL};
  uint64_t recv_right_ints[4]  = {0,0,0,SHIFTED_MPI_PROC_NULL};
  uint64_t total_left_ints[4]  = {0,0,0,SHIFTED_MPI_PROC_NULL};
  uint64_t total_right_ints[4] = {0,0,0,SHIFTED_MPI_PROC_NULL};

  /* allocate arrays to track number of groups and ranks
   * for each of our elements */
  int* counts_left  = dtcmp_malloc(2 * sizeof(uint64_t) * num, 0, __FILE__, __LINE__);
  int* counts_right = dtcmp_malloc(2 * sizeof(uint64_t) * num, 0, __FILE__, __LINE__);

  /* run through the elements we have locally and prepare
   * out left-to-right and right-to-left scan items */
  for (i = 0; i < num; i++) {
    if (leading[i]) {
      send_right_ints[ASSIGN_FLAG] = 1;
      send_right_ints[ASSIGN_GROUPS]++;
      send_right_ints[ASSIGN_RANKS] = 1;
    } else {
      send_right_ints[ASSIGN_RANKS]++;
    }

    if (leading[num-1-i]) {
      /* note that we set ranks to 0 here instead of 1 like above */
      send_left_ints[ASSIGN_FLAG]  = 1;
      send_left_ints[ASSIGN_GROUPS]++;
      send_left_ints[ASSIGN_RANKS] = 0;
    } else {
      send_left_ints[ASSIGN_RANKS]++;
    }
  }

  int left_rank  = rank - 1;
  if (left_rank < 0) {
    left_rank = MPI_PROC_NULL;
  }
  int right_rank = rank + 1;
  if (right_rank >= ranks) {
    right_rank = MPI_PROC_NULL;
  }

  /* execute exclusive scan in both directions to count number
   * of ranks in our group to our left and right sides */
  MPI_Request request[4];
  MPI_Status  status[4];
  while (left_rank != MPI_PROC_NULL || right_rank != MPI_PROC_NULL) {
    /* select our left and right partners for this iteration */
    int k = 0;

    /* send and receive data with left partner */
    if (left_rank != MPI_PROC_NULL) {
      MPI_Irecv(recv_left_ints, 4, MPI_UINT64_T, left_rank, 0, comm, &request[k]);
      k++;

      if (right_rank != MPI_PROC_NULL) {
        send_left_ints[ASSIGN_NEXT] = (uint64_t) right_rank + 1;
      } else {
        send_left_ints[ASSIGN_NEXT] = SHIFTED_MPI_PROC_NULL;
      }
      MPI_Isend(send_left_ints, 4, MPI_UINT64_T, left_rank, 0, comm, &request[k]);
      k++;
    }

    /* send and receive data with right partner */
    if (right_rank != MPI_PROC_NULL) {
      MPI_Irecv(recv_right_ints, 4, MPI_UINT64_T, right_rank, 0, comm, &request[k]);
      k++;

      if (left_rank != MPI_PROC_NULL) {
        send_right_ints[ASSIGN_NEXT] = (uint64_t) left_rank + 1;
      } else {
        send_right_ints[ASSIGN_NEXT] = SHIFTED_MPI_PROC_NULL;
      } 
      MPI_Isend(send_right_ints, 4, MPI_UINT64_T, right_rank, 0, comm, &request[k]);
      k++;
    }

    /* wait for communication to finsih */
    if (k > 0) {
      MPI_Waitall(k, request, status);
    }

    /* reduce data from left partner */
    if (left_rank != MPI_PROC_NULL) {
      /* accumulate totals at left end in left-to-right scan */
      if (total_left_ints[ASSIGN_FLAG] != 1) {
        total_left_ints[ASSIGN_FLAG]   = recv_left_ints[ASSIGN_FLAG];
        total_left_ints[ASSIGN_RANKS] += recv_left_ints[ASSIGN_RANKS];
      }
      total_left_ints[ASSIGN_GROUPS] += recv_left_ints[ASSIGN_GROUPS];

      /* accumulate total at right end in left-to-right scan */
      if (send_right_ints[ASSIGN_FLAG] != 1) {
        send_right_ints[ASSIGN_FLAG]   = recv_left_ints[ASSIGN_FLAG];
        send_right_ints[ASSIGN_RANKS] += recv_left_ints[ASSIGN_RANKS];
      }
      send_right_ints[ASSIGN_GROUPS] += recv_left_ints[ASSIGN_GROUPS];

      /* get the next rank on the left */
      if (recv_left_ints[ASSIGN_NEXT] != SHIFTED_MPI_PROC_NULL) {
        left_rank = (int) recv_left_ints[ASSIGN_NEXT] - 1;
      } else {
        left_rank = MPI_PROC_NULL;
      }
    }

    /* reduce data from right partner */
    if (right_rank != MPI_PROC_NULL) {
      /* accumulate totals at right end in right-to-left scan */
      if (total_right_ints[ASSIGN_FLAG] != 1) {
        total_right_ints[ASSIGN_FLAG]   = recv_right_ints[ASSIGN_FLAG];
        total_right_ints[ASSIGN_RANKS] += recv_right_ints[ASSIGN_RANKS];
      }
      total_right_ints[ASSIGN_GROUPS] += recv_right_ints[ASSIGN_GROUPS];

      /* accumulate total at left end in right-to-left scan */
      if (send_left_ints[ASSIGN_FLAG] != 1) {
        send_left_ints[ASSIGN_FLAG]   = recv_right_ints[ASSIGN_FLAG];
        send_left_ints[ASSIGN_RANKS] += recv_right_ints[ASSIGN_RANKS];
      }
      send_left_ints[ASSIGN_GROUPS] += recv_right_ints[ASSIGN_GROUPS];

      /* get the next rank on the left */
      if (recv_right_ints[ASSIGN_NEXT] != SHIFTED_MPI_PROC_NULL) {
        right_rank = (int) recv_right_ints[ASSIGN_NEXT] - 1;
      } else {
        right_rank = MPI_PROC_NULL;
      }
    }
  }

  if (num > 0) {
    /* now that we have exclusive results for our left and right sides,
     * compute the inclusive results for each of our items */
    for (i = 0; i < num; i++) {
      if (leading[i]) {
        total_left_ints[ASSIGN_GROUPS]++;
        total_left_ints[ASSIGN_RANKS] = 1;
      } else {
        total_left_ints[ASSIGN_RANKS]++;
      }
      counts_left[i*2 + 0] = total_left_ints[ASSIGN_GROUPS];
      counts_left[i*2 + 1] = total_left_ints[ASSIGN_RANKS];

      total_right_ints[ASSIGN_RANKS]++;
      counts_right[(num-1-i)*2 + 0] = total_right_ints[ASSIGN_GROUPS];
      counts_right[(num-1-i)*2 + 1] = total_right_ints[ASSIGN_RANKS];
      if (leading[num-1-i]) {
        total_right_ints[ASSIGN_GROUPS]++;
        total_right_ints[ASSIGN_RANKS] = 0;
      }
    }

    /* now run over each of our values to compute its group id,
     * its rank within its group, the number of ranks in its group,
     * and the number of groups */
    int GAR_GROUP_ID  = 0;
    int GAR_RANK_ID   = 1;
    int GAR_NUM_RANKS = 2;
    uint64_t* gar = groups_and_ranks;
    for (i = 0; i < num; i++) {
        gar[GAR_GROUP_ID]  = counts_left[i*2 + 0] - 1;
        gar[GAR_RANK_ID]   = counts_left[i*2 + 1] - 1;
        gar[GAR_NUM_RANKS] = counts_left[i*2 + 1] + counts_right[i*2 + 1] - 1;
        gar += 3;
    }

    /* compute total number of groups */
    *num_groups = counts_left[0] + counts_right[0];
  } else {
    /* compute total number of groups */
    *num_groups = total_left_ints[ASSIGN_GROUPS] + total_right_ints[ASSIGN_GROUPS];
  }

  /* free our temporary data */
  dtcmp_free(&counts_right);
  dtcmp_free(&counts_left);

  return DTCMP_SUCCESS;
}

/* assigns globally unique rank ids to items in buf,
 * returns number of globally distinct items in groups,
 * and sets group_id, group_ranks, group_rank for each item in buf,
 * group_id[i] is set from 0 to groups-1 and specifies to which group the ith item (in buf) belongs,
 * group_ranks[i] returns the global number of items in that group,
 * group_rank[i] is set from 0 to group_ranks[i]-1 and it specifies the item's rank within its group,
 * with any ties broken first by MPI rank and then by the item's index within buf */
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
  MPI_Comm comm)
{
  int i, tmp_rc;
  int rc = DTCMP_SUCCESS;

  /* The approach here is to build an array of items where each element
   * consists of a (key,rank,index) tuple.  We then globally sort these
   * tuples, first by key, then rank, then index.  After that, we detect
   * edges between elements where the key differs and then execute scans to
   * determine the group information for each item (group_id, group_ranks,
   * and group_rank).  Finally, this info is sent back to the originating
   * rank with another sort on (rank,index) with the group info attached. */

  /* get our rank and number or ranks in communicator */
  int rank, ranks;
  MPI_Comm_rank(comm, &rank);
  MPI_Comm_size(comm, &ranks);

  /* get extent of keysat type so we can step through
   * and copy key value from each one */
  MPI_Aint keysat_lb, keysat_extent;
  MPI_Type_get_extent(keysat, &keysat_lb, &keysat_extent);

  /* get true extent of key type so we can copy them into their own buffers */
  MPI_Aint key_true_lb, key_true_extent;
  MPI_Type_get_true_extent(key, &key_true_lb, &key_true_extent);

  /* build items to be sorted with return address */
  size_t sort_size   = key_true_extent + 2 * sizeof(int);
  size_t return_size = 5 * sizeof(uint64_t);
  char* sortbuf   = dtcmp_malloc(count * sort_size, 0, __FILE__, __LINE__);
  char* returnbuf = dtcmp_malloc(count * return_size, 0, __FILE__, __LINE__);
  int*  first_in_group = dtcmp_malloc(count * sizeof(int), 0, __FILE__, __LINE__);
  int*  last_in_group  = dtcmp_malloc(count * sizeof(int), 0, __FILE__, __LINE__);
  uint64_t* sorted_group_and_ranks = dtcmp_malloc(3 * count * sizeof(uint64_t), 0, __FILE__, __LINE__);

  /* create type to be sorted: key, then rank (int), then index (int) */
  MPI_Datatype type_item;
  MPI_Datatype types[3];
  types[0] = key;
  types[1] = MPI_INT;
  types[2] = MPI_INT;
  dtcmp_type_concat(3, types, &type_item);

  /* create our comparison operation for sorting:
   * sort by key, then rank, then index */
  DTCMP_Op cmp_item;
  DTCMP_Op ops[3];
  ops[0] = cmp;
  ops[1] = DTCMP_OP_INT_ASCEND;
  ops[2] = DTCMP_OP_INT_ASCEND;
  DTCMP_Op_create_series(3, ops, &cmp_item);

  /* copy our items into our temporary buffer for sorting */
  char* sortptr = sortbuf;
  const char* ptr = (char*) buf;
  for (i = 0; i < count; i++) {
    /* copy in key value */
    DTCMP_Memcpy(sortptr, 1, key, ptr, 1, key);
    ptr += keysat_extent;
    sortptr += key_true_extent;

    /* set rank and index for this item */
    int* indicies = (int*) sortptr;
    indicies[0] = rank;
    indicies[1] = i;
    sortptr += 2 * sizeof(int);
  }

  /* sort items */
  tmp_rc = DTCMP_Sortv(
    DTCMP_IN_PLACE, sortbuf, count,
    type_item, type_item, cmp_item, hints, comm
  );
  if (tmp_rc != DTCMP_SUCCESS) {
    rc = tmp_rc;
  }

  /* detect edges across sorted items, note we pass in the full items,
   * which includes (key,rank,index) but we just use the key cmp operation */
  tmp_rc = detect_edges(
    sortbuf, count, type_item, cmp, hints,
    first_in_group, last_in_group, comm
  );
  if (tmp_rc != DTCMP_SUCCESS) {
    rc = tmp_rc;
  }

  /* execute scan to determine group info: total number of groups and
   * group_id, group_ranks, and group_rank for each item */
  uint64_t num_groups;
  tmp_rc = assign_ids(
    first_in_group, count,
    sorted_group_and_ranks, &num_groups, comm
  );
  if (tmp_rc != DTCMP_SUCCESS) {
    rc = tmp_rc;
  }

  /* build types for return items, each item contains
   * rank/index/groupid/grouprank/groupindex,
   * and they're sorted by the leading rank/index pair */
  MPI_Datatype type_2uint64t, type_5uint64t;
  MPI_Type_contiguous(2, MPI_UINT64_T, &type_2uint64t);
  MPI_Type_contiguous(5, MPI_UINT64_T, &type_5uint64t);
  MPI_Type_commit(&type_2uint64t);
  MPI_Type_commit(&type_5uint64t);

  /* build a op to sort by rank then by local index on that rank */
  DTCMP_Op cmp_2uint64t;
  DTCMP_Op_create_series2(DTCMP_OP_UINT64T_ASCEND, DTCMP_OP_UINT64T_ASCEND, &cmp_2uint64t);

  /* build return items */
  sortptr = sortbuf + key_true_extent;
  uint64_t* retptr = (uint64_t*) returnbuf;
  uint64_t* sgar = sorted_group_and_ranks;
  for (i = 0; i < count; i++) {
    /* set the rank and index, which we extract from the sorted items */
    int* indicies = (int*) sortptr;
    retptr[0] = (uint64_t) indicies[0];
    retptr[1] = (uint64_t) indicies[1];

    /* set the groupid/grouprank/groupranks values for each item */
    retptr[2] = sgar[0];
    retptr[3] = sgar[1];
    retptr[4] = sgar[2];

    /* advance pointers to next item */
    sortptr += sort_size;
    sgar   += 3;
    retptr += 5;
  }

  /* TODO: replace this sort with DSDE operation */
  /* sort back to sender */
  tmp_rc = DTCMP_Sortv(
    DTCMP_IN_PLACE, returnbuf, count,
    type_2uint64t, type_5uint64t, cmp_2uint64t, hints, comm
  );
  if (tmp_rc != DTCMP_SUCCESS) {
    rc = tmp_rc;
  }

  /* copy values into output arrays */
  uint64_t* gar = (uint64_t*) returnbuf;
  for (i = 0; i < count; i++) {
    group_id[i]    = gar[2];
    group_rank[i]  = gar[3];
    group_ranks[i] = gar[4];
    gar += 5;
  }

  /* set total number of groups */
  *groups = num_groups;

  dtcmp_free(&sortbuf);
  dtcmp_free(&returnbuf);
  dtcmp_free(&first_in_group);
  dtcmp_free(&last_in_group);
  dtcmp_free(&sorted_group_and_ranks);

  DTCMP_Op_free(&cmp_2uint64t);
  DTCMP_Op_free(&cmp_item);

  MPI_Type_free(&type_2uint64t);
  MPI_Type_free(&type_5uint64t);
  MPI_Type_free(&type_item);

  return rc;
}

int DTCMP_Rankv_strings_sort(
  int count,
  const char* strings[],
  uint64_t* groups,
  uint64_t  group_id[],
  uint64_t  group_ranks[],
  uint64_t  group_rank[],
  DTCMP_Flags hints,
  MPI_Comm comm)
{
  int i;

  /* determine the maximum length of each of our strings */
  int max = 0;
  for (i = 0; i < count; i++) {
    int len = strlen(strings[i]) + 1;
    if (len > max) {
      max = len;
    }
  }

  /* compute the maximum string length across all processors */
  int allmax;
  MPI_Allreduce(&max, &allmax, 1, MPI_INT, MPI_MAX, comm);

  /* bail out with success if no one gave us a string */
  if (allmax == 0) {
    /* for this to be 0, all procs must have count = 0,
     * because if any count > 0, then some process has at least
     * one string, and the input allreduce value of that process
     * must be at least 1 since we take strlen(str) + 1 */
     return DTCMP_SUCCESS;
  }

  /* allocate space to copy our strings into */
  char* buf = dtcmp_malloc(allmax * count, 0, __FILE__, __LINE__);
  memset(buf, 0, allmax * count);

  /* copy each of our strings into the buffer */
  char* str = buf;
  for (i = 0; i < count; i++) {
    strcpy(str, strings[i]);
    str += allmax;
  }

  /* build type to use as key and keysat types */
  MPI_Datatype type_string;
  DTCMP_Op cmp_string;
  DTCMP_Str_create_ascend(allmax, &type_string, &cmp_string);

  /* rank items */
  int rc = DTCMP_Rankv_sort(
    count, buf,
    groups, group_id, group_ranks, group_rank,
    type_string, type_string, cmp_string, hints, comm
  );

  /* free our string types */
  DTCMP_Op_free(&cmp_string);
  MPI_Type_free(&type_string);

  /* free buffer */
  dtcmp_free(&buf);

  return rc;
}
