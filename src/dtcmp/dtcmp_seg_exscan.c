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

#define ASSIGN_VALID  (0)
#define ASSIGN_STOP   (1)
#define ASSIGN_NEXT   (2)

/* Executes a segmented exclusive/inclusive scan on items in buf.
 * Executes specified MPI operation on value data
 * for items whose keys are equal.  Stores result in left-to-right
 * scan in ltrbuf and result of right-to-left scan in rtlbuf.
 * Items must be in sorted order. */
static int DTCMP_Segmented_scan_base(
  int exclusive,
  int count,
  const void* keybuf,
  MPI_Datatype key,
  DTCMP_Op cmp,
  const void* valbuf,
  void* ltrbuf,
  void* rtlbuf,
  MPI_Datatype val,
  MPI_Op op,
  DTCMP_Flags hints,
  MPI_Comm comm)
{
  int i;
  int rc = DTCMP_SUCCESS;

  /* get our rank and number of ranks in the communicator */
  int rank, ranks;
  MPI_Comm_rank(comm, &rank);
  MPI_Comm_size(comm, &ranks);

  /* TODO: The first two fields are just binary flags, so they could be
   * encoded into a bit field to save space.  Also, the next MPI rank
   * could be computed, so it's not necessary to send it. */

  /* build type for exchange */
  MPI_Datatype type_item;
  MPI_Datatype types[5];
  types[0] = MPI_INT; /* whether incoming key/value is valid */
  types[1] = MPI_INT; /* flag indicating whether the start of the current group is set */
  types[2] = MPI_INT; /* next MPI rank to talk to */
  types[3] = key;     /* user key */
  types[4] = val;     /* incoming scan value */
  dtcmp_type_concat(5, types, &type_item);

  /* get extent of key type */
  MPI_Aint key_lb, key_extent;
  MPI_Type_get_extent(key, &key_lb, &key_extent);

  /* get true extent of key type */
  MPI_Aint key_true_lb, key_true_extent;
  MPI_Type_get_true_extent(key, &key_true_lb, &key_true_extent);

  /* get extent of val type */
  MPI_Aint val_lb, val_extent;
  MPI_Type_get_extent(val, &val_lb, &val_extent);

  /* get true extent of val type */
  MPI_Aint val_true_lb, val_true_extent;
  MPI_Type_get_true_extent(val, &val_true_lb, &val_true_extent);

  /* compute the size of the scan item, we'll send a flag indicating
   * whether the value is valid, a flag indicating whether the value
   * is the start of a new group, the next rank a process should exchange
   * data with, and finally a copy of the keysat with current value */
  size_t scan_buf_size = 3 * sizeof(int) + key_true_extent + val_true_extent;

  /* allocate memory for scan buffers */
  void* scan_ltr_recv = dtcmp_malloc(scan_buf_size, 0, __FILE__, __LINE__);
  void* scan_ltr_low  = dtcmp_malloc(scan_buf_size, 0, __FILE__, __LINE__);
  void* scan_ltr_high = dtcmp_malloc(scan_buf_size, 0, __FILE__, __LINE__);
  void* scan_rtl_recv = dtcmp_malloc(scan_buf_size, 0, __FILE__, __LINE__);
  void* scan_rtl_low  = dtcmp_malloc(scan_buf_size, 0, __FILE__, __LINE__);
  void* scan_rtl_high = dtcmp_malloc(scan_buf_size, 0, __FILE__, __LINE__);

  /* we need a temporary to accumulate scan values */
  void* scan_tmp_val = dtcmp_malloc(val_true_extent, 0, __FILE__, __LINE__);

  /* get pointers to int portion of buffers */
  int* scan_ltr_recv_ints = (int*) scan_ltr_recv;
  int* scan_ltr_low_ints  = (int*) scan_ltr_low;
  int* scan_ltr_high_ints = (int*) scan_ltr_high;
  int* scan_rtl_recv_ints = (int*) scan_rtl_recv;
  int* scan_rtl_low_ints  = (int*) scan_rtl_low;
  int* scan_rtl_high_ints = (int*) scan_rtl_high;

  /* get pointers to keysat portion of buffers */
  char* scan_ltr_recv_key = (char*)scan_ltr_recv + 3 * sizeof(int);
  char* scan_ltr_low_key  = (char*)scan_ltr_low  + 3 * sizeof(int);
  char* scan_ltr_high_key = (char*)scan_ltr_high + 3 * sizeof(int);
  char* scan_rtl_recv_key = (char*)scan_rtl_recv + 3 * sizeof(int);
  char* scan_rtl_low_key  = (char*)scan_rtl_low  + 3 * sizeof(int);
  char* scan_rtl_high_key = (char*)scan_rtl_high + 3 * sizeof(int);

  /* TODO: this assumes the satellite datatype starts at the end of the normal
   * extent of the key type */
  /* get pointers to sat portion of buffers */
  char* scan_ltr_recv_val = (char*)scan_ltr_recv + 3 * sizeof(int) + key_true_extent;
  char* scan_ltr_low_val  = (char*)scan_ltr_low  + 3 * sizeof(int) + key_true_extent;
  char* scan_ltr_high_val = (char*)scan_ltr_high + 3 * sizeof(int) + key_true_extent;
  char* scan_rtl_recv_val = (char*)scan_rtl_recv + 3 * sizeof(int) + key_true_extent;
  char* scan_rtl_low_val  = (char*)scan_rtl_low  + 3 * sizeof(int) + key_true_extent;
  char* scan_rtl_high_val = (char*)scan_rtl_high + 3 * sizeof(int) + key_true_extent;

  /* our low values aren't valid unless we receive a valid message */
  scan_ltr_low_ints[ASSIGN_VALID] = 0;
  scan_rtl_low_ints[ASSIGN_VALID] = 0;

  /* our high scan values are valid if we have at least one item */
  scan_ltr_high_ints[ASSIGN_VALID] = 0;
  scan_rtl_high_ints[ASSIGN_VALID] = 0;
  if (count > 0) {
    /* if we have at least one item, our high values are valid */
    scan_ltr_high_ints[ASSIGN_VALID] = 1;
    scan_rtl_high_ints[ASSIGN_VALID] = 1;

    /* copy key of last item into left-to-right scan message */
    char* lastkey = (char*)keybuf + (count - 1) * key_extent;
    DTCMP_Memcpy(scan_ltr_high_key, 1, key, lastkey, 1, key);

    /* copy key of first item into right-to-left scan message */
    char* firstkey = (char*)keybuf;
    DTCMP_Memcpy(scan_rtl_high_key, 1, key, firstkey, 1, key);
  }

  /* we assume we do not start a new group */
  scan_ltr_high_ints[ASSIGN_STOP] = 0;
  scan_rtl_high_ints[ASSIGN_STOP] = 0;

  /* run through the elements we have locally and prepare
   * our left-to-right and right-to-left scan items */

  /* initialize our scan values with our first value, if one exists */
  if (count > 0) {
      char* leftval  = (char*)valbuf;
      char* rightval = (char*)valbuf + (count - 1) * val_extent;
      DTCMP_Memcpy(scan_ltr_high_val, 1, val, leftval,  1, val);
      DTCMP_Memcpy(scan_rtl_high_val, 1, val, rightval, 1, val);
  }

  /* accumulate values in high buffers */
  for (i = 1; i < count; i++) {
    /* determine whether current ltr item is in same group as
     * previous ltr item */
    int first_in_group = 0;
    char* prevkey = (char*)keybuf + (i - 1) * key_extent;
    char* currkey = (char*)keybuf + (i + 0) * key_extent;
    int result = dtcmp_op_eval(prevkey, currkey, cmp);
    if (result != 0) {
      /* the current item is different from the previous item,
       * so the current item is the start of a new group */
      first_in_group = 1;
    }
    
    /* either reset ltr value or accumulate current value
     * with running total depending on whether current item
     * starts a new group or not */
    char* currval = (char*)valbuf + i * val_extent;
    if (first_in_group) {
      /* left edge of new group, set stop flag */
      scan_ltr_high_ints[ASSIGN_STOP] = 1;

      /* reset accumulator with current value */
      DTCMP_Memcpy(scan_ltr_high_val, 1, val, currval, 1, val);
    } else {
      /* item is not the start of a new group,
       * accumulate user's input in high buffer,
       * current = temp + current (keep order) */
      DTCMP_Memcpy(scan_tmp_val, 1, val, currval, 1, val);
      MPI_Reduce_local(scan_ltr_high_val, scan_tmp_val, 1, val, op);
      DTCMP_Memcpy(scan_ltr_high_val, 1, val, scan_tmp_val, 1, val);
    }

    /* determine whether current rtl item is in same group as
     * previous rtl item */
    first_in_group = 0;
    int j = count - 1 - i;
    prevkey = (char*)keybuf + (j + 1) * key_extent;
    currkey = (char*)keybuf + (j + 0) * key_extent;
    result = dtcmp_op_eval(prevkey, currkey, cmp);
    if (result != 0) {
      /* the current item is different from the previous item,
       * so the current item is the start of a new group */
      first_in_group = 1;
    }

    /* either reset rtl value or accumulate current value
     * with running total depending on whether current item
     * starts a new group or not */
    currval = (char*)valbuf + j * val_extent;
    if (first_in_group) {
      /* right edge of new group, set flag */
      scan_rtl_high_ints[ASSIGN_STOP] = 1;

      /* copy user data into high buf */
      DTCMP_Memcpy(scan_rtl_high_val, 1, val, currval, 1, val);
    } else {
      /* item is not the start of a new group */
      /* accumulate user's input in high buffer,
       * current = temp + current (keep order) */
      DTCMP_Memcpy(scan_tmp_val, 1, val, currval, 1, val);
      MPI_Reduce_local(scan_rtl_high_val, scan_tmp_val, 1, val, op);
      DTCMP_Memcpy(scan_rtl_high_val, 1, val, scan_tmp_val, 1, val);
    }
  }

  /* get first rank to our left if we have one */
  int left_rank  = rank - 1;
  if (left_rank < 0) {
    left_rank = MPI_PROC_NULL;
  }

  /* get first rank to our right if we have one */
  int right_rank = rank + 1;
  if (right_rank >= ranks) {
    right_rank = MPI_PROC_NULL;
  }

  /* execute exclusive scan in both directions */
  MPI_Request request[4];
  MPI_Status  status[4];
  while (left_rank != MPI_PROC_NULL || right_rank != MPI_PROC_NULL) {
    /* select our left and right partners for this iteration */
    int k = 0;

    /* send and receive data with left partner */
    if (left_rank != MPI_PROC_NULL) {
      MPI_Irecv(scan_ltr_recv, 1, type_item, left_rank, 0, comm, &request[k]);
      k++;

      scan_rtl_high_ints[ASSIGN_NEXT] = right_rank;
      MPI_Isend(scan_rtl_high, 1, type_item, left_rank, 0, comm, &request[k]);
      k++;
    }

    /* send and receive data with right partner */
    if (right_rank != MPI_PROC_NULL) {
      MPI_Irecv(scan_rtl_recv, 1, type_item, right_rank, 0, comm, &request[k]);
      k++;

      scan_ltr_high_ints[ASSIGN_NEXT] = left_rank;
      MPI_Isend(scan_ltr_high, 1, type_item, right_rank, 0, comm, &request[k]);
      k++;
    }

    /* wait for communication to finsih */
    if (k > 0) {
      MPI_Waitall(k, request, status);
    }

    /* reduce data from left partner */
    if (left_rank != MPI_PROC_NULL) {
      /* check whether data is valid from left side */
      if (scan_ltr_recv_ints[ASSIGN_VALID]) {
        /* we got valid data from the left side, process it */

        /* check whether our current low value is valid */
        if (! scan_ltr_low_ints[ASSIGN_VALID]) {
          /* our current low value is not valid, copy received */
          DTCMP_Memcpy(scan_ltr_low, 1, type_item, scan_ltr_recv, 1, type_item);
        } else {
          /* we have valid data and we got valid data from the left side,
           * check whether we should accumulate into low end of ltr scan */
          if (! scan_ltr_low_ints[ASSIGN_STOP]) {
            /* we've not seen a flag to stop accumulating yet,
             * check whether its key matches ours */
            int result = dtcmp_op_eval(scan_ltr_low_key, scan_ltr_recv_key, cmp);
            if (result != 0) {
              /* keys differ, we are first item of a group */
              scan_ltr_low_ints[ASSIGN_STOP] = 1;
            }

            /* if we still need to accumulate, add the value */
            if (! scan_ltr_low_ints[ASSIGN_STOP]) {
              /* accumulate received value into ours */
              MPI_Reduce_local(scan_ltr_recv_val, scan_ltr_low_val, 1, val, op);

              /* set our stop bit according to received data */
              scan_ltr_low_ints[ASSIGN_STOP] = scan_ltr_recv_ints[ASSIGN_STOP];
            }
          }
        }

        /* check whether our current high value is valid */
        if (! scan_ltr_high_ints[ASSIGN_VALID]) {
          /* our current high value is not valid, copy received */
          DTCMP_Memcpy(scan_ltr_high, 1, type_item, scan_ltr_recv, 1, type_item);
        } else {
          /* we have valid data and we got valid data from the left side,
           * check whether we should accumulate into high end of ltr scan */
          if (! scan_ltr_high_ints[ASSIGN_STOP]) {
            /* we've not seen a flag to stop accumulating yet,
             * check whether its key matches ours */
            int result = dtcmp_op_eval(scan_ltr_high_key, scan_ltr_recv_key, cmp);
            if (result != 0) {
              /* keys differ, we are first item of a group */
              scan_ltr_high_ints[ASSIGN_STOP] = 1;
            }

            /* if we still need to accumulate, add the value */
            if (! scan_ltr_high_ints[ASSIGN_STOP]) {
              /* accumulate received value into ours */
              MPI_Reduce_local(scan_ltr_recv_val, scan_ltr_high_val, 1, val, op);

              /* set our stop bit according to received data */
              scan_ltr_high_ints[ASSIGN_STOP] = scan_ltr_recv_ints[ASSIGN_STOP];
            }
          }
        }
      }

      /* get the next rank on the left */
      left_rank = scan_ltr_recv_ints[ASSIGN_NEXT];
    }

    /* reduce data from right partner */
    if (right_rank != MPI_PROC_NULL) {
      /* check whether data is valid from right side */
      if (scan_rtl_recv_ints[ASSIGN_VALID]) {
        /* we got valid data from the right side, process it */

        /* check whether our current low value is valid */
        if (! scan_rtl_low_ints[ASSIGN_VALID]) {
          /* our current low value is not valid, copy received */
          DTCMP_Memcpy(scan_rtl_low, 1, type_item, scan_rtl_recv, 1, type_item);
        } else {
          /* we have valid data and we got valid data from the right side,
           * check whether we should accumulate into low end of rtl scan */
          if (! scan_rtl_low_ints[ASSIGN_STOP]) {
            /* we've not seen a flag to stop accumulating yet,
             * check whether its key matches ours */
            int result = dtcmp_op_eval(scan_rtl_low_key, scan_rtl_recv_key, cmp);
            if (result != 0) {
              /* keys differ, we are first item of a group */
              scan_rtl_low_ints[ASSIGN_STOP] = 1;
            }

            /* if we still need to accumulate, add the value */
            if (! scan_rtl_low_ints[ASSIGN_STOP]) {
              /* accumulate received value into ours */
              MPI_Reduce_local(scan_rtl_recv_val, scan_rtl_low_val, 1, val, op);

              /* set our stop bit according to received data */
              scan_rtl_low_ints[ASSIGN_STOP] = scan_rtl_recv_ints[ASSIGN_STOP];
            }
          }
        }

        /* check whether our current high value is valid */
        if (! scan_rtl_high_ints[ASSIGN_VALID]) {
          /* our current high value is not valid, copy received */
          DTCMP_Memcpy(scan_rtl_high, 1, type_item, scan_rtl_recv, 1, type_item);
        } else {
          /* we have valid data and we got valid data from the right side,
           * check whether we should accumulate into high end of rtl scan */
          if (! scan_rtl_high_ints[ASSIGN_STOP]) {
            /* we've not seen a flag to stop accumulating yet,
             * check whether its key matches ours */
            int result = dtcmp_op_eval(scan_rtl_high_key, scan_rtl_recv_key, cmp);
            if (result != 0) {
              /* keys differ, we are first item of a group */
              scan_rtl_high_ints[ASSIGN_STOP] = 1;
            }

            /* if we still need to accumulate, add the value */
            if (! scan_rtl_high_ints[ASSIGN_STOP]) {
              /* accumulate received value into ours */
              MPI_Reduce_local(scan_rtl_recv_val, scan_rtl_high_val, 1, val, op);

              /* set our stop bit according to received data */
              scan_rtl_high_ints[ASSIGN_STOP] = scan_rtl_recv_ints[ASSIGN_STOP];
            }
          }
        }
      }

      /* get the next rank on the right */
      right_rank = scan_rtl_recv_ints[ASSIGN_NEXT];
    }
  }

  /* if low scan value is valid, initialize our high value */
  if (count > 0) {
    /* if we have a low ltr value, compare our first key to that,
     * otherwise initialize accumulator to our first value */
    int first_in_group = 1;
    if (scan_ltr_low_ints[ASSIGN_VALID]) {
      char* prevkey = scan_ltr_low_key;
      char* currkey = (char*)keybuf;
      int result = dtcmp_op_eval(prevkey, currkey, cmp);
      if (result == 0) {
        /* keys match, so we're not first in the group */
        first_in_group = 0;

        /* for exclusive scan, set first item in output buffer to low value */
        if (exclusive) {
          char* ltrval = (char*)ltrbuf;
          DTCMP_Memcpy(ltrval, 1, val, scan_ltr_low_val, 1, val);
        }
      }
    }

    /* initialize accumulator (use high buffer) */
    char* currval = (char*)valbuf;
    if (first_in_group) {
      /* init our accumulator with user's data if we start a new group */
      DTCMP_Memcpy(scan_ltr_high_val, 1, val, currval, 1, val);
    } else {
      /* otherwise init accumulator with low data */
      DTCMP_Memcpy(scan_ltr_high_val, 1, val, scan_ltr_low_val, 1, val);

      /* add users data to accumulation, be careful with the order here */
      DTCMP_Memcpy(scan_tmp_val, 1, val, currval, 1, val);
      MPI_Reduce_local(scan_ltr_high_val, scan_tmp_val, 1, val, op);
      DTCMP_Memcpy(scan_ltr_high_val, 1, val, scan_tmp_val, 1, val);
    }

    /* for inclusive scan, we include user's input value in output */
    if (! exclusive) {
      char* ltrval = (char*)ltrbuf;
      DTCMP_Memcpy(ltrval, 1, val, scan_ltr_high_val, 1, val);
    }

    /* if we have a low rtl value, compare our first key to that,
     * otherwise initialize accumulator to our first value */
    first_in_group = 1;
    int j = count - 1;
    if (scan_rtl_low_ints[ASSIGN_VALID]) {
      char* prevkey = scan_rtl_low_key;
      char* currkey = (char*)keybuf +  j * key_extent;
      int result = dtcmp_op_eval(prevkey, currkey, cmp);
      if (result == 0) {
        /* keys match, so we're not first in the group */
        first_in_group = 0;

        /* for exclusive scan, set first item in output buffer to low value */
        if (exclusive) {
          char* rtlval = (char*)rtlbuf + j * val_extent;
          DTCMP_Memcpy(rtlval, 1, val, scan_rtl_low_val, 1, val);
        }
      }
    }

    /* initialize accumulator (use high buffer) */
    currval = (char*)valbuf + j * val_extent;
    if (first_in_group) {
      /* init our accumulator with user's data if we start a new group */
      DTCMP_Memcpy(scan_rtl_high_val, 1, val, currval, 1, val);
    } else {
      /* otherwise init accumulator with low data */
      DTCMP_Memcpy(scan_rtl_high_val, 1, val, scan_rtl_low_val, 1, val);

      /* add users data to accumulation, be careful with the order here */
      DTCMP_Memcpy(scan_tmp_val, 1, val, currval, 1, val);
      MPI_Reduce_local(scan_rtl_high_val, scan_tmp_val, 1, val, op);
      DTCMP_Memcpy(scan_rtl_high_val, 1, val, scan_tmp_val, 1, val);
    }

    /* for inclusive scan, we include user's input value in output */
    if (! exclusive) {
      char* rtlval = (char*)rtlbuf + j * val_extent;
      DTCMP_Memcpy(rtlval, 1, val, scan_rtl_high_val, 1, val);
    }
  }

  /* finally set user's output buffers */
  for (i = 1; i < count; i++) {
    /* get pointer to user's value */
    char* currval = (char*)valbuf + i * val_extent;
    char* prevkey = (char*)keybuf + (i - 1) * key_extent;
    char* currkey = (char*)keybuf + i * key_extent;
    int result = dtcmp_op_eval(prevkey, currkey, cmp);
    if (result != 0) {
      /* keys differ, reinit our accumulator with user's data if we start a new group */
      DTCMP_Memcpy(scan_ltr_high_val, 1, val, currval, 1, val);
    } else {
      /* for exclusive scan, copy scan result to user's buffer */
      if (exclusive) {
        char* ltrval = (char*)ltrbuf + i * val_extent;
        DTCMP_Memcpy(ltrval, 1, val, scan_ltr_high_val, 1, val);
      }
      
      /* add users data to accumulation,
       * be careful with the order here */
      DTCMP_Memcpy(scan_tmp_val, 1, val, currval, 1, val);
      MPI_Reduce_local(scan_ltr_high_val, scan_tmp_val, 1, val, op);
      DTCMP_Memcpy(scan_ltr_high_val, 1, val, scan_tmp_val, 1, val);
    }

    /* for inclusive scan, include user's input in output buffer */
    if (! exclusive) {
      char* ltrval = (char*)ltrbuf + i * val_extent;
      DTCMP_Memcpy(ltrval, 1, val, scan_ltr_high_val, 1, val);
    }

    /* get pointer to user's value */
    int j = count - 1 - i;
    currval = (char*)valbuf + j * val_extent;
    prevkey = (char*)keybuf + (j + 1) * key_extent;
    currkey = (char*)keybuf + j * key_extent;
    result = dtcmp_op_eval(prevkey, currkey, cmp);
    if (result != 0) {
      /* keys differ, reinit our accumulator with user's data if we start a new group */
      DTCMP_Memcpy(scan_rtl_high_val, 1, val, currval, 1, val);
    } else {
      /* for exclusive scan, copy scan result to user's buffer */
      if (exclusive) {
        char* rtlval = (char*)rtlbuf + j * val_extent;
        DTCMP_Memcpy(rtlval, 1, val, scan_rtl_high_val, 1, val);
      }
      
      /* add users data to accumulation,
       * be careful with the order here */
      DTCMP_Memcpy(scan_tmp_val, 1, val, currval, 1, val);
      MPI_Reduce_local(scan_rtl_high_val, scan_tmp_val, 1, val, op);
      DTCMP_Memcpy(scan_rtl_high_val, 1, val, scan_tmp_val, 1, val);
    }

    /* for inclusive scan, include user's input in output buffer */
    if (! exclusive) {
      char* rtlval = (char*)rtlbuf + j * val_extent;
      DTCMP_Memcpy(rtlval, 1, val, scan_rtl_high_val, 1, val);
    }
  }

  /* free buffers */
  dtcmp_free(&scan_tmp_val);

  dtcmp_free(&scan_ltr_high);
  dtcmp_free(&scan_ltr_low);
  dtcmp_free(&scan_ltr_recv);
  dtcmp_free(&scan_rtl_high);
  dtcmp_free(&scan_rtl_low);
  dtcmp_free(&scan_rtl_recv);

  MPI_Type_free(&type_item);

  return rc;
}

/* Executes a segmented inclusive scan on items in buf.
 * Executes specified MPI operation on value data
 * for items whose keys are equal.  Stores result in left-to-right
 * scan in outbuf.
 * Items must be in sorted order. */
static int DTCMP_Segmented_scan_ltr_base(
  int exclusive,
  int count,
  const void* keybuf,
  MPI_Datatype key,
  DTCMP_Op cmp,
  const void* valbuf,
  void* outbuf,
  MPI_Datatype val,
  MPI_Op op,
  DTCMP_Flags hints,
  MPI_Comm comm)
{
  /* get true extent of val type */
  MPI_Aint val_true_lb, val_true_extent;
  MPI_Type_get_true_extent(val, &val_true_lb, &val_true_extent);

  /* allocate buffer for right-to-left output */
  size_t bufsize = count * val_true_extent;
  void* rtlbuf = dtcmp_malloc(bufsize, 0, __FILE__, __LINE__); 

  int rc = DTCMP_Segmented_scan_base(exclusive, count, keybuf, key, cmp, valbuf, outbuf, rtlbuf, val, op, hints, comm);

  /* free right-to-left buffer */
  dtcmp_free(&rtlbuf);

  return rc;
}

/* Executes a segmented inclusive scan on items in buf.
 * Executes specified MPI operation on value data
 * for items whose keys are equal.  Stores result in left-to-right
 * scan in outbuf.
 * Items must be in sorted order. */
static int DTCMP_Segmented_scan_fused_base(
  int exclusive,
  int count,
  const void* buf,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  void* ltrbuf,
  void* rtlbuf,
  MPI_Datatype val,
  MPI_Op op,
  DTCMP_Flags hints,
  MPI_Comm comm)
{
  /* get extent of key type */
  MPI_Aint key_lb, key_extent;
  MPI_Type_get_extent(key, &key_lb, &key_extent);

  /* get extent of keysat type */
  MPI_Aint keysat_lb, keysat_extent;
  MPI_Type_get_extent(keysat, &keysat_lb, &keysat_extent);

  /* get extent of val type */
  MPI_Aint val_lb, val_extent;
  MPI_Type_get_extent(val, &val_lb, &val_extent);

  /* get true extent of val type */
  MPI_Aint val_true_lb, val_true_extent;
  MPI_Type_get_true_extent(val, &val_true_lb, &val_true_extent);

  /* allocate space for value buffer */
  size_t bufsize = count * val_true_extent;
  void* valbuf = dtcmp_malloc(bufsize, 0, __FILE__, __LINE__); 

  /* copy stallite portion of keysat from keybuf into valbuf */
  int i;
  char* keyptr = (char*) buf;
  char* valptr = (char*) valbuf;
  for (i = 0; i < count; i++) {
    /* get pointer to satellite data */
    char* ptr = keyptr + key_extent;

    /* copy into value buffer */
    DTCMP_Memcpy((void*)valptr, 1, val, (void*)ptr, 1, val);

    /* advance pointers for next element */
    keyptr += keysat_extent;
    valptr += val_extent;
  }

  int rc = DTCMP_Segmented_scan_base(exclusive, count, buf, keysat, cmp, valbuf, ltrbuf, rtlbuf, val, op, hints, comm);

  /* free right-to-left buffer */
  dtcmp_free(&valbuf);

  return rc;
}

/* Executes a segmented exclusive scan on items in buf.
 * Executes specified MPI operation on value data
 * for items whose keys are equal.  Stores result in left-to-right
 * scan in ltrbuf and result of right-to-left scan in rtlbuf.
 * Items must be in sorted order. */
int DTCMP_Segmented_exscanv(
  int count,
  const void* keybuf,
  MPI_Datatype key,
  DTCMP_Op cmp,
  const void* valbuf,
  void* ltrbuf,
  void* rtlbuf,
  MPI_Datatype val,
  MPI_Op op,
  DTCMP_Flags hints,
  MPI_Comm comm)
{
  return DTCMP_Segmented_scan_base(1, count, keybuf, key, cmp, valbuf, ltrbuf, rtlbuf, val, op, hints, comm);
}

/* Executes a segmented exclusive scan on items in buf.
 * Executes specified MPI operation on value data
 * for items whose keys are equal.  Stores result in left-to-right
 * scan in outbuf.
 * Items must be in sorted order. */
int DTCMP_Segmented_exscanv_ltr(
  int count,
  const void* keybuf,
  MPI_Datatype key,
  DTCMP_Op cmp,
  const void* valbuf,
  void* outbuf,
  MPI_Datatype val,
  MPI_Op op,
  DTCMP_Flags hints,
  MPI_Comm comm)
{
  return DTCMP_Segmented_scan_ltr_base(1, count, keybuf, key, cmp, valbuf, outbuf, val, op, hints, comm);
}

/* Executes a segmented exclusive scan on items in buf.
 * Executes specified MPI operation on value data
 * for items whose keys are equal.  Input values are assumed
 * to start at the first byte of the satellite data.  Stores result
 * in left-to-right scan in ltrbuf and result of right-to-left
 * scan in rtlbuf.  Items must be in sorted order. */
int DTCMP_Segmented_exscanv_fused(
  int count,
  const void* buf,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  void* ltrbuf,
  void* rtlbuf,
  MPI_Datatype val,
  MPI_Op op,
  DTCMP_Flags hints,
  MPI_Comm comm)
{
  return DTCMP_Segmented_scan_fused_base(1, count, buf, key, keysat, cmp, ltrbuf, rtlbuf, val, op, hints, comm);
}

/* Executes a segmented inclusive scan on items in buf.
 * Executes specified MPI operation on value data
 * for items whose keys are equal.  Stores result in left-to-right
 * scan in ltrbuf and result of right-to-left scan in rtlbuf.
 * Items must be in sorted order. */
int DTCMP_Segmented_scanv(
  int count,
  const void* keybuf,
  MPI_Datatype key,
  DTCMP_Op cmp,
  const void* valbuf,
  void* ltrbuf,
  void* rtlbuf,
  MPI_Datatype val,
  MPI_Op op,
  DTCMP_Flags hints,
  MPI_Comm comm)
{
  return DTCMP_Segmented_scan_base(0, count, keybuf, key, cmp, valbuf, ltrbuf, rtlbuf, val, op, hints, comm);
}

/* Executes a segmented inclusive scan on items in buf.
 * Executes specified MPI operation on value data
 * for items whose keys are equal.  Stores result in left-to-right
 * scan in outbuf.
 * Items must be in sorted order. */
int DTCMP_Segmented_scanv_ltr(
  int count,
  const void* keybuf,
  MPI_Datatype key,
  DTCMP_Op cmp,
  const void* valbuf,
  void* outbuf,
  MPI_Datatype val,
  MPI_Op op,
  DTCMP_Flags hints,
  MPI_Comm comm)
{
  return DTCMP_Segmented_scan_ltr_base(0, count, keybuf, key, cmp, valbuf, outbuf, val, op, hints, comm);
}

/* Executes a segmented inclusive scan on items in buf.
 * Executes specified MPI operation on value data
 * for items whose keys are equal.  Input values are assumed
 * to start at the first byte of the satellite data.  Stores result
 * in left-to-right scan in ltrbuf and result of right-to-left
 * scan in rtlbuf.  Items must be in sorted order. */
int DTCMP_Segmented_scanv_fused(
  int count,
  const void* buf,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  void* ltrbuf,
  void* rtlbuf,
  MPI_Datatype val,
  MPI_Op op,
  DTCMP_Flags hints,
  MPI_Comm comm)
{
  return DTCMP_Segmented_scan_fused_base(0, count, buf, key, keysat, cmp, ltrbuf, rtlbuf, val, op, hints, comm);
}
