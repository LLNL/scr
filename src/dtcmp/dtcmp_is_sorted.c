/* Copyright (c) 2012, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory.
 * Written by Adam Moody <moody20@llnl.gov>.
 * LLNL-CODE-557516.
 * All rights reserved.
 * This file is part of the DTCMP library.
 * For details, see https://github.com/hpc/dtcmp
 * Please also read this file: LICENSE.TXT. */

#include "mpi.h"
#include "dtcmp_internal.h"

void copy_key_if_valid(
  void* invec,
  void* inoutvec,
  int* len,
  MPI_Datatype* type)
{
  /* get extent of user's datatype */
  MPI_Aint lb, extent;
  MPI_Type_get_extent(*type, &lb, &extent);

  /* get pointers to start of input and output buffers */
  char* inbuf  = (char*) invec;
  char* outbuf = (char*) inoutvec;

  /* loop over each element provided in call */
  int i = 0;
  while (i < *len) {
    /* if our current entry is valid, keep it,
     * otherwise just copy over whatever first value is */
    int valid2 = *(int*) inoutvec;
    if (!valid2) {
      /* TODO: if type is big, could optimize by avoiding copy
       * if inbuf is also not valid */
      /* copy value from inbuf to outbuf */
      DTCMP_Memcpy(outbuf, 1, *type, inbuf, 1, *type);
    }

    /* increment pointers to handle next element */
    inbuf  += extent;
    outbuf += extent;
    i++;
  }
}

/* check whether all items in buf are already in sorted order */
int DTCMP_Is_sorted_local(
  const void* buf,
  int count,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints,
  int* flag)
{
  int rc = DTCMP_SUCCESS;

  /* assume that items are globally sorted,
   * we'll set this to 0 if we find otherwise */
  int sorted = 1;

  if (count > 0) {
    /* get extent of keysat */
    MPI_Aint lb, extent;
    MPI_Type_get_extent(keysat, &lb, &extent);

    /* step through and check that all of our local items are in order */
    const char* item1 = buf;
    const char* item2 = (char*)buf + extent;
    const char* last_item = (char*)buf + extent * count;
    while (item2 < last_item) {
      if (dtcmp_op_eval(item1, item2, cmp) > 0) {
        sorted = 0;
        break;
      }
      item1 = item2;
      item2 += extent;
    }
  }

  /* set output flag and return */
  *flag = sorted;
  return rc;
}

/* check whether all items in buf are already in sorted order */
int DTCMP_Is_sorted(
  const void* buf,
  int count,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints,
  MPI_Comm comm,
  int* flag)
{
  int rc = DTCMP_SUCCESS;

  /* assume that items are globally sorted,
   * we'll set this to 0 if we find otherwise */
  int sorted = 1;

  /* get our rank and the number of ranks in the communicator */
  int rank, ranks;
  MPI_Comm_rank(comm, &rank);
  MPI_Comm_size(comm, &ranks);

  /* first, step through and check that all of our local items are in order */
  DTCMP_Is_sorted_local(buf, count, key, keysat, cmp, hints, &sorted);

  /* bail out at this point if ranks == 1 */
  if (ranks <= 1) {
    *flag = sorted;
    return DTCMP_SUCCESS;
  }

  /* get extent of keysat */
  MPI_Aint lb, extent;
  MPI_Type_get_extent(keysat, &lb, &extent);

  /* get true extent of key */
  MPI_Aint key_true_lb, key_true_extent;
  MPI_Type_get_true_extent(key, &key_true_lb, &key_true_extent);

  /* TODO: if we know that each proc has an item,
   * we could just do a single pt2pt send to the rank one higher,
   * compare, then allreduce, and thereby avoid the type/op creation
   * and scan that follows */

  /* allocate type for scan, one int to say whether key is valid,
   * and our largest key */
  size_t item_size = sizeof(int) + key_true_extent;
  char* sendbuf = dtcmp_malloc(item_size, 0, __FILE__, __LINE__);
  char* recvbuf = dtcmp_malloc(item_size, 0, __FILE__, __LINE__);

  /* copy our largest item into our send buffer,
   * set valid flag to 1 if we have a value */
  int*  valid = (int*) sendbuf;
  void* value = (void*) (sendbuf + sizeof(int));
  if (count > 1) {
    *valid = 1;

    /* get pointer to largest element in our buffer,
     * and copy it to our send buffer */
    const void* lastitem = (const void*) ((const char*)buf + (count - 1) * extent);
    DTCMP_Memcpy(value, 1, key, lastitem, 1, key);
  } else {
    /* we dont have any items, so set valid flag to 0 */
    *valid = 0;
  }

  /* create and commit type that consists of leading int followed by key */
  MPI_Datatype validtype;
  dtcmp_type_concat2(MPI_INT, key, &validtype);

  /* create user-defined reduction operation to copy key if its valid */
  MPI_Op validop;
  MPI_Op_create(copy_key_if_valid, 0, &validop);

  /* execute scan to get key from next process to our left (that has an item) */
  MPI_Exscan(sendbuf, recvbuf, 1, validtype, validop, comm);

  /* free off our user-defined reduction op and datatype */
  MPI_Op_free(&validop);
  MPI_Type_free(&validtype);

  /* compare our smallest item to the received item */
  if (count > 0 && rank > 0) {
    int recvvalid = *(int*) recvbuf;
    if (recvvalid) {
      const void* recvkey = (const void*) (recvbuf + sizeof(int));
      if (dtcmp_op_eval(recvkey, buf, cmp) > 0) {
        sorted = 0;
      }
    }
  }

  /* allreduce to determine whether all items are in order */
  int all_sorted;
  MPI_Allreduce(&sorted, &all_sorted, 1, MPI_INT, MPI_LAND, comm);

  /* free the scratch space */
  dtcmp_free(&recvbuf);
  dtcmp_free(&sendbuf);

  /* set caller's output flag and return */
  *flag = all_sorted;
  return rc;
}
