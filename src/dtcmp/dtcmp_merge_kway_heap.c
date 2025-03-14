/* Copyright (c) 2012, Lawrence Livermore National Security, LLC.
 * Produced at the Lawrence Livermore National Laboratory.
 * Written by Adam Moody <moody20@llnl.gov>.
 * LLNL-CODE-557516.
 * All rights reserved.
 * This file is part of the DTCMP library.
 * For details, see https://github.com/hpc/dtcmp
 * Please also read this file: LICENSE.TXT. */

#include <string.h>
#include "dtcmp_internal.h"

/* merges k sorted lists into into single sorted list using a min-heap merge */
int DTCMP_Merge_local_kway_heap(
  int k,
  const void* inbufs[],
  int counts[],
  void* outbuf,
  MPI_Datatype key,
  MPI_Datatype keysat,
  DTCMP_Op cmp,
  DTCMP_Flags hints)
{
  /* setup a pointer to march through elements in output buffer */
  char* out = (char*)outbuf;

  /* get size of an element so we can increase our pointer by the correct amount */
  MPI_Aint lb, extent;
  MPI_Type_get_extent(keysat, &lb, &extent);

  /* each element in our heap will store a pointer to the list element, followed by
   * index of the list 0..k-1 it came from, followed by the index within that list 1..counts[j] */
  int ptr_with_two_ints = sizeof(void*) + 2 * sizeof(int);

  /* allocate space for our heap */
  void* tmp  = dtcmp_malloc(ptr_with_two_ints, 0, __FILE__, __LINE__);
  void* heap = dtcmp_malloc(k * ptr_with_two_ints, 0, __FILE__, __LINE__);

  /* initialize our heap */
  int heap_size = 0;
  int i;
  for (i = 0; i < k; i++) {
    if (counts[i] > 0) {
      /* get a pointer to the last position in the heap */
      int current_index = heap_size;
      void* last_ptr = (char*)heap + current_index * ptr_with_two_ints;

      /* insert our item, copy a pointer to the item as well as
       * the list it came from and the next index position in that list */
      void** void_ptr = (void**) (last_ptr);
      int*   int_ptr  = (int*)   ((char*)last_ptr + sizeof(void*));
      void_ptr[0] = (void*) (inbufs[i]);
      int_ptr[0]  = i;
      int_ptr[1]  = 1;

      /* increment the size of the heap */
      heap_size++;

      /* bubble-up the newly added item */
      while (1) {
        /* if we are at the root of the tree, we have a valid heap */
        if (current_index == 0) {
          break;
        }

        /* otherwise, get a pointer to our item */
        void* current_ptr = (char*)heap + current_index * ptr_with_two_ints;
        void* current_item = *(void**) current_ptr;

        /* and get a pointer to our parent's item */
        int parent_index = ((current_index-1) >> 1);
        void* parent_ptr = (char*)heap + parent_index * ptr_with_two_ints;
        void* parent_item = *(void**) parent_ptr;

        /* if our item is smaller than our parent's item,
         * swap them, otherwise we have a valid heap */
        int result = dtcmp_op_eval(current_item, parent_item, cmp);
        if (result < 0) {
          memcpy(tmp, parent_ptr, ptr_with_two_ints);
          memcpy(parent_ptr, current_ptr, ptr_with_two_ints);
          memcpy(current_ptr, tmp, ptr_with_two_ints);
          current_index = parent_index;
        } else {
          break;
        }
      }
    }
  }

  /* merge */
  int count = 0;
  while (heap_size > 1) {
    /* copy the item from the top of the heap to our merge buffer */
    void* top_item = *(void**) heap;
    DTCMP_Memcpy(out, 1, keysat, top_item, 1, keysat);
    out += extent;
    count++;

    /* get the indicies for this element from the top of the heap */
    int* int_ptr = (int*) ((char*)heap + sizeof(void*));
    int j     = int_ptr[0];
    int index = int_ptr[1];

    if (index < counts[j]) {
      /* fetch another item from list j and copy it to top of heap */
      void** void_ptr = (void**) heap;
      *void_ptr = (void*) ((char*)inbufs[j] + index * extent);
      int_ptr[1] = index + 1;
    } else {
      /* remove item from end of heap and move it to root of heap */
      heap_size--;
      if (heap_size > 0) {
        memcpy(heap, (char*)heap + heap_size * ptr_with_two_ints, ptr_with_two_ints);
      }
    }

    /* heapify */
    int current_index = 0;
    while (1) {
      /* get a pointer to our item */
      void* current_ptr = (char*)heap + current_index * ptr_with_two_ints;
      void* current_item = *(void**) current_ptr;

      /* check whether we have a left child */
      int left_index = (current_index << 1) + 1;
      if (left_index < heap_size) {
        /* get pointers to the item belonging to the left child */
        void* left_ptr = (char*)heap + left_index * ptr_with_two_ints;
        void* left_item = *(void**) left_ptr;

        /* assume the left child is the smaller of the left and right items */
        int min_index  = left_index;
        void* min_ptr  = left_ptr;
        void* min_item = left_item;

        /* check whether we have a right child */
        int right_index = left_index + 1;
        if (right_index < heap_size) {
          /* get pointers to the item belonging to the right child */
          void* right_ptr = (char*)heap + right_index * ptr_with_two_ints;
          void* right_item = *(void**) right_ptr;
        
          /* if the right item is smaller than the left,
           * use it to compare to our current item */
          int result = dtcmp_op_eval(right_item, left_item, cmp);
          if (result < 0) {
            min_index = right_index;
            min_ptr   = right_ptr;
            min_item  = right_item;
          }
        }

        /* if the smaller of our left and right items is less than our current item, swap them */
        int result = dtcmp_op_eval(min_item, current_item, cmp);
        if (result < 0) {
          memcpy(tmp, min_ptr, ptr_with_two_ints);
          memcpy(min_ptr, current_ptr, ptr_with_two_ints);
          memcpy(current_ptr, tmp, ptr_with_two_ints);
          current_index = min_index;
        } else {
          /* our current item is smaller than either the left or right items, so we have a valid heap */
          break;
        }
      } else {
        /* we have no children, so we're at the bottom of the heap */
        break;
      }
    }
  }

  /* copy remainder of last list if we have one */
  if (heap_size == 1) {
    /* get the indicies for this element from the top of the heap */
    int* int_ptr = (int*) ((char*)heap + sizeof(void*));
    int j     = int_ptr[0];
    int index = int_ptr[1];

    /* subtract one from the index, and compute the remaining number of elements in this list */
    index--;
    int remainder = counts[j] - index;
    if (remainder > 0) {
      /* if the remainder is positive, copy the remaining elements */
      DTCMP_Memcpy(out, remainder, keysat, (char*)inbufs[j] + index * extent, remainder, keysat);
    }
  }
  
  /* free the space we allocated for our heap */
  dtcmp_free(&heap);
  dtcmp_free(&tmp);

  return DTCMP_SUCCESS;
}
