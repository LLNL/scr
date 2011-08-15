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

/* Defines a recursive hash data structure, where at the top level,
 * there is a list of elements indexed by string.  Each
 * of these elements in turn consists of a list of elements
 * indexed by string, and so on. */

#include "scr.h"
#include "scr_io.h"
#include "scr_hash.h"
#include "scr_hash_mpi.h"

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>

/* The sparse_exchange function will eventually be moved to a separate library, as
 * it is generally useful to many MPI applications.  The implementation included
 * here is a simplified version based on Brucks algorithm.  It assumes each rank
 * will send small amounts of data to a small number of other tasks. */

/* In the Brucks implementation, we pack and forward data through intermediate ranks.
 * An individual message is packed into an element, and then a list of elements is
 * packed into a packet and forwarded on to the destination.  The message data is
 * assumed to be small, and is thus inlined with the element header.
 *  <packet_header> : <element_header> <element_data>, <element_header> <element data>, ...
 */

/* qsort integer compare (using first four bytes of structure) */
static int int_cmp_fn(const void* a, const void* b)
{
  return (int) (*(int*)a - *(int*)b);
}

typedef struct {
  int rank;    /* rank of final destination */
  int msgs;    /* count of total number of messages headed for destination */
  int bytes;   /* count of total number of data bytes headed for destination */
  int payload; /* number of bytes in current packet (sum of bytes of headers and data of elements) */
} exv_packet_header;

typedef struct {
  int rank; /* rank of original sender */
  int size; /* number of message bytes associated with element */
} exv_elem_header;

static int sparse_unpack(void* tmp_data, int tmp_data_size,
                  int* idx, int* rank_list, int* size_list, void** data_list,
                  void* buf, int* off,
                  int rank, MPI_Comm comm)
{
  /* read packet header and check that it arrived at correct rank */
  exv_packet_header* packet = (exv_packet_header*) tmp_data;
  int current_rank = packet->rank;
  if (current_rank != rank) {
    /* error, received data for a different rank (shouldn't happen) */
    scr_abort(1, "Received data for rank %d @ %s:%d",
      current_rank, __FILE__, __LINE__
    );
  }

  /* set offset past packet header */
  int tmp_data_offset = sizeof(exv_packet_header);

  /* extract contents of each element in packet */
  int index  = *idx;
  int offset = *off;
  while (tmp_data_offset < tmp_data_size) {
    /* get pointer to header of the next element */
    exv_elem_header* elem = (exv_elem_header*) (tmp_data + tmp_data_offset);

    /* read the source rank, element type, and message size */
    int elem_rank = elem->rank;
    int elem_size = elem->size;

    /* advance pointer past the element header */
    tmp_data_offset += sizeof(exv_elem_header);

    /* the message data is inlined, copy the data to our receive buffer */
    rank_list[index] = elem_rank;
    size_list[index] = elem_size;
    data_list[index] = buf + offset;
    if (elem_size > 0) {
      memcpy(buf + offset, tmp_data + tmp_data_offset, elem_size);
    }
    offset += elem_size;
    tmp_data_offset += elem_size;
    index++;
  }

  *idx = index;
  *off = offset;

  return 0;
}

/* used to efficiently implement an alltoallv where each process only sends to a handful of other processes,
 * uses the indexing algorithm by Jehoshua Bruck et al, IEEE TPDS, Nov. 97 */
static int sparse_exchangev_malloc_brucks(
    int  send_nranks, int*  send_ranks, int*  send_sizes, void**  send_ptrs,
    int* recv_nranks, int** recv_ranks, int** recv_sizes, void*** recv_ptrs, void** recv_alloc,
    MPI_Comm comm)
{
  int i;
  exv_packet_header* packet = NULL;
  exv_elem_header* elem = NULL;
  int rc = MPI_SUCCESS;

  /* currently limits one message to a single destination */

  /* data is grouped into packets based on the sender of form
   *   dest_rank, num_send_ranks, total_bytes,
   *       send_rank_1, send_size_1, send_data_1, send_rank_2, send_size_2, send_data_2, ...
   * total_bytes measures the total bytes in the data list including the bytes taken to record the
   * sender rank and size
   */

  /* get the size of our communicator and our position within it */
  int rank, ranks;
  MPI_Comm_rank(comm, &rank);
  MPI_Comm_size(comm, &ranks);

  /* compute number of communication rounds */
  int num_rounds = 0;
  int factor = 1;
  while (factor < ranks) {
    num_rounds++;
    factor *= 2;
  }

  /* allocate space for send and destination rank arrays, size arrays, and MPI request and status objects
   * to be a bit more efficient, we allocation one big section of memory and then adjust pointers for each
   * of these data structures into that larger block of memory */
  int dst = MPI_PROC_NULL;    /* rank this process will send to during a given round */
  int src = MPI_PROC_NULL;    /* rank this process will receive from in a given round */
  int send_bytes = 0;    /* number of bytes this process will send */
  int recv_bytes = 0;    /* number of bytes this process will receive */
  int recv_offset = 0;    /* array to hold offset into each of the receive buffers for the merge step */
  int payload_sizes[2]; /* number of bytes in each payload */
  int payload_offs[2];  /* offsets into each payload */
  void* payload_bufs[2];  /* array of pointers to payload data to be merged */
  MPI_Request req[2];  /* array of MPI_Requests for outstanding messages when exchanging number of bytes */
  MPI_Request req2[2]; /* array of MPI_Requests for outstanding messages when exchanging data */
  MPI_Status stat[2];  /* array of MPI_Status objects for number-of-byte messages */
  MPI_Status stat2[2]; /* array of MPI_Status objects for data exchange messages */

  void* tmp_data = NULL;
  int tmp_data_size = 0;
  if (send_nranks > 0) {
    /* sort the input ranks, remember their original location to copy the data */
    int* sort_list = (int*) malloc(2 * sizeof(int) * send_nranks);
    if (sort_list == NULL) {
      scr_abort(1, "Failed to allocate memory for sort list @ %s:%d",
        __FILE__, __LINE__
      );
    }
    int send_byte_count = 0;
    for (i=0; i < send_nranks; i++) {
      sort_list[i*2+0] = send_ranks[i];
      sort_list[i*2+1] = i;
      send_byte_count += send_sizes[i];
    }
    qsort(sort_list, send_nranks, 2 * sizeof(int), &int_cmp_fn);

    /* prepare data for sending */
    tmp_data = malloc((sizeof(exv_packet_header) + sizeof(exv_elem_header)) * send_nranks + send_byte_count);
    if (tmp_data == NULL) {
      scr_abort(1, "Failed to allocate temporary send buffer @ %s:%d",
        __FILE__, __LINE__
      );
    }

    int last_rank = MPI_PROC_NULL;
    for (i=0; i < send_nranks; i++) {
      /* get the rank this packet is headed to */
      int current_rank = sort_list[i*2+0];
      if (current_rank == last_rank && last_rank != MPI_PROC_NULL) {
        scr_abort(1,"Destination rank %d specified multiple times @ %s:%d",
          __FILE__, __LINE__
        );
      }
      if (current_rank >= 0 && current_rank < ranks) {
        int rank_index = sort_list[i*2+1];
        int send_size  = send_sizes[rank_index];
        void* send_ptr = send_ptrs[rank_index];

        packet = (exv_packet_header*) (tmp_data + tmp_data_size);
        tmp_data_size += sizeof(exv_packet_header);

        elem = (exv_elem_header*) (tmp_data + tmp_data_size);
        tmp_data_size += sizeof(exv_elem_header);

        /* fill in our packet header */
        int packet_payload = sizeof(exv_elem_header);
        packet_payload += send_size;
        packet->rank    = current_rank;
        packet->msgs    = 1;
        packet->bytes   = send_size;
        packet->payload = packet_payload;

        /* fill in out element header */
        elem->rank = rank;
        elem->size = send_size;

        /* inline the message data */
        if (send_size > 0) {
          memcpy(tmp_data + tmp_data_size, send_ptr, send_size);
          tmp_data_size += send_size;
        }
      } else if (current_rank != MPI_PROC_NULL) {
        /* error, rank out of range */
        scr_abort(1, "Invalid destination rank %d @ %s:%d",
          current_rank, __FILE__, __LINE__
        );
      }
    }

    /* free the sort array */
    if (sort_list != NULL) {
      free(sort_list);
      sort_list = NULL;
    }
  }

  /* now run through Bruck's index algorithm to exchange data */
  factor = 1;
  while (factor < ranks) {
    /* allocate a buffer to pack our send data in, assume we need to send the full buffer to each destination */
    void* tmp_send = NULL;
    if (tmp_data_size > 0) {
      tmp_send = malloc(tmp_data_size);
      if (tmp_send == NULL) {
        scr_abort(1, "Failed to allocate temporary pack buffer @ %s:%d",
          __FILE__, __LINE__
        );
      }
    }

    /* determine our source and destination ranks for this step and set our send buffer locations and send size */
    dst = (rank + factor + ranks) % ranks;
    src = (rank - factor + ranks) % ranks;

    /* pack our send messages and count number of bytes in each */
    send_bytes = 0;
    int offset = 0;
    while (offset < tmp_data_size) {
      packet = (exv_packet_header*) (tmp_data + offset);
      int current_rank = packet->rank;
      int current_size = sizeof(exv_packet_header) + packet->payload;
      int relative_rank = (current_rank - rank + ranks) % ranks;
      int relative_id = (relative_rank / factor) % 2;
      if (relative_id > 0) {
        memcpy(tmp_send + send_bytes, tmp_data + offset, current_size);
        send_bytes += current_size;
      }
      offset += current_size;
    }

    /* exchange number of bytes for this round */
    MPI_Irecv(&recv_bytes, 1, MPI_INT, src, 0, comm, &req[0]);
    MPI_Isend(&send_bytes, 1, MPI_INT, dst, 0, comm, &req[1]);

    /* eagerly send our non-zero messages */
    int req2_count = 0;
    if (send_bytes > 0) {
      MPI_Isend(tmp_send, send_bytes, MPI_BYTE, dst, 0, comm, &req2[req2_count]);
      req2_count++;
    }

    /* wait for the number-of-bytes messages to complete */
    MPI_Waitall(2, req, stat);

    /* count total number of bytes we'll receive in this round */
    int tmp_recv_size = recv_bytes;

    /* allocate buffer to hold all of those bytes */
    void* tmp_recv = NULL;
    if (tmp_recv_size > 0) {
      tmp_recv = (void*) malloc(tmp_recv_size);
      if (tmp_recv == NULL) {
        scr_abort(1, "Failed to allocate temporary receive buffer @ %s:%d",
          __FILE__, __LINE__
        );
      }
    }

    /* finally, recv each non-zero message, and wait on sends and receives of non-zero messages */
    if (recv_bytes > 0) {
      MPI_Irecv(tmp_recv, recv_bytes, MPI_BYTE, src, 0, comm, &req2[req2_count]);
      req2_count++;
    }
    if (req2_count > 0) {
      MPI_Waitall(req2_count, req2, stat2);
    }

    /* free the temporary send buffer */
    if (tmp_send != NULL) {
      free(tmp_send);
      tmp_send = NULL;
    }

    /* allocate space to merge received data with our data */
    void* new_data = malloc(tmp_data_size + tmp_recv_size);
    if (new_data == NULL) {
      scr_abort(1, "Failed to allocate merge buffer @ %s:%d",
        __FILE__, __LINE__
      );
    }
    int new_data_size = 0;

    /* initialize offsets for our current buffer and each of our receive buffers */
    int tmp_offset  = 0;
    recv_offset = 0;

    /* merge received data with data we've yet to send */
    int data_remaining = 1;
    while (data_remaining) {
      /* first, scan through and identify lowest rank among our buffer and receive buffers */
      int reduce_rank  = -1;

      /* first check our buffer */
      int tmp_current_rank = -1;
      while (tmp_offset < tmp_data_size && tmp_current_rank == -1) { 
        packet = (exv_packet_header*) (tmp_data + tmp_offset);
        int current_rank = packet->rank;
        int relative_rank = (current_rank - rank + ranks) % ranks;
        int relative_id = (relative_rank / factor) % 2;
        if (relative_id == 0) {
          /* we kept the data for this rank during this round, so consider this rank for merging */
          tmp_current_rank = current_rank;
        } else {
          /* we sent this data for this rank to someone else during this step, so skip it */
          tmp_offset += sizeof(exv_packet_header) + packet->payload;
        }
      }
      if (tmp_current_rank != -1) {
        reduce_rank = tmp_current_rank;
      }

      /* now check each of our receive buffers */
      if (recv_offset < recv_bytes) {
        packet = (exv_packet_header*) (tmp_recv + recv_offset);
        int current_rank = packet->rank;
        if (current_rank < reduce_rank || reduce_rank == -1) {
          reduce_rank = current_rank;
        }
      }

      /* if we found a rank, merge the data in the new_data buffer */
      if (reduce_rank != -1) {
        int num_msgs = 0;
        int num_bytes = 0;
        int payload_count = 0;

        /* if the current destination rank of the temp buffer matches the reduce rank,
         * get the count of the number of messages for the destination rank */
        if (tmp_offset < tmp_data_size) { 
          packet = (exv_packet_header*) (tmp_data + tmp_offset);
          int current_rank = packet->rank;
          if (current_rank == reduce_rank) {
            num_msgs  += packet->msgs;
            num_bytes += packet->bytes;
            int payload = packet->payload;
            payload_bufs[payload_count]  = (tmp_data + tmp_offset + sizeof(exv_packet_header));
            payload_sizes[payload_count] = payload;
            payload_offs[payload_count]  = 0;
            payload_count++;
            tmp_offset += sizeof(exv_packet_header) + payload;
          }
        }

        /* get the count from each receive buffer if they match the destination rank */
        if (recv_offset < recv_bytes) {
          packet = (exv_packet_header*) (tmp_recv + recv_offset);
          int current_rank = packet->rank;
          if (current_rank == reduce_rank) {
            num_msgs  += packet->msgs;
            num_bytes += packet->bytes;
            int payload = packet->payload;
            payload_bufs[payload_count]  = tmp_recv + recv_offset + sizeof(exv_packet_header);
            payload_sizes[payload_count] = payload;
            payload_offs[payload_count]  = 0;
            payload_count++;
            recv_offset += sizeof(exv_packet_header) + payload;
          }
        }

        /* prepare our packet header */
        packet = (exv_packet_header*) (new_data + new_data_size);
        packet->rank    = reduce_rank;
        packet->msgs    = num_msgs;
        packet->bytes   = num_bytes;

        /* merge payloads into a single payload */
        void* merge_buf = new_data + new_data_size + sizeof(exv_packet_header);
        int merge_size = 0;
        for (i = 0; i < payload_count; i++) {
          memcpy(merge_buf + merge_size, payload_bufs[i], payload_sizes[i]);
          merge_size += payload_sizes[i];
        }

        /* update based on the merged payload size */
        packet->payload = merge_size;
        new_data_size += sizeof(exv_packet_header) + merge_size;
      } else {
        /* found no rank on this scan, so we must be done */
        data_remaining = 0;
      }
    }

    /* we've merged our receive data, so free the receive buffer */
    if (tmp_recv != NULL) {
      free(tmp_recv);
      tmp_recv = NULL;
    }

    /* free our current buffer and assign the pointer to the newly merged buffer */
    if (tmp_data != NULL) {
      free(tmp_data);
      tmp_data = NULL;
    }
    tmp_data      = new_data;
    tmp_data_size = new_data_size;
    new_data      = NULL;
    new_data_size = 0;

    /* go on to the next phase of the exchange */
    factor *= 2;
  }

  /* format received data according to interface */
  *recv_nranks = 0;
  *recv_ranks  = NULL;
  *recv_sizes  = NULL;
  *recv_ptrs   = NULL;
  *recv_alloc  = NULL;
  if (tmp_data_size > 0) {
    /* read packet header and check that it arrived at correct rank */
    packet = (exv_packet_header*) tmp_data;
    int current_rank  = packet->rank;
    if (current_rank != rank) {
      /* error, received data for a different rank (shouldn't happen) */
      scr_abort(1, "Received data for rank %d @ %s:%d",
        current_rank, __FILE__, __LINE__
      );
    }

    /* count the number of elements we received */
    int msgs  = packet->msgs;
    int bytes = packet->bytes;

    /* compute number of bytes to allocate to receive all data */
    void* ret_buf = NULL;
    int ret_buf_size = (2 * sizeof(int) + sizeof(void*)) * msgs + bytes;
    if (ret_buf_size > 0) {
      ret_buf = (void*) malloc(ret_buf_size);
      if (ret_buf == NULL) {
        scr_abort(1, "Failed to allocate memory for return data @ %s:%d",
          __FILE__, __LINE__
        );
      }
    }

    int*   rank_list = NULL;
    int*   size_list = NULL;
    void** data_list = NULL;
    void*  data_set  = NULL;

    void* ret_buf_tmp = ret_buf;
    if (msgs > 0) {
      rank_list = ret_buf_tmp;
      ret_buf_tmp += sizeof(int) * msgs;

      size_list = ret_buf_tmp;
      ret_buf_tmp += sizeof(int) * msgs;

      data_list = ret_buf_tmp;
      ret_buf_tmp += sizeof(void*) * msgs;
    }
    if (bytes > 0) {
      data_set = ret_buf_tmp;
      ret_buf_tmp += bytes;
    }

    int index = 0;
    int data_set_offset = 0;
    sparse_unpack(tmp_data, tmp_data_size,
                  &index, rank_list, size_list, data_list,
                  data_set, &data_set_offset,
                  rank, comm
    );

    *recv_nranks = msgs;
    *recv_ranks  = rank_list;
    *recv_sizes  = size_list;
    *recv_ptrs   = data_list;
    *recv_alloc  = ret_buf;
  }

  /* free off the result buffer */
  if (tmp_data != NULL) {
    free(tmp_data);
    tmp_data = NULL;
  }

  return rc;
}

/*
=========================================
Hash MPI transfer functions
=========================================
*/

/* packs and send the given hash to the specified rank */
int scr_hash_send(const scr_hash* hash, int rank, MPI_Comm comm)
{
  /* first get the size of the hash */
  size_t size = scr_hash_pack_size(hash);
  
  /* tell rank how big the pack size is */
  MPI_Send(&size, sizeof(size), MPI_BYTE, rank, 0, comm);

  /* pack the hash and send it */
  if (size > 0) {
    /* allocate a buffer big enough to pack the hash */
    char* buf = (char*) malloc(size);
    if (buf != NULL) {
      /* pack the hash, send it, and free our buffer */
      scr_hash_pack(buf, hash);
      MPI_Send(buf, size, MPI_BYTE, rank, 0, comm);
      free(buf);
      buf = NULL;
    } else {
      scr_abort(-1, "scr_hash_send: Failed to malloc buffer to pack hash @ %s:%d",
              __FILE__, __LINE__
      );
    }
  }

  return SCR_SUCCESS;
}

/* receives a hash from the specified rank and unpacks it into specified hash */
int scr_hash_recv(scr_hash* hash, int rank, MPI_Comm comm)
{
  /* check that we got a hash to receive into */
  if (hash == NULL) {
    return SCR_FAILURE;
  }

  /* clear the hash */
  scr_hash_unset_all(hash);

  /* get the size of the incoming hash */
  MPI_Status status;
  size_t size = 0;
  MPI_Recv(&size, sizeof(size), MPI_BYTE, rank, 0, comm, &status);
  
  /* receive the hash and unpack it */
  if (size > 0) {
    /* allocate a buffer big enough to receive the packed hash */
    char* buf = (char*) malloc(size);
    if (buf != NULL) {
      /* receive the hash, unpack it, and free our buffer */
      MPI_Recv(buf, size, MPI_BYTE, rank, 0, comm, &status);
      scr_hash_unpack(buf, hash);
      free(buf);
      buf = NULL;
    } else {
      scr_abort(-1, "scr_hash_recv: Failed to malloc buffer to receive hash @ %s:%d",
              __FILE__, __LINE__
      );
    }
  }

  return SCR_SUCCESS;
}

/* send and receive a hash in the same step */
int scr_hash_sendrecv(const scr_hash* hash_send, int rank_send,
                            scr_hash* hash_recv, int rank_recv,
                            MPI_Comm comm)
{
  int rc = SCR_SUCCESS;

  int num_req;
  MPI_Request request[2];
  MPI_Status  status[2];

  /* determine whether we have a rank to send to and a rank to receive from */
  int have_outgoing = 0;
  int have_incoming = 0;
  if (rank_send != MPI_PROC_NULL) {
    have_outgoing = 1;
  }
  if (rank_recv != MPI_PROC_NULL) {
    scr_hash_unset_all(hash_recv);
    have_incoming = 1;
  }

  /* exchange hash pack sizes in order to allocate buffers */
  num_req = 0;
  size_t size_send = 0;
  size_t size_recv = 0;
  if (have_incoming) {
    MPI_Irecv(&size_recv, sizeof(size_recv), MPI_BYTE, rank_recv, 0, comm, &request[num_req]);
    num_req++;
  }
  if (have_outgoing) {
    size_send = scr_hash_pack_size(hash_send);
    MPI_Isend(&size_send, sizeof(size_send), MPI_BYTE, rank_send, 0, comm, &request[num_req]);
    num_req++;
  }
  if (num_req > 0) {
    MPI_Waitall(num_req, request, status);
  }

  /* allocate space to pack our hash and space to receive the incoming hash */
  num_req = 0;
  char* buf_send = NULL;
  char* buf_recv = NULL;
  if (size_recv > 0) {
    /* allocate space to receive a packed hash, and receive it */
    buf_recv = (char*) malloc(size_recv);
    /* TODO: check for errors */
    MPI_Irecv(buf_recv, size_recv, MPI_BYTE, rank_recv, 0, comm, &request[num_req]);
    num_req++;
  }
  if (size_send > 0) {
    /* allocate space, pack our hash, and send it */
    buf_send = (char*) malloc(size_send);
    /* TODO: check for errors */
    scr_hash_pack(buf_send, hash_send);
    MPI_Isend(buf_send, size_send, MPI_BYTE, rank_send, 0, comm, &request[num_req]);
    num_req++;
  }
  if (num_req > 0) {
    MPI_Waitall(num_req, request, status);
  }

  /* unpack the hash into the hash_recv provided by the caller */
  if (size_recv > 0) {
    scr_hash_unpack(buf_recv, hash_recv);
  }

  /* free the pack buffers */
  if (buf_recv != NULL) {
    free(buf_recv);
    buf_recv = NULL;
  }
  if (buf_send != NULL) {
    free(buf_send);
    buf_send = NULL;
  }

  return rc;
}

/* broadcasts a hash from a root and unpacks it into specified hash on all other tasks */
int scr_hash_bcast(scr_hash* hash, int root, MPI_Comm comm)
{
  /* get our rank in the communicator */
  int rank;
  MPI_Comm_rank(comm, &rank);

  /* determine whether we are the root of the bcast */
  if (rank == root) {
    /* first get the size of the hash */
    size_t size = scr_hash_pack_size(hash);
  
    /* broadcast the size */
    MPI_Bcast(&size, sizeof(size), MPI_BYTE, root, comm);

    /* pack the hash and send it */
    if (size > 0) {
      /* allocate a buffer big enough to pack the hash */
      char* buf = (char*) malloc(size);
      if (buf != NULL) {
        /* pack the hash, broadcast it, and free our buffer */
        scr_hash_pack(buf, hash);
        MPI_Bcast(buf, size, MPI_BYTE, root, comm);
        free(buf);
        buf = NULL;
      } else {
        scr_abort(-1, "scr_hash_bcast: Failed to malloc buffer to pack hash @ %s:%d",
                __FILE__, __LINE__
        );
      }
    }
  } else {
    /* clear the hash */
    scr_hash_unset_all(hash);

    /* get the size of the incoming hash */
    size_t size = 0;
    MPI_Bcast(&size, sizeof(size), MPI_BYTE, root, comm);
  
    /* receive the hash and unpack it */
    if (size > 0) {
      /* allocate a buffer big enough to receive the packed hash */
      char* buf = (char*) malloc(size);
      if (buf != NULL) {
        /* receive the hash, unpack it, and free our buffer */
        MPI_Bcast(buf, size, MPI_BYTE, root, comm);
        scr_hash_unpack(buf, hash);
        free(buf);
        buf = NULL;
      } else {
        scr_abort(-1, "scr_hash_bcast: Failed to malloc buffer to receive hash @ %s:%d",
                __FILE__, __LINE__
        );
      }
    }
  }

  return SCR_SUCCESS;
}

/* execute a (sparse) global exchange, similar to an alltoallv operation
 * 
 * hash_send specifies destinations as:
 * <rank_X>
 *   <hash_to_send_to_rank_X>
 * <rank_Y>
 *   <hash_to_send_to_rank_Y>
 *
 * hash_recv returns hashes sent from remote ranks as:
 * <rank_A>
 *   <hash_received_from_rank_A>
 * <rank_B>
 *   <hash_received_from_rank_B> */
int scr_hash_exchange(const scr_hash* hash_send, scr_hash* hash_recv, MPI_Comm comm)
{
  int index;
  scr_hash_elem* elem = NULL;

  /* get the number of ranks we'll be sending to */
  int send_count = scr_hash_size(hash_send);

  /* allocate space for our list of ranks, sizes, and send buffer pointers */
  int* send_ranks  = NULL;
  int* send_sizes  = NULL;
  void** send_bufs = NULL;
  void* scratch = NULL;
  int scratch_size = (2 * sizeof(int) + sizeof(void*)) * send_count;
  if (scratch_size > 0) {
    /* attempt to allocate some memory for our data strucutres */
    scratch = (void*) malloc(scratch_size);
    if (scratch == NULL) {
      scr_abort(-1, "Failed to allocate list of ranks, sizes, or buffers @ %s:%d",
        __FILE__, __LINE__
      );
    }
    void* tmp_scratch = scratch;

    send_ranks = (int*) tmp_scratch;
    tmp_scratch += sizeof(int) * send_count;

    send_sizes = (int*) tmp_scratch;
    tmp_scratch += sizeof(int) * send_count;

    send_bufs = (void*) tmp_scratch;
    tmp_scratch += sizeof(void*) * send_count;
  }
    
  /* fill in the list of ranks and sizes, and count the total number of bytes we'll send */
  index = 0;
  size_t pack_count = 0;
  for (elem = scr_hash_elem_first(hash_send);
       elem != NULL;
       elem = scr_hash_elem_next(elem))
  {
    /* record the rank id */
    int send_rank = scr_hash_elem_key_int(elem);
    send_ranks[index] = send_rank;

    /* record the size of the packed hash, and add to total send size */
    scr_hash* hash = scr_hash_elem_hash(elem);
    int pack_size = scr_hash_pack_size(hash);
    send_sizes[index] = pack_size;
    pack_count += pack_size;

    index++;
  }

  /* allocate space to pack all of the outgoing hashes */
  void* send_buf = NULL;
  if (pack_count > 0) {
    send_buf = (void*) malloc(pack_count);
    if (send_buf == NULL) {
      scr_abort(-1, "Failed to allocate buffer to pack outgoing hashes @ %s:%d",
              __FILE__, __LINE__
      );
    }
  }

  /* pack each of our outgoing hashes */
  index = 0;
  size_t pack_offset = 0;
  for (elem = scr_hash_elem_first(hash_send);
       elem != NULL;
       elem = scr_hash_elem_next(elem))
  {
    /* record the location of the buffer to pack the current hash */
    void* buf = send_buf + pack_offset;
    send_bufs[index] = buf;

    /* pack the hash into our buffer */
    scr_hash* hash = scr_hash_elem_hash(elem);
    int pack_size = scr_hash_pack(buf, hash);
    pack_offset += pack_size;

    index++;
  }

  /* exchange messages using a sparse_exchange call */
  int  recv_count = 0;
  int* recv_ranks = NULL;
  int* recv_sizes = NULL;
  void** recv_bufs = NULL;
  void* recv_malloc = NULL;
  sparse_exchangev_malloc_brucks(
    send_count,  send_ranks,  send_sizes,  send_bufs,
    &recv_count, &recv_ranks, &recv_sizes, &recv_bufs, &recv_malloc,
    comm
  );

  /* unpack our received messages and merge into our receive hash */
  for(index = 0; index < recv_count; index++) {
    /* unpack hash into a temporary hash */
    void* buf = recv_bufs[index];
    scr_hash* hash = scr_hash_new();
    scr_hash_unpack(buf, hash);

    /* store this hash in our recv hash */
    int rank = recv_ranks[index];
    scr_hash_setf(hash_recv, hash, "%d", rank);
  }

  /* free memory allocated in call to sparse_exchange */
  if (recv_malloc != NULL) {
    free(recv_malloc);
    recv_malloc = NULL;
  }

  /* free off our internal data structures */
  if (send_buf != NULL) {
    free(send_buf);
    send_buf = NULL;
  }
  if (scratch != NULL) {
    free(scratch);
    scratch = NULL;
  }

  return SCR_SUCCESS;
}
