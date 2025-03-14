/* Defines a recursive hash data structure, where at the top level,
 * there is a list of elements indexed by string.  Each
 * of these elements in turn consists of a list of elements
 * indexed by string, and so on. */

#include "kvtree.h"
#include "kvtree_io.h"
#include "kvtree_err.h"
#include "kvtree_helpers.h"
#include "kvtree.h"
#include "kvtree_mpi.h"

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include <limits.h>

/*
=========================================
Hash MPI transfer functions
=========================================
*/

/* packs and send the given hash to the specified rank */
int kvtree_send(const kvtree* hash, int rank, MPI_Comm comm)
{
  /* get size of hash and check that it doesn't exceed INT_MAX */
  size_t pack_size = kvtree_pack_size(hash);
  size_t max_int = (size_t) INT_MAX;
  if (pack_size > max_int) {
    kvtree_abort(-1, "kvtree_send: hash size %lu is bigger than INT_MAX %d @ %s:%d",
      (unsigned long) pack_size, INT_MAX, __FILE__, __LINE__
    );
  }

  /* tell destination how big the pack size is */
  int size = (int) pack_size;
  MPI_Send(&size, 1, MPI_INT, rank, 0, comm);

  /* pack the hash and send it */
  if (size > 0) {
    /* allocate a buffer big enough to pack the hash */
    /* pack the hash, send it, and free our buffer */
    char* buf = (char*) KVTREE_MALLOC((size_t)size);
    kvtree_pack(buf, hash);
    MPI_Send(buf, size, MPI_BYTE, rank, 0, comm);
    kvtree_free(&buf);
  }

  return KVTREE_SUCCESS;
}

/* receives a hash from the specified rank and unpacks it into specified hash */
int kvtree_recv(kvtree* hash, int rank, MPI_Comm comm)
{
  /* check that we got a hash to receive into */
  if (hash == NULL) {
    return KVTREE_FAILURE;
  }

  /* clear the hash */
  kvtree_unset_all(hash);

  /* get the size of the incoming hash */
  int size;
  MPI_Status status;
  MPI_Recv(&size, 1, MPI_INT, rank, 0, comm, &status);

  /* receive the hash and unpack it */
  if (size > 0) {
    /* allocate a buffer big enough to receive the packed hash */
    /* receive the hash, unpack it, and free our buffer */
    char* buf = (char*) KVTREE_MALLOC((size_t)size);
    MPI_Recv(buf, size, MPI_BYTE, rank, 0, comm, &status);
    kvtree_unpack(buf, hash);
    kvtree_free(&buf);
  }

  return KVTREE_SUCCESS;
}

/* send and receive a hash in the same step */
int kvtree_sendrecv(const kvtree* hash_send, int rank_send,
                            kvtree* hash_recv, int rank_recv,
                            MPI_Comm comm)
{
  int rc = KVTREE_SUCCESS;

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
    kvtree_unset_all(hash_recv);
    have_incoming = 1;
  }

  /* exchange hash pack sizes in order to allocate buffers */
  num_req = 0;
  int size_send = 0;
  int size_recv = 0;
  if (have_incoming) {
    MPI_Irecv(&size_recv, 1, MPI_INT, rank_recv, 0, comm, &request[num_req]);
    num_req++;
  }
  if (have_outgoing) {
    /* get size of packed hash and check that it doesn't exceed INT_MAX */
    size_t pack_size = kvtree_pack_size(hash_send);
    size_t max_int = (size_t) INT_MAX;
    if (pack_size > max_int) {
      kvtree_abort(-1, "kvtree_sendrecv: hash size %lu is bigger than INT_MAX %d @ %s:%d",
        (unsigned long) pack_size, INT_MAX, __FILE__, __LINE__
      );
    }

    /* tell rank how big the pack size is */
    size_send = (int) pack_size;
    MPI_Isend(&size_send, 1, MPI_INT, rank_send, 0, comm, &request[num_req]);
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
    buf_recv = (char*) KVTREE_MALLOC((size_t)size_recv);
    MPI_Irecv(buf_recv, size_recv, MPI_BYTE, rank_recv, 0, comm, &request[num_req]);
    num_req++;
  }
  if (size_send > 0) {
    /* allocate space, pack our hash, and send it */
    buf_send = (char*) KVTREE_MALLOC((size_t)size_send);
    kvtree_pack(buf_send, hash_send);
    MPI_Isend(buf_send, size_send, MPI_BYTE, rank_send, 0, comm, &request[num_req]);
    num_req++;
  }
  if (num_req > 0) {
    MPI_Waitall(num_req, request, status);
  }

  /* unpack the hash into the hash_recv provided by the caller */
  if (size_recv > 0) {
    kvtree_unpack(buf_recv, hash_recv);
  }

  /* free the pack buffers */
  kvtree_free(&buf_recv);
  kvtree_free(&buf_send);

  return rc;
}

/* broadcasts a hash from a root and unpacks it into specified hash on all other tasks */
int kvtree_bcast(kvtree* hash, int root, MPI_Comm comm)
{
  /* get our rank in the communicator */
  int rank;
  MPI_Comm_rank(comm, &rank);

  /* determine whether we are the root of the bcast */
  if (rank == root) {
    /* get size of hash and check that it doesn't exceed INT_MAX */
    size_t pack_size = kvtree_pack_size(hash);
    size_t max_int = (size_t) INT_MAX;
    if (pack_size > max_int) {
      kvtree_abort(-1, "kvtree_bcast: hash size %lu is bigger than INT_MAX %d @ %s:%d",
        (unsigned long) pack_size, INT_MAX, __FILE__, __LINE__
      );
    }

    /* broadcast the size */
    int size = (int) pack_size;
    MPI_Bcast(&size, 1, MPI_INT, root, comm);

    /* pack the hash and send it */
    if (size > 0) {
      /* allocate a buffer big enough to pack the hash */
      /* pack the hash, broadcast it, and free our buffer */
      char* buf = (char*) KVTREE_MALLOC((size_t)size);
      kvtree_pack(buf, hash);
      MPI_Bcast(buf, size, MPI_BYTE, root, comm);
      kvtree_free(&buf);
    }
  } else {
    /* clear the hash */
    kvtree_unset_all(hash);

    /* get the size of the incoming hash */
    int size;
    MPI_Bcast(&size, 1, MPI_INT, root, comm);

    /* receive the hash and unpack it */
    if (size > 0) {
      /* allocate a buffer big enough to receive the packed hash */
      /* receive the hash, unpack it, and free our buffer */
      char* buf = (char*) KVTREE_MALLOC((size_t)size);
      MPI_Bcast(buf, size, MPI_BYTE, root, comm);
      kvtree_unpack(buf, hash);
      kvtree_free(&buf);
    }
  }

  return KVTREE_SUCCESS;
}

/* insert message destined for rank into send kvtree */
int kvtree_exchange_sendq(kvtree* send_hash, int rank, const kvtree* msg)
{
  /* see whether we've already got data for this rank,
   * if so, merge it in, otherwise create a copy and add it */
  kvtree* hash = kvtree_getf(send_hash, "%d", rank);
  if (hash == NULL) {
    /* no hash going to this rank yet, make a copy of the message and attach it */
    kvtree* copy = kvtree_new();
    kvtree_merge(copy, msg);
    kvtree_setf(send_hash, copy, "%d", rank);
  } else {
    /* got something already, just merge this message with outgoing data */
    kvtree_merge(hash, msg);
  }
  return KVTREE_SUCCESS;
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
static int kvtree_exchange_direction_hops(
  const kvtree* hash_in,
        kvtree* hash_out,
  MPI_Comm comm,
  kvtree_exchange_enum direction,
  int hops)
{
  kvtree_elem* elem;

  /* get the size of our communicator and our position within it */
  int rank, ranks;
  MPI_Comm_rank(comm, &rank);
  MPI_Comm_size(comm, &ranks);

  /* set up current hash using input hash */
  kvtree* current = kvtree_new();
  for (elem = kvtree_elem_first(hash_in);
       elem != NULL;
       elem = kvtree_elem_next(elem))
  {
    /* get destination rank */
    int dest_rank = kvtree_elem_key_int(elem);

    /* copy data into current hash with DEST and SRC keys */
    kvtree* data_hash = kvtree_elem_hash(elem);
    kvtree* dest_hash = kvtree_set_kv_int(current,   "D", dest_rank);
    kvtree* src_hash  = kvtree_set_kv_int(dest_hash, "S", rank);
    kvtree_merge(src_hash, data_hash);
  }

  /* now run through Bruck's index algorithm to exchange data,
   * if hops is positive we can stop after that many steps */
  int bit = 1;
  int step = 1;
  int hop_count = 0;
  while (step < ranks && (hops < 0 || hop_count < hops)) {
    /* compute left and right ranks for this step */
    int left = rank - step;
    if (left < 0) {
      left += ranks;
    }
    int right = rank + step;
    if (right >= ranks) {
      right -= ranks;
    }

    /* determine source and destination ranks for this step */
    int dst, src;
    if (direction == KVTREE_EXCHANGE_RIGHT) {
      /* send to the right */
      dst = right;
      src = left;
    } else {
      /* send to the left */
      dst = left;
      src = right;
    }

    /* create hashes for those we'll keep, send, and receive */
    kvtree* keep = kvtree_new();
    kvtree* send = kvtree_new();
    kvtree* recv = kvtree_new();

    /* identify hashes we'll send and keep in this step */
    kvtree* dest_hash = kvtree_get(current, "D");
    for (elem = kvtree_elem_first(dest_hash);
         elem != NULL;
         elem = kvtree_elem_next(elem))
    {
      /* get destination rank and pointer to its hash */
      int dest_rank = kvtree_elem_key_int(elem);
      kvtree* elem_hash = kvtree_elem_hash(elem);

      /* compute relative distance from our rank */
      int dist;
      if (direction == KVTREE_EXCHANGE_RIGHT) {
        dist = dest_rank - rank;
      } else {
        dist = rank - dest_rank;
      }
      if (dist < 0) {
        dist += ranks;
      }

      /* copy item to send, keep, or output hash */
      if (dest_rank == rank) {
        /* we are the destination for this item, discard SRC key
         * and copy hash to output hash */
        kvtree* dest_hash = kvtree_get(elem_hash, "S");
        kvtree_merge(hash_out, dest_hash);
      } else if (dist & bit) {
        /* we send the hash if the bit is set */
        kvtree* dest_send = kvtree_set_kv_int(send, "D", dest_rank);
        kvtree_merge(dest_send, elem_hash);
      } else {
        /* otherwise, copy hash to keep */
        kvtree* dest_keep = kvtree_set_kv_int(keep, "D", dest_rank);
        kvtree_merge(dest_keep, elem_hash);
      }
    }

    /* exchange hashes with our partners */
    kvtree_sendrecv(send, dst, recv, src, comm);

    /* merge received hash into keep */
    kvtree_merge(keep, recv);

    /* delete current hash and point it to keep instead */
    kvtree_delete(&current);
    current = keep;

    /* prepare for next rount */
    kvtree_delete(&recv);
    kvtree_delete(&send);
    bit <<= 1;
    step *= 2;
    hop_count++;
  }

  /* TODO: check that all items are really destined for this rank */

  /* copy current into output hash */
  kvtree* dest_hash = kvtree_get_kv_int(current, "D", rank);
  kvtree* elem_hash = kvtree_get(dest_hash, "S");
  kvtree_merge(hash_out, elem_hash);

  /* free the current hash */
  kvtree_delete(&current);

  return KVTREE_SUCCESS;
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
#define STEPS_LEFT  (0)
#define STEPS_RIGHT (1)
int kvtree_exchange(const kvtree* send, kvtree* recv, MPI_Comm comm)
{
  /* get our rank and number of ranks in comm */
  int rank, ranks;
  MPI_Comm_rank(comm, &rank);
  MPI_Comm_size(comm, &ranks);

  /* Since we have two paths, we try to be more efficient by sending
   * each item in the direction of fewest hops.  For example, consider
   * an 11 task job in which rank 0 wants to send to rank 8.
   * Rank 8 is closer to rank 0 from the left (dist = 3) than the
   * right (dist = 8).  However rank 0 can send to rank 8 using
   * a single hop (direct send) going right whereas the data takes 2
   * hops to go left. In this case, we send the data to the right to
   * minimize the number of hops. */
  kvtree* left  = kvtree_new();
  kvtree* right = kvtree_new();

  /* we compute maximum steps needed to each side */
  int max_steps[2];
  max_steps[STEPS_LEFT]  = 0;
  max_steps[STEPS_RIGHT] = 0;

  /* iterate through elements and assign to left or right hash */
  kvtree_elem* elem;
  for (elem = kvtree_elem_first(send);
       elem != NULL;
       elem = kvtree_elem_next(elem))
  {
    /* get dest rank and pointer to hash for that rank */
    int dest = kvtree_elem_key_int(elem);
    kvtree* elem_hash = kvtree_elem_hash(elem);

    /* compute distance to our left */
    int dist_left = rank - dest;
    if (dist_left < 0) {
      dist_left += ranks;
    }

    /* compute distance to our right */
    int dist_right = dest - rank;
    if (dist_right < 0) {
      dist_right += ranks;
    }

    /* count hops in each direction */
    int hops_left = 0;
    int hops_right = 0;
    int dist = 1;
    int step = 1;
    int steps_left  = 0;
    int steps_right = 0;
    while (dist < ranks) {
      /* if distance is odd in this bit,
       * we'd send it during this step */
      if (dist_left & dist) {
        hops_left++;
        steps_left = step;
      }
      if (dist_right & dist) {
        hops_right++;
        steps_right = step;
      }

      /* go to the next step */
      dist <<= 1;
      step++;
    }

    /* assign to hash having the fewest hops */
    kvtree* tmp = kvtree_new();
    kvtree_merge(tmp, elem_hash);
    if (hops_left < hops_right) {
      /* assign to left-going exchange */
      kvtree_setf(left, tmp, "%d", dest);
      if (steps_left > max_steps[STEPS_LEFT]) {
        max_steps[STEPS_LEFT] = steps_left;
      }
    } else {
      /* assign to right-going exchange */
      kvtree_setf(right, tmp, "%d", dest);
      if (steps_right > max_steps[STEPS_RIGHT]) {
        max_steps[STEPS_RIGHT] = steps_right;
      }
    }
  }

  /* most hash exchanges have a small number of hops
   * compared to the size of the job, so determine max
   * hops counts with allreduce and cut exchange off early */
  int all_steps[2];
  MPI_Allreduce(max_steps, all_steps, 2, MPI_INT, MPI_MAX, comm);

  /* delegate work to kvtree_exchange_direction */
  int rc = kvtree_exchange_direction_hops(
    left, recv, comm, KVTREE_EXCHANGE_LEFT, all_steps[STEPS_LEFT]
  );
  int right_rc = kvtree_exchange_direction_hops(
    right, recv, comm, KVTREE_EXCHANGE_RIGHT, all_steps[STEPS_RIGHT]
  );
  if (rc == KVTREE_SUCCESS) {
    rc = right_rc;
  }

  /* free our left and right hashes */
  kvtree_delete(&right);
  kvtree_delete(&left);

  return rc;
}

int kvtree_exchange_direction(
  const kvtree* send,
        kvtree* recv,
  MPI_Comm comm,
  kvtree_exchange_enum dir)
{
  int rc = kvtree_exchange_direction_hops(send, recv, comm, dir, -1);
  return rc;
}
