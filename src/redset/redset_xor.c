#include <stdio.h>
#include <string.h>
#include <errno.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "config.h"

#ifdef HAVE_OPENMP
#include <omp.h>
#endif /* HAVE_OPENMP */

#include "mpi.h"

#include "kvtree.h"
#include "kvtree_util.h"
#include "kvtree_mpi.h"

#include "redset_io.h"
#include "redset_util.h"
#include "redset.h"
#include "redset_internal.h"

#define REDSET_KEY_COPY_XOR_CHUNK "CHUNK"

/*
=========================================
Distribute and file rebuild functions
=========================================
*/

/* XOR a with b, store result in a */
static void reduce_xor(unsigned char* a, const unsigned char* b, size_t count)
{
  size_t i;
  #pragma omp parallel for
  for (i = 0; i < count; i++) {
    a[i] ^= b[i];
  }
}

/* set chunk filenames of form:  xor.<group_id>_<xor_rank+1>_of_<xor_ranks>.redset */
static void redset_build_xor_filename(
  const char* name,
  const redset_base* d,
  char* file,
  size_t len)
{
  int rank_world;
  MPI_Comm_rank(d->parent_comm, &rank_world);
  snprintf(file, len, "%s%d.xor.grp_%d_of_%d.mem_%d_of_%d.redset",
    name, rank_world, d->group_id+1, d->groups, d->rank+1, d->ranks
  );
}

/* returns true if a an XOR file is found for this rank for the given checkpoint id,
 * sets xor_file to full filename */
static int redset_read_xor_file(
  const char* name,
  const redset_base* d,
  kvtree* header)
{
  /* set chunk filenames of form:  xor.<group_id>_<xor_rank+1>_of_<xor_ranks>.redset */
  char file[REDSET_MAX_FILENAME];
  redset_build_xor_filename(name, d, file, sizeof(file));

  /* check that we can read the file */
  if (redset_file_is_readable(file) != REDSET_SUCCESS) {
    redset_dbg(2, "Do not have read access to file: %s @ %s:%d",
      file, __FILE__, __LINE__
    );
    return REDSET_FAILURE;
  }

  /* read xor header info from file */
  if (kvtree_read_file(file, header) != KVTREE_SUCCESS) {
    return REDSET_FAILURE;
  }

  return REDSET_SUCCESS;
}

#define REDSET_KEY_COPY_XOR_DESC  "DESC"
#define REDSET_KEY_COPY_XOR_GROUP "GROUP"
#define REDSET_KEY_COPY_XOR_GROUP_RANK  "RANK"
#define REDSET_KEY_COPY_XOR_GROUP_RANKS "RANKS"

/* given a redundancy descriptor with all top level fields filled in
 * allocate and fill in structure for xor specific fields in state */
int redset_construct_xor(MPI_Comm parent_comm, redset_base* d)
{
  int rc = REDSET_SUCCESS;

  /* allocate a new structure to hold XOR state */
  redset_xor* state = (redset_xor*) REDSET_MALLOC(sizeof(redset_xor));

  /* attach structure to reddesc */
  d->state = (void*) state;

  /* allocate a new hash to store group mapping info */
  kvtree* header = kvtree_new();

  /* create a new empty hash to track group info for this xor set */
  kvtree* hash = kvtree_new();
  kvtree_set(header, REDSET_KEY_COPY_XOR_GROUP, hash);

  /* record the total number of ranks in the xor communicator */
  int ranks_comm;
  MPI_Comm_size(d->comm, &ranks_comm);
  kvtree_set_kv_int(hash, REDSET_KEY_COPY_XOR_GROUP_RANKS, ranks_comm);

  /* record mapping of rank in xor group to corresponding parent rank */
  if (ranks_comm > 0) {
    /* allocate array to receive rank from each process */
    int* ranklist = (int*) REDSET_MALLOC(ranks_comm * sizeof(int));

    /* gather rank values */
    int parent_rank;
    MPI_Comm_rank(parent_comm, &parent_rank);
    MPI_Allgather(&parent_rank, 1, MPI_INT, ranklist, 1, MPI_INT, d->comm);

    /* map ranks in comm to ranks in comm */
    int i;
    for (i=0; i < ranks_comm; i++) {
      int rank = ranklist[i];
      kvtree_setf(hash, NULL, "%s %d %d", REDSET_KEY_COPY_XOR_GROUP_RANK, i, rank);
    }

    /* free the temporary array */
    redset_free(&ranklist);
  }

  /* record group mapping info in descriptor */
  state->group_map = header;

  /* record group rank, world rank, and hostname of left and right partners */
  redset_set_partners(
    parent_comm, d->comm, 1,
    &state->lhs_rank, &state->lhs_rank_world, &state->lhs_hostname,
    &state->rhs_rank, &state->rhs_rank_world, &state->rhs_hostname
  );

  /* check that we got valid partners */
  if (state->lhs_hostname == NULL ||
      state->rhs_hostname == NULL ||
      strcmp(state->lhs_hostname, "") == 0 ||
      strcmp(state->rhs_hostname, "") == 0)
  {
    /* disable this descriptor */
    d->enabled = 0;
    redset_warn("Failed to find partner processes for redundancy descriptor, disabling @ %s:%d",
      __FILE__, __LINE__
    );
    rc = REDSET_FAILURE;
  } else {
    redset_dbg(2, "LHS partner: %s (%d)  -->  My name: %s (%d)  -->  RHS partner: %s (%d)",
      state->lhs_hostname, state->lhs_rank_world,
      redset_hostname, redset_rank,
      state->rhs_hostname, state->rhs_rank_world
    );
  }

  /* verify that all groups have a sufficient number of procs */
  int valid = 1;
  if (d->ranks <= 1) {
    valid = 0;
  }
  if (! redset_alltrue(valid, parent_comm)) {
    if (! valid) {
      redset_abort(-1, "XOR requires at least 2 ranks per set, but found %d rank(s) in set @ %s:%d",
        d->ranks, __FILE__, __LINE__
      );
    }
  }

  return rc;
}

int redset_delete_xor(redset_base* d)
{
  redset_xor* state = (redset_xor*) d->state;
  if (state != NULL) {
    /* free the hash mapping group ranks to world ranks */
    kvtree_delete(&state->group_map);

    /* free strings that we received */
    redset_free(&state->lhs_hostname);
    redset_free(&state->rhs_hostname);

    /* free the structure */
    redset_free(&d->state);
  }
  return REDSET_SUCCESS;
}

/* copy our redundancy descriptor info to a partner */
int redset_encode_reddesc_xor(
  kvtree* hash,
  const char* name,
  const redset_base* d)
{
  int rc = REDSET_SUCCESS;

  /* get pointer to XOR state structure */
  redset_xor* state = (redset_xor*) d->state;

  /* exchange our redundancy descriptor hash with our partners */
  kvtree* partner_hash = kvtree_new();
  kvtree_sendrecv(hash, state->rhs_rank, partner_hash, state->lhs_rank, d->comm);

  /* store partner hash in our under its name */
  kvtree_merge(hash, partner_hash);
  kvtree_delete(&partner_hash);

  return rc;
}

static int redset_xor_encode(
  const redset_base* d,
  redset_lofi rsf,
  const char* chunk_file,
  int fd_chunk,
  size_t chunk_size)
{
  int rc = REDSET_SUCCESS;

  /* get pointer to XOR state structure */
  redset_xor* state = (redset_xor*) d->state;

  /* allocate buffer to read a piece of my file */
  unsigned char** send_bufs = (unsigned char**) redset_buffers_alloc(1, redset_mpi_buf_size);
  unsigned char* send_buf = send_bufs[0];

  /* allocate buffer to read a piece of the recevied chunk file */
  unsigned char** recv_bufs = (unsigned char**) redset_buffers_alloc(1, redset_mpi_buf_size);
  unsigned char* recv_buf = recv_bufs[0];

  MPI_Request request[2];
  MPI_Status  status[2];

  /* XOR Reduce_scatter */
  size_t nread = 0;
  while (nread < chunk_size) {
    size_t count = chunk_size - nread;
    if (count > redset_mpi_buf_size) {
      count = redset_mpi_buf_size;
    }

    int chunk_id;
    for(chunk_id = d->ranks-1; chunk_id >= 0; chunk_id--) {
      /* read the next set of bytes for this chunk from my file into send_buf */
      if (chunk_id > 0) {
        int chunk_id_rel = (d->rank + d->ranks + chunk_id) % d->ranks;
        if (chunk_id_rel > d->rank) {
          chunk_id_rel--;
        }
        unsigned long offset = chunk_size * (unsigned long) chunk_id_rel + nread;
        if (redset_lofi_pread(&rsf, send_buf, count, offset) != REDSET_SUCCESS)
        {
          rc = REDSET_FAILURE;
        }
      } else {
        memset(send_buf, 0, count);
      }

      /* TODO: XORing with unsigned long would be faster here (if chunk size is multiple of this size) */
      /* merge the blocks via xor operation */
      if (chunk_id < d->ranks-1) {
        reduce_xor(send_buf, recv_buf, count);
      }

      if (chunk_id > 0) {
        /* not our chunk to write, forward it on and get the next */
        MPI_Irecv(recv_buf, count, MPI_BYTE, state->lhs_rank, 0, d->comm, &request[0]);
        MPI_Isend(send_buf, count, MPI_BYTE, state->rhs_rank, 0, d->comm, &request[1]);
        MPI_Waitall(2, request, status);
      } else {
        /* write send block to send chunk file */
        if (redset_write_attempt(chunk_file, fd_chunk, send_buf, count) != count) {
          rc = REDSET_FAILURE;
        }
      }
    }

    nread += count;
  }

  /* free the buffers */
  redset_buffers_free(1, &send_bufs);
  redset_buffers_free(1, &recv_bufs);

  return rc;
}

/* apply XOR redundancy scheme to dataset files */
int redset_apply_xor(
  int numfiles,
  const char** files,
  const char* name,
  const redset_base* d)
{
  int rc = REDSET_SUCCESS;

  /* pick out communicator */
  MPI_Comm comm = d->comm;

  /* get pointer to XOR state structure */
  redset_xor* state = (redset_xor*) d->state;

  /* allocate a structure to record meta data about our files and redundancy descriptor */
  kvtree* current_hash = kvtree_new();

  /* encode file info into hash */
  redset_lofi_encode_kvtree(current_hash, numfiles, files);

  /* open our logical file for reading */
  redset_lofi rsf;
  if (redset_lofi_open(current_hash, O_RDONLY, (mode_t)0, &rsf) != REDSET_SUCCESS) {
    redset_abort(-1, "Opening data files for copying: @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* get size of our logical file */
  unsigned long my_bytes = redset_lofi_bytes(&rsf);

  /* store our redundancy descriptor in hash */
  kvtree* desc_hash = kvtree_new();
  redset_store_to_kvtree(d, desc_hash);
  kvtree_set(current_hash, REDSET_KEY_COPY_XOR_DESC, desc_hash);

  /* create a hash to define our header information */
  kvtree* header = kvtree_new();

  /* record our rank within our redundancy group */
  kvtree_set_kv_int(header, REDSET_KEY_COPY_XOR_GROUP_RANK, d->rank);

  /* copy meta data to hash */
  kvtree_setf(header, current_hash, "%s %d", REDSET_KEY_COPY_XOR_DESC, d->rank);

  /* exchange meta data with partner */
  kvtree* partner_hash = kvtree_new();
  kvtree_sendrecv(current_hash, state->rhs_rank, partner_hash, state->lhs_rank, comm);

  /* copy meta data for partner to our left */
  kvtree_setf(header, partner_hash, "%s %d", REDSET_KEY_COPY_XOR_DESC, state->lhs_rank);

  /* record the global ranks of the processes in our xor group */
  kvtree_merge(header, state->group_map);

  /* allreduce to get maximum filesize */
  unsigned long max_bytes;
  MPI_Allreduce(&my_bytes, &max_bytes, 1, MPI_UNSIGNED_LONG, MPI_MAX, comm);

  /* TODO: use unsigned long integer arithmetic (with proper byte padding) instead of char to speed things up */

  /* compute chunk size according to maximum file length and number of ranks in xor set */
  /* if filesize doesn't divide evenly, then add one byte to chunk_size */
  /* TODO: check that ranks > 1 for this divide to be safe (or at partner selection time) */
  size_t chunk_size = max_bytes / (unsigned long) (d->ranks - 1);
  if ((d->ranks - 1) * chunk_size < max_bytes) {
    chunk_size++;
  }

  /* TODO: need something like this to handle 0-byte files? */
  if (chunk_size == 0) {
    chunk_size++;
  }

  /* record the dataset id and the chunk size in the xor chunk header */
  kvtree_util_set_bytecount(header, REDSET_KEY_COPY_XOR_CHUNK, chunk_size);

  /* set chunk filenames of form:  xor.<group_id>_<xor_rank+1>_of_<xor_ranks>.redset */
  char chunk_file[REDSET_MAX_FILENAME];
  redset_build_xor_filename(name, d, chunk_file, sizeof(chunk_file));

  /* open my chunk file */
  mode_t mode_file = redset_getmode(1, 1, 0);
  int fd_chunk = redset_open(chunk_file, O_WRONLY | O_CREAT | O_TRUNC, mode_file);
  if (fd_chunk < 0) {
    /* TODO: try again? */
    redset_abort(-1, "Opening XOR chunk file for writing: redset_open(%s) errno=%d %s @ %s:%d",
            chunk_file, errno, strerror(errno), __FILE__, __LINE__
    );
  }

  /* sort the header to list items alphabetically,
   * this isn't strictly required, but it ensures the kvtrees
   * are stored in the same byte order so that we can reproduce
   * the redundancy file identically on a rebuild */
  redset_sort_kvtree(header);

  /* write out the xor chunk header */
  kvtree_write_fd(chunk_file, fd_chunk, header);
  kvtree_delete(&header);

  switch (redset_encode_method) {
#ifdef HAVE_CUDA
  case REDSET_ENCODE_CUDA:
    rc = redset_xor_encode_gpu(d, rsf, chunk_file, fd_chunk, chunk_size);
    break;
#endif /* HAVE_CUDA */
#ifdef HAVE_PTHREADS
  case REDSET_ENCODE_PTHREADS:
    rc = redset_xor_encode_pthreads(d, rsf, chunk_file, fd_chunk, chunk_size);
    break;
#endif /* HAVE_PTHREADS */
#ifdef HAVE_OPENMP
  case REDSET_ENCODE_OPENMP: /* OpenMP pragmas are in CPU code */
#endif /* HAVE_OPENMP */
  case REDSET_ENCODE_CPU:
    rc = redset_xor_encode(d, rsf, chunk_file, fd_chunk, chunk_size);
    break;
  default:
    redset_abort(-1, "Unsupported encode method specified for XOR %d %s @ %s:%d",
      redset_encode_method, chunk_file, __FILE__, __LINE__
    );
  }

  /* close my chunkfile, with fsync */
  if (redset_close(chunk_file, fd_chunk) != REDSET_SUCCESS) {
    rc = REDSET_FAILURE;
  }

  /* close my dataset files */
  redset_lofi_close(&rsf);

#if 0
  /* if crc_on_copy is set, compute and store CRC32 value for chunk file */
  if (scr_crc_on_copy) {
    scr_compute_crc(map, id, scr_my_rank_world, chunk_file);
    /* TODO: would be nice to save this CRC in our partner's XOR file so we can check correctness on a rebuild */
  }
#endif

  return rc;
}

static int redset_xor_decode(
  const redset_base* d,
  int root,
  redset_lofi rsf,
  const char* chunk_file,
  int fd_chunk,
  size_t chunk_size)
{
  int rc = REDSET_SUCCESS;

  /* get pointer to XOR state structure */
  redset_xor* state = (redset_xor*) d->state;

  /* allocate buffer to read a piece of my file */
  unsigned char** send_bufs = (unsigned char**) redset_buffers_alloc(1, redset_mpi_buf_size);
  unsigned char* send_buf = send_bufs[0];

  /* allocate buffer to read a piece of the recevied chunk file */
  unsigned char** recv_bufs = (unsigned char**) redset_buffers_alloc(1, redset_mpi_buf_size);
  unsigned char* recv_buf = recv_bufs[0];

  /* Pipelined XOR Reduce to root */
  MPI_Status status[2];
  unsigned long offset = 0;
  int chunk_id;
  for (chunk_id = 0; chunk_id < d->ranks; chunk_id++) {
    size_t nread = 0;
    while (nread < chunk_size) {
      size_t count = chunk_size - nread;
      if (count > redset_mpi_buf_size) {
        count = redset_mpi_buf_size;
      }

      if (root != d->rank) {
        /* read the next set of bytes for this chunk from my file into send_buf */
        if (chunk_id != d->rank) {
          /* for this chunk, read data from the logical file */
          if (redset_lofi_pread(&rsf, send_buf, count, offset) != REDSET_SUCCESS)
          {
            /* read failed, make sure we fail this rebuild */
            rc = REDSET_FAILURE;
          }
          offset += count;
        } else {
          /* for this chunk, read data from the XOR file */
          if (redset_read_attempt(chunk_file, fd_chunk, send_buf, count) != count) {
            /* read failed, make sure we fail this rebuild */
            rc = REDSET_FAILURE;
          }
        }

        /* if not start of pipeline, receive data from left and xor with my own */
        if (root != state->lhs_rank) {
          MPI_Recv(recv_buf, count, MPI_BYTE, state->lhs_rank, 0, d->comm, &status[0]);
          reduce_xor(send_buf, recv_buf, count);
        }

        /* send data to right-side partner */
        MPI_Send(send_buf, count, MPI_BYTE, state->rhs_rank, 0, d->comm);
      } else {
        /* root of rebuild, just receive incoming chunks and write them out */
        MPI_Recv(recv_buf, count, MPI_BYTE, state->lhs_rank, 0, d->comm, &status[0]);

        /* if this is not my xor chunk, write data to normal file, otherwise write to my xor chunk */
        if (chunk_id != d->rank) {
          /* for this chunk, write data to the logical file */
          if (redset_lofi_pwrite(&rsf, recv_buf, count, offset) != REDSET_SUCCESS)
          {
            /* write failed, make sure we fail this rebuild */
            rc = REDSET_FAILURE;
          }
          offset += count;
        } else {
          /* for this chunk, write data from the XOR file */
          if (redset_write_attempt(chunk_file, fd_chunk, recv_buf, count) != count) {
            /* write failed, make sure we fail this rebuild */
            rc = REDSET_FAILURE;
          }
        }
      }

      nread += count;
    }
  }

  /* free the buffers */
  redset_buffers_free(1, &recv_bufs);
  redset_buffers_free(1, &send_bufs);

  return rc;
}

/* given a filemap, a redundancy descriptor, a dataset id, and a failed rank in my xor set,
 * rebuild files and add them to the filemap */
int redset_recover_xor_rebuild(
  const char* name,
  const redset_base* d,
  int root)
{
  int rc = REDSET_SUCCESS;

  redset_lofi rsf;
  int fd_chunk = -1;

  /* get pointer to XOR state structure */
  redset_xor* state = (redset_xor*) d->state;

  /* set chunk filenames of form:  xor.<group_id>_<xor_rank+1>_of_<xor_ranks>.redset */
  char xor_file[REDSET_MAX_FILENAME];
  redset_build_xor_filename(name, d, xor_file, sizeof(xor_file));

  /* allocate hash object to read in (or receive) the header of the XOR file */
  kvtree* header = kvtree_new();

  //int num_files = -1;
  kvtree* current_hash = NULL;
  if (root != d->rank) {
    /* open our xor file for reading */
    fd_chunk = redset_open(xor_file, O_RDONLY);
    if (fd_chunk < 0) {
      redset_abort(-1, "Opening XOR file for reading in XOR rebuild: redset_open(%s, O_RDONLY) errno=%d %s @ %s:%d",
        xor_file, errno, strerror(errno), __FILE__, __LINE__
      );
    }

    /* read in the xor chunk header */
    kvtree_read_fd(xor_file, fd_chunk, header);

    /* lookup our file info */
    current_hash = kvtree_getf(header, "%s %d", REDSET_KEY_COPY_XOR_DESC, d->rank);

    /* open our data files for reading */
    if (redset_lofi_open(current_hash, O_RDONLY, (mode_t)0, &rsf) != REDSET_SUCCESS) {
      redset_abort(-1, "Failed to open data files for reading during rebuild @ %s:%d",
        __FILE__, __LINE__
      );
    }

    /* if failed rank is to my left, i have the meta for it files, send it the header */
    if (root == state->lhs_rank) {
      kvtree_send(header, state->lhs_rank, d->comm);
    }

    /* if failed rank is to my right, send it my file info so it can write its XOR header */
    if (root == state->rhs_rank) {
      kvtree_send(current_hash, state->rhs_rank, d->comm);
    }
  } else {
    /* receive the header from right-side partner;
     * includes number of files and meta data for my files, as well as,
     * the checkpoint id and the chunk size */
    kvtree_recv(header, state->rhs_rank, d->comm);

    /* get our file info */
    current_hash = kvtree_getf(header, "%s %d", REDSET_KEY_COPY_XOR_DESC, d->rank);

    /* replace the rank id with our own */
    kvtree_util_set_int(header, REDSET_KEY_COPY_XOR_GROUP_RANK, d->rank);

    /* unset file info for rank who sent this to us */
    /* TODO: do this more cleanly */
    /* have to define the rank as a string */
    char rankstr[1024];
    snprintf(rankstr, sizeof(rankstr), "%s %d", REDSET_KEY_COPY_XOR_DESC, state->rhs_rank);
    kvtree_unset(header, rankstr);

    /* receive number of files our left-side partner has and allocate an array of
     * meta structures to store info */
    kvtree* partner_hash = kvtree_new();
    kvtree_recv(partner_hash, state->lhs_rank, d->comm);
    kvtree_setf(header, partner_hash, "%s %d", REDSET_KEY_COPY_XOR_DESC, state->lhs_rank);

    /* get permissions for file */
    mode_t mode_file = redset_getmode(1, 1, 0);

    /* get the number of files that we need to rebuild */
    if (redset_lofi_open(current_hash, O_WRONLY | O_CREAT | O_TRUNC, mode_file, &rsf) != REDSET_SUCCESS) {
      redset_abort(-1, "Failed to open data files for writing during rebuild @ %s:%d",
        __FILE__, __LINE__
      );
    }

    /* open my xor file for writing */
    fd_chunk = redset_open(xor_file, O_WRONLY | O_CREAT | O_TRUNC, mode_file);
    if (fd_chunk < 0) {
      /* TODO: try again? */
      redset_abort(-1, "Opening XOR chunk file for writing in XOR rebuild: redset_open(%s) errno=%d %s @ %s:%d",
        xor_file, errno, strerror(errno), __FILE__, __LINE__
      );
    }

    /* sort the header to list items alphabetically,
     * this isn't strictly required, but it ensures the kvtrees
     * are stored in the same byte order so that we can reproduce
     * the redundancy file identically on a rebuild */
    redset_sort_kvtree(header);

    /* write XOR chunk file header */
    kvtree_write_fd(xor_file, fd_chunk, header);
  }

  /* read the chunk size used to compute the xor data */
  unsigned long chunk_size;
  if (kvtree_util_get_unsigned_long(header, REDSET_KEY_COPY_XOR_CHUNK, &chunk_size) != REDSET_SUCCESS) {
    redset_abort(-1, "Failed to read chunk size from XOR file header %s @ %s:%d",
      xor_file, __FILE__, __LINE__
    );
  }

  switch (redset_encode_method) {
#ifdef HAVE_CUDA
  case REDSET_ENCODE_CUDA:
    rc = redset_xor_decode_gpu(d, root, rsf, xor_file, fd_chunk, chunk_size);
    break;
#endif /* HAVE_CUDA */
#ifdef HAVE_PTHREADS
  case REDSET_ENCODE_PTHREADS:
    rc = redset_xor_decode_pthreads(d, root, rsf, xor_file, fd_chunk, chunk_size);
    break;
#endif /* HAVE_PTHREADS */
#ifdef HAVE_OPENMP
  case REDSET_ENCODE_OPENMP: /* OpenMP pragmas are in CPU code */
#endif /* HAVE_OPENMP */
  case REDSET_ENCODE_CPU:
    rc = redset_xor_decode(d, root, rsf, xor_file, fd_chunk, chunk_size);
    break;
  default:
    redset_abort(-1, "Unsupported encode method specified for XOR %d %s @ %s:%d",
      redset_encode_method, xor_file, __FILE__, __LINE__
    );
  }

  /* close my chunkfile */
  if (redset_close(xor_file, fd_chunk) != REDSET_SUCCESS) {
    rc = REDSET_FAILURE;
  }

  /* close my checkpoint files */
  if (redset_lofi_close(&rsf) != REDSET_SUCCESS) {
    rc = REDSET_FAILURE;
  }

#if 0
  /* if i'm the rebuild rank, complete my file and xor chunk */
  if (root == d->rank) {
    /* complete each of our files and mark each as complete */
    for (i=0; i < num_files; i++) {
      /* TODO: need to check for errors, check that file is really valid */

      /* fill out meta info for our file and complete it */
      kvtree* meta_tmp = kvtree_get_kv_int(current_hash, REDSET_KEY_COPY_XOR_FILE, i);

      /* TODODSET:write out filemap here? */

      /* if crc_on_copy is set, compute and store CRC32 value for each file */
      if (scr_crc_on_copy) {
        /* check for mismatches here, in case we failed to rebuild the file correctly */
        if (scr_compute_crc(map, id, scr_my_rank_world, filenames[i]) != REDSET_SUCCESS) {
          scr_err("Failed to verify CRC32 after rebuild on file %s @ %s:%d",
            filenames[i], __FILE__, __LINE__
          );

          /* make sure we fail this rebuild */
          rc = REDSET_FAILURE;
        }
      }
    }

    /* if crc_on_copy is set, compute and store CRC32 value for chunk file */
    if (scr_crc_on_copy) {
      /* TODO: would be nice to check for mismatches here, but we did not save this value in the partner XOR file */
      scr_compute_crc(map, id, scr_my_rank_world, xor_file);
    }
  }
#endif

  /* reapply metadata properties to file: uid, gid, mode bits, timestamps,
   * we do this on every file instead of just the rebuilt files so that we preserve atime on all files */
  redset_lofi_apply_meta(current_hash);

  /* free the buffers */
  kvtree_delete(&header);

  return rc;
}

/* given a dataset id, check whether files can be rebuilt via xor and execute the rebuild if needed */
int redset_recover_xor(
  const char* name,
  const redset_base* d)
{
  MPI_Comm comm_world = d->parent_comm;

  /* assume we have our files */
  int need_rebuild = 0;

  /* check whether we have our XOR file */
  kvtree* header = kvtree_new();
  if (redset_read_xor_file(name, d, header) == REDSET_SUCCESS) {
    /* got our XOR file, see if we have each data file */
    kvtree* current_hash = kvtree_getf(header, "%s %d", REDSET_KEY_COPY_XOR_DESC, d->rank);
    if (redset_lofi_check(current_hash) != REDSET_SUCCESS) {
      /* some data file is bad */
      need_rebuild = 1;
    }
  } else {
    /* missing our XOR file */
    need_rebuild = 1;
  }
  kvtree_delete(&header);

  /* count how many in my xor set need to rebuild */
  int total_rebuild;
  MPI_Allreduce(&need_rebuild, &total_rebuild, 1, MPI_INT, MPI_SUM, d->comm);

  /* check whether all sets can rebuild, if not, bail out */
  int set_can_rebuild = (total_rebuild <= 1);
  if (! redset_alltrue(set_can_rebuild, comm_world)) {
    return REDSET_FAILURE;
  }

  /* it's possible to rebuild; rebuild if we need to */
  int rc = REDSET_SUCCESS;
  if (total_rebuild > 0) {
    /* someone in my set needs to rebuild, determine who */
    int tmp_rank = need_rebuild ? d->rank : -1;
    int rebuild_rank;
    MPI_Allreduce(&tmp_rank, &rebuild_rank, 1, MPI_INT, MPI_MAX, d->comm);

    /* rebuild */
    if (need_rebuild) {
      redset_dbg(2, "Rebuilding file from XOR segments");
    }
    rc = redset_recover_xor_rebuild(name, d, rebuild_rank);
  }

  /* check whether all sets rebuilt ok */
  if (! redset_alltrue(rc == REDSET_SUCCESS, comm_world)) {
    rc = REDSET_FAILURE;
  }

  return rc;
}

int redset_unapply_xor(
  const char* name,
  const redset_base* d)
{
  /* get name of xor file */
  char file[REDSET_MAX_FILENAME];
  redset_build_xor_filename(name, d, file, sizeof(file));
  int rc = redset_file_unlink(file);
  return rc;
}

/* returns a list of files added by redundancy descriptor */
redset_list* redset_filelist_enc_get_xor(
  const char* name,
  redset_base* d)
{
  char file[REDSET_MAX_FILENAME];
  redset_build_xor_filename(name, d, file, sizeof(file));
  redset_list* list = (redset_list*) REDSET_MALLOC(sizeof(redset_list));
  list->count = 1;
  list->files = (const char**) REDSET_MALLOC(sizeof(char*));
  list->files[0] = strdup(file);
  return list;
}

/* returns a list of original files encoded by redundancy descriptor */
redset_list* redset_filelist_orig_get_xor(
  const char* name,
  const redset_base* d)
{
  redset_list* list = NULL;

  /* check whether we have our files and our partner's files */
  kvtree* header = kvtree_new();
  if (redset_read_xor_file(name, d, header) == REDSET_SUCCESS) {
    /* get pointer to hash for this rank */
    kvtree* current_hash = kvtree_getf(header, "%s %d", REDSET_KEY_COPY_XOR_DESC, d->rank);
    list = redset_lofi_filelist(current_hash);
  }
  kvtree_delete(&header);

  return list;
}
