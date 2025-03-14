#include <stdio.h>
#include <string.h>
#include <errno.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "mpi.h"

#include "kvtree.h"
#include "kvtree_util.h"
#include "kvtree_mpi.h"

#include "redset_io.h"
#include "redset_util.h"
#include "redset.h"
#include "redset_internal.h"

#define REDSET_KEY_COPY_PARTNER_DESC "DESC"
#define REDSET_KEY_COPY_PARTNER_GROUP_RANK "RANK"

/* set partner filename */
static void redset_build_partner_filename(
  const char* name,
  const redset_base* d,
  char* file,
  size_t len)
{
  int rank_world;
  MPI_Comm_rank(d->parent_comm, &rank_world);
  snprintf(file, len, "%s%d.partner.grp_%d_of_%d.mem_%d_of_%d.redset",
    name, rank_world, d->group_id+1, d->groups, d->rank+1, d->ranks
  );
}

/* returns 1 if we successfully read a partner file, 0 otherwise */
static int redset_read_partner_file(
  const char* name,
  const redset_base* d,
  kvtree* header)
{
  /* get partner filename */
  char file[REDSET_MAX_FILENAME];
  redset_build_partner_filename(name, d, file, sizeof(file));

  /* check that we can read the file */
  if (redset_file_is_readable(file) != REDSET_SUCCESS) {
    redset_dbg(2, "Do not have read access to file: %s @ %s:%d",
      file, __FILE__, __LINE__
    );
    return REDSET_FAILURE;
  }

  /* read partner header from file */
  if (kvtree_read_file(file, header) != KVTREE_SUCCESS) {
    return REDSET_FAILURE;
  }

  return REDSET_SUCCESS;
}

/* given a redundancy descriptor with all top level fields filled in
 * allocate and fill in structure for partner specific fields in state */
int redset_construct_partner(MPI_Comm parent_comm, redset_base* d, int replicas)
{
  int rc = REDSET_SUCCESS;

  /* allocate a new structure to hold partner state */
  redset_partner* state = (redset_partner*) REDSET_MALLOC(sizeof(redset_partner));

  /* attach structure to reddesc */
  d->state = (void*) state;

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

  /* record number of replicas */
  state->replicas = replicas;

  /* verify that all groups have a sufficient number of procs,
   * for the requested number of encoding blocks, number of
   * encoding blocks has to be less than number of procs in
   * each redundancy set */
  int valid = 1;
  if (replicas < 1 || replicas >= d->ranks) {
    valid = 0;
  }
  if (! redset_alltrue(valid, parent_comm)) {
    if (! valid) {
      redset_abort(-1, "Number of partner replicas (%d) must be in range [1,%d) with %d ranks in set @ %s:%d",
        replicas, d->ranks, d->ranks, __FILE__, __LINE__
      );
    }
  }

  return rc;
}

int redset_delete_partner(redset_base* d)
{
  redset_partner* state = (redset_partner*) d->state;
  if (state != NULL) {
    /* free strings that we received */
    redset_free(&state->lhs_hostname);
    redset_free(&state->rhs_hostname);

    /* free the structure */
    redset_free(&d->state);
  }
  return REDSET_SUCCESS;
}

/* copy our redundancy descriptor info to a partner */
int redset_store_to_kvtree_partner(
  const redset_base* d,
  kvtree* hash)
{
  int rc = REDSET_SUCCESS;

  /* get pointer to partner state structure */
  redset_partner* state = (redset_partner*) d->state;

  /* record number of replicas */
  kvtree_util_set_int(hash, "REPLICAS", state->replicas);

  return rc;
}

/* this extracts parameters from the hash that are needed
 * in order to call create_partner */
int redset_read_from_kvtree_partner(
  const kvtree* hash,
  int* outreplicas)
{
  int rc = REDSET_SUCCESS;

  /* get number of replicas from hash */
  if (kvtree_util_get_int(hash, "REPLICAS", outreplicas) != KVTREE_SUCCESS) {
    rc = REDSET_FAILURE;
  }

  return rc;
}

/* copy our redundancy descriptor info to a partner */
int redset_encode_reddesc_partner(
  kvtree* hash,
  const char* name,
  const redset_base* d)
{
  int rc = REDSET_SUCCESS;

  /* get pointer to partner state structure */
  redset_partner* state = (redset_partner*) d->state;

  /* make a copy of the hash we want to encode */
  kvtree* send_hash = kvtree_new();
  kvtree_merge(send_hash, hash);

  /* we copy this hash to match the number of encoding blocks */
  int i;
  for (i = 1; i <= state->replicas; i++) {
    /* get ranks of procs to our left and right sides */
    int lhs_rank = (d->rank - i + d->ranks) % d->ranks;
    int rhs_rank = (d->rank + i + d->ranks) % d->ranks;

    /* send our redundancy descriptor hash to the right,
     * receive incoming hash from left neighbors */
    kvtree* partner_hash = kvtree_new();
    kvtree_sendrecv(send_hash, rhs_rank, partner_hash, lhs_rank, d->comm);

    /* store partner hash in our under its name */
    kvtree_merge(hash, partner_hash);
    kvtree_delete(&partner_hash);
  }

  /* delete our copy */
  kvtree_delete(&send_hash);

  return rc;
}

int redset_apply_partner(
  int numfiles,
  const char** files,
  const char* name,
  const redset_base* d)
{
  int i;
  int rc = REDSET_SUCCESS;

  /* pick out communicator */
  MPI_Comm comm = d->comm;

  /* get pointer to partner state structure */
  redset_partner* state = (redset_partner*) d->state;

  /* allocate a structure to record meta data about our files and redundancy descriptor */
  kvtree* current_hash = kvtree_new();

  /* record info about our data files */
  redset_lofi_encode_kvtree(current_hash, numfiles, files);

  /* open our logical file for reading */
  redset_lofi rsf;
  if (redset_lofi_open(current_hash, O_RDONLY, (mode_t)0, &rsf) != REDSET_SUCCESS) {
    redset_abort(-1, "Opening data files for copying: @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* store our redundancy descriptor in hash */
  kvtree* desc_hash = kvtree_new();
  redset_store_to_kvtree(d, desc_hash);
  kvtree_set(current_hash, REDSET_KEY_COPY_PARTNER_DESC, desc_hash);

  /* copy meta data to hash */
  kvtree* header = kvtree_new();

  /* record our rank within our redundancy group */
  kvtree_set_kv_int(header, REDSET_KEY_COPY_PARTNER_GROUP_RANK, d->rank);

  /* copy meta data to hash */
  kvtree_setf(header, current_hash, "%s %d", REDSET_KEY_COPY_PARTNER_DESC, d->rank);

  /* copy our descriptor N times to other ranks so it can be recovered
   * with to the same degree as our encoding scheme */
  for (i = 1; i <= state->replicas; i++) {
    /* get ranks of procs to our left and right sides */
    int lhs_rank = (d->rank - i + d->ranks) % d->ranks;
    int rhs_rank = (d->rank + i + d->ranks) % d->ranks;

    /* send our redundancy descriptor hash to the right,
     * receive incoming hash from left neighbors */
    kvtree* partner_hash = kvtree_new();
    kvtree_sendrecv(current_hash, rhs_rank, partner_hash, lhs_rank, comm);

    /* attach hash from this neighbor to our header */
    kvtree_setf(header, partner_hash, "%s %d", REDSET_KEY_COPY_PARTNER_DESC, lhs_rank);
  }

  /* define file name for our redundancy file */
  char partner_file[REDSET_MAX_FILENAME];
  redset_build_partner_filename(name, d, partner_file, sizeof(partner_file));

  /* open the redundancy file */
  mode_t mode_file = redset_getmode(1, 1, 0);
  int fd_partner = redset_open(partner_file, O_WRONLY | O_CREAT | O_TRUNC, mode_file);
  if (fd_partner < 0) {
    /* TODO: try again? */
    redset_abort(-1, "Opening partner file for writing: redset_open(%s) errno=%d %s @ %s:%d",
      partner_file, errno, strerror(errno), __FILE__, __LINE__
    );
  }

  /* sort the header to list items alphabetically,
   * this isn't strictly required, but it ensures the kvtrees
   * are stored in the same byte order so that we can reproduce
   * the redundancy file identically on a rebuild */
  redset_sort_kvtree(header);

  /* write out the partner header */
  kvtree_write_fd(partner_file, fd_partner, header);
  kvtree_delete(&header);

  /* get offset into file immediately following the header */
  off_t header_size = lseek(fd_partner, 0, SEEK_CUR);

  /* allocate buffer to read a piece of my file */
  unsigned char** send_bufs = (unsigned char**) redset_buffers_alloc(1, redset_mpi_buf_size);

  /* allocate a receive buffer for each partner */
  unsigned char** recv_bufs = (unsigned char**) redset_buffers_alloc(state->replicas, redset_mpi_buf_size);

  /* number of bytes summed across all of our files */
  unsigned long outgoing = redset_lofi_bytes(&rsf);

  /* determine how many bytes are coming from each of our partners */
  unsigned long* incoming = (unsigned long*) REDSET_MALLOC(state->replicas * sizeof(unsigned long));
  unsigned long* received = (unsigned long*) REDSET_MALLOC(state->replicas * sizeof(unsigned long));
  unsigned long* offsets  = (unsigned long*) REDSET_MALLOC(state->replicas * sizeof(unsigned long));
  for (i = 0; i < state->replicas; i++) {
    /* get ranks of procs to our left and right sides */
    int lhs_rank = (d->rank - (i + 1) + d->ranks) % d->ranks;
    int rhs_rank = (d->rank + (i + 1) + d->ranks) % d->ranks;

    /* send them our byte count and receive theirs */
    MPI_Status st;
    MPI_Sendrecv(
      &outgoing,    1, MPI_UNSIGNED_LONG, rhs_rank, 0,
      &incoming[i], 1, MPI_UNSIGNED_LONG, lhs_rank, 0,
      comm, &st
    );

    /* this will track amount of data we have received from each partner */
    received[i] = 0;

    /* tracks starting offset into our redundancy file to store data for each partner */
    if (i == 0) {
      /* first partner file goes right after our header */
      offsets[0] = 0;
    } else {
      /* each partner file is appended after the one before */
      offsets[i] = offsets[i - 1] + incoming[i - 1];
    }
  }

  /* for each potential file, step through a call to swap */
  MPI_Request* request = (MPI_Request*) REDSET_MALLOC(state->replicas * 2 * sizeof(MPI_Request));
  MPI_Status*  status  = (MPI_Status*)  REDSET_MALLOC(state->replicas * 2 * sizeof(MPI_Status));

  int done = 0;
  unsigned long send_offset = 0;
  while (! done) {
    /* we'll assume this is the last iteration,
     * and unset this flag if any process still has data at the end */
    done = 1;

    /* use this to track number of outstanding MPI operations */
    int k = 0;

    /* post receive buffers for each partner sending to us */
    for (i = 0; i < state->replicas; i++) {
      /* get rank to our left for this step */
      int lhs_rank = (d->rank - (i + 1) + d->ranks) % d->ranks;

      /* compute number of incoming bytes in this step */
      size_t recv_count = (size_t) (incoming[i] - received[i]);
      if (recv_count > redset_mpi_buf_size) {
        recv_count = redset_mpi_buf_size;
      }

      /* post receive for incoming data, if any */
      if (recv_count > 0) {
        MPI_Irecv(recv_bufs[i], recv_count, MPI_BYTE, lhs_rank, 0, d->comm, &request[k]);
        k++;
      }
    }

    /* compute number of outgoing bytes in this step */
    size_t send_count = (size_t) (outgoing - send_offset);
    if (send_count > redset_mpi_buf_size) {
      send_count = redset_mpi_buf_size;
    }

    /* read data from files */
    if (send_count > 0) {
      if (redset_lofi_pread(&rsf, send_bufs[0], send_count, send_offset) != REDSET_SUCCESS)
      {
        rc = REDSET_FAILURE;
      }
    }

    /* send our data to each partner to our right */
    for (i = 0; i < state->replicas; i++) {
      /* get rank of right partner in this step */
      int rhs_rank = (d->rank + (i + 1) + d->ranks) % d->ranks;

      /* send data if we have any */
      if (send_count > 0) {
        MPI_Isend(send_bufs[0], send_count, MPI_BYTE, rhs_rank, 0, d->comm, &request[k]);
        k++;
      }
    }

    /* wait for communication to complete */
    MPI_Waitall(k, request, status);

    /* write received data to our partner file */
    for (i = 0; i < state->replicas; i++) {
      /* compute number of incoming bytes in this step */
      size_t recv_count = (size_t) (incoming[i] - received[i]);
      if (recv_count > redset_mpi_buf_size) {
        recv_count = redset_mpi_buf_size;
      }

      /* nothing to do if this partner has sent us everything */
      if (recv_count == 0) {
        continue;
      }

      /* write block to partner file */
      off_t offset = offsets[i] + received[i] + header_size;
      if (redset_lseek(partner_file, fd_partner, offset, SEEK_SET) != REDSET_SUCCESS) {
        rc = REDSET_FAILURE;
      }
      if (redset_write_attempt(partner_file, fd_partner, recv_bufs[i], recv_count) != recv_count) {
        rc = REDSET_FAILURE;
      }

      /* update number of bytes we're received from this partner */
      received[i] += recv_count;

      /* unset done flag if we still need data from this partner */
      if (received[i] < incoming[i]) {
        done = 0;
      }
    }

    /* update number of bytes we have sent */
    send_offset += send_count;

    /* unset done flag if we still need to send data */
    if (send_offset < outgoing) {
      done = 0;
    }
  }

  /* close my partner */
  if (redset_close(partner_file, fd_partner) != REDSET_SUCCESS) {
    rc = REDSET_FAILURE;
  }

  /* close my data files */
  if (redset_lofi_close(&rsf) != REDSET_SUCCESS) {
    rc = REDSET_FAILURE;
  }

  /* free the buffers */
  redset_buffers_free(state->replicas, &recv_bufs);
  redset_buffers_free(1, &send_bufs);

  redset_free(&incoming);
  redset_free(&received);
  redset_free(&offsets);

  redset_free(&request);
  redset_free(&status);

  return rc;
}

int redset_recover_partner_rebuild(
  const char* name,
  const redset_base* d,
  int have_my_files)
{
  int i;
  int rc = REDSET_SUCCESS;
  MPI_Comm comm_world = d->parent_comm;

  /* pick out communicator */
  MPI_Comm comm = d->comm;

  /* get pointer to partner state structure */
  redset_partner* state = (redset_partner*) d->state;

  MPI_Request* request = (MPI_Request*) REDSET_MALLOC(state->replicas * 2 * sizeof(MPI_Request));
  MPI_Status*  status  = (MPI_Status*)  REDSET_MALLOC(state->replicas * 2 * sizeof(MPI_Status));

  /* let our partners know if we need our files */
  int need_files = (have_my_files == 0);

  int k = 0;
  int* lhs_need = (int*) REDSET_MALLOC(state->replicas * sizeof(int));
  for (i = 0; i < state->replicas; i++) {
    /* get rank to our left for this step */
    int lhs_rank = (d->rank - (i + 1) + d->ranks) % d->ranks;
    int rhs_rank = (d->rank + (i + 1) + d->ranks) % d->ranks;

    /* determine whether left parnter needs their files */
    MPI_Irecv(&lhs_need[i], 1, MPI_INT, lhs_rank, 0, d->comm, &request[k]);
    k++;

    /* let our left partner know whether we need our files */
    MPI_Isend(&need_files, 1, MPI_INT, rhs_rank, 0, d->comm, &request[k]);
    k++;
  }

  /* wait for communication to complete */
  MPI_Waitall(k, request, status);

  k = 0;
  int* rhs_need = (int*) REDSET_MALLOC(state->replicas * sizeof(int));
  for (i = 0; i < state->replicas; i++) {
    /* get rank to our left for this step */
    int lhs_rank = (d->rank - (i + 1) + d->ranks) % d->ranks;
    int rhs_rank = (d->rank + (i + 1) + d->ranks) % d->ranks;

    /* determine whether left parnter needs their files */
    MPI_Irecv(&rhs_need[i], 1, MPI_INT, rhs_rank, 0, d->comm, &request[k]);
    k++;

    /* let our left partner know whether we need our files */
    MPI_Isend(&need_files, 1, MPI_INT, lhs_rank, 0, d->comm, &request[k]);
    k++;
  }

  /* wait for communication to complete */
  MPI_Waitall(k, request, status);

  /* if we're missing our files and all of our partners are
   * also missing files, we're out of luck */
  int can_rebuild = 1;
  if (need_files) {
    /* we're missing our files,
     * check whether our partners have them */
    can_rebuild = 0;
    for (i = 0; i < state->replicas; i++) {
      if (! rhs_need[i]) {
        /* found someone who has our files,
         * so we can rebuild */
        can_rebuild = 1;
      }
    }
  }

  /* determine whether all processes can rebuild */
  if (! redset_alltrue(can_rebuild, comm_world)) {
    return REDSET_FAILURE;
  }

  redset_lofi rsf;
  int fd_partner = -1;

  kvtree* send_hash = NULL;
  kvtree* recv_hash = NULL;

  /* allocate a structure to record meta data about our files and redundancy descriptor */
  kvtree* current_hash = NULL;

  /* get partner filename */
  char partner_file[REDSET_MAX_FILENAME];
  redset_build_partner_filename(name, d, partner_file, sizeof(partner_file));

  /* allocate hash object to read in (or receive) the header of the partner file */
  kvtree* header = kvtree_new();

  /* size of header as encoded in redundancy file */
  off_t header_size = 0;

  /* if process to our left or right is missing files, send along header info
   * so that process can write its header again */
  if (! need_files) {
    /* open our partner file for reading */
    fd_partner = redset_open(partner_file, O_RDONLY);
    if (fd_partner < 0) {
      redset_abort(-1, "Opening partner file for reading in partner rebuild: redset_open(%s, O_RDONLY) errno=%d %s @ %s:%d",
        partner_file, errno, strerror(errno), __FILE__, __LINE__
      );
    }

    /* read in the partner header */
    kvtree_read_fd(partner_file, fd_partner, header);

    /* get offset into file immediately following the header */
    header_size = lseek(fd_partner, 0, SEEK_CUR);

    /* get file info for this rank */
    current_hash = kvtree_getf(header, "%s %d", REDSET_KEY_COPY_PARTNER_DESC, d->rank);

    /* open our logical file for reading */
    if (redset_lofi_open(current_hash, O_RDONLY, (mode_t)0, &rsf) != REDSET_SUCCESS) {
      redset_abort(-1, "Failed to open data files to read for rebuild: %s @ %s:%d",
        partner_file, __FILE__, __LINE__
      );
    }

    /* if failed rank is to my left, i have its file info, send it the header */
    send_hash = kvtree_new();
    recv_hash = kvtree_new();
    for (i = 0; i < state->replicas; i++) {
      int lhs_rank = (d->rank - (i + 1) + d->ranks) % d->ranks;
      if (lhs_need[i]) {
        kvtree* payload = kvtree_new();
        kvtree_merge(payload, header);
        kvtree_setf(send_hash, payload, "%d", lhs_rank);
      }
    }
    kvtree_exchange(send_hash, recv_hash, d->comm);
    kvtree_delete(&recv_hash);
    kvtree_delete(&send_hash);
  } else {
    /* this process failed, read our metadata from another process
     * we get our header from any rank that might have a copy */
    send_hash = kvtree_new();
    recv_hash = kvtree_new();
    kvtree_exchange(send_hash, recv_hash, d->comm);

    /* get our descriptor from first entry we find,
     * they are all the same */
    kvtree_elem* desc_elem = kvtree_elem_first(recv_hash);
    int source_rank = kvtree_elem_key_int(desc_elem);
    kvtree* desc_hash = kvtree_elem_hash(desc_elem);
    kvtree_merge(header, desc_hash);

    kvtree_delete(&recv_hash);
    kvtree_delete(&send_hash);

    /* overwrite rank of partner with our own rank in the redundancy group */
    kvtree_set_kv_int(header, REDSET_KEY_COPY_PARTNER_GROUP_RANK, d->rank);

    /* get our current hash from header we received */
    current_hash = kvtree_getf(header, "%s %d", REDSET_KEY_COPY_PARTNER_DESC, d->rank);

    /* unset descriptors for ranks other than our partners */
    desc_hash = kvtree_get(header, REDSET_KEY_COPY_PARTNER_DESC);
    for (i = 0; i <= state->replicas; i++) {
      /* step through entries the source rank would have */
      int lhs_rank = (source_rank - i + d->ranks) % d->ranks;

      /* don't delete our own entry */
      if (lhs_rank == d->rank) {
        continue;
      }

      /* TODO: do this more cleanly */
      /* have to define the rank as a string */
      char rankstr[1024];
      snprintf(rankstr, sizeof(rankstr), "%d", lhs_rank);

      /* now we can delete this entry */
      kvtree_unset(desc_hash, rankstr);
    }

    /* get permissions for file */
    mode_t mode_file = redset_getmode(1, 1, 0);

    /* open our logical file for writing */
    if (redset_lofi_open(current_hash, O_RDWR | O_CREAT | O_TRUNC, mode_file, &rsf) != REDSET_SUCCESS) {
      redset_abort(-1, "Failed to open data files for writing during rebuild @ %s:%d",
        __FILE__, __LINE__
      );
    }

    /* open my redundancy file for writing */
    fd_partner = redset_open(partner_file, O_WRONLY | O_CREAT | O_TRUNC, mode_file);
    if (fd_partner < 0) {
      /* TODO: try again? */
      redset_abort(-1, "Opening redundancy file for writing in rebuild: redset_open(%s) errno=%d %s @ %s:%d",
        partner_file, errno, strerror(errno), __FILE__, __LINE__
      );
    }
  }

  /* if failed rank is to my right, send it my file info so it can write its header */
  send_hash = kvtree_new();
  recv_hash = kvtree_new();
  for (i = 0; i < state->replicas; i++) {
    int rhs_rank = (d->rank + (i + 1) + d->ranks) % d->ranks;
    if (rhs_need[i]) {
      kvtree* payload = kvtree_new();
      kvtree_merge(payload, current_hash);
      kvtree_setf(send_hash, payload, "%d", rhs_rank);
    }
  }
  kvtree_exchange(send_hash, recv_hash, d->comm);

  if (need_files) {
    /* receive copy of file info from left-side partners,
     * we'll store a copy of their headers for redudancy */
    kvtree_elem* desc_elem;
    for (desc_elem = kvtree_elem_first(recv_hash);
         desc_elem != NULL;
         desc_elem = kvtree_elem_next(desc_elem))
    {
      /* get source rank that sent this descriptor */
      char* rank_key = kvtree_elem_key(desc_elem);

      /* get the descriptor that was sent to us */
      kvtree* desc_hash = kvtree_elem_hash(desc_elem);

      /* make a copy of it */
      kvtree* partner_hash = kvtree_new();
      kvtree_merge(partner_hash, desc_hash);

      /* attach the copy to our header */
      kvtree_setf(header, partner_hash, "%s %s", REDSET_KEY_COPY_PARTNER_DESC, rank_key);
    }

    /* sort the header to list items alphabetically,
     * this isn't strictly required, but it ensures the kvtrees
     * are stored in the same byte order so that we can reproduce
     * the redundancy file identically on a rebuild */
    redset_sort_kvtree(header);

    /* write partner file header */
    kvtree_write_fd(partner_file, fd_partner, header);

    /* get offset into file immediately following the header */
    header_size = lseek(fd_partner, 0, SEEK_CUR);
  }

  kvtree_delete(&recv_hash);
  kvtree_delete(&send_hash);

  /* allocate buffer to read a piece of my file */
  unsigned char** send_bufs = (unsigned char**) redset_buffers_alloc(1, redset_mpi_buf_size);

  /* allocate a receive buffer for each partner */
  unsigned char** recv_bufs = (unsigned char**) redset_buffers_alloc(state->replicas, redset_mpi_buf_size);

  /* get file size of our logical file */
  unsigned long bytes = redset_lofi_bytes(&rsf);

  unsigned long outgoing = bytes;
  unsigned long* incoming = (unsigned long*) REDSET_MALLOC(state->replicas * sizeof(unsigned long));
  unsigned long* received = (unsigned long*) REDSET_MALLOC(state->replicas * sizeof(unsigned long));
  unsigned long* offsets  = (unsigned long*) REDSET_MALLOC(state->replicas * sizeof(unsigned long));
  for (i = 0; i < state->replicas; i++) {
    /* get ranks of procs to our left and right sides */
    int lhs_rank = (d->rank - (i + 1) + d->ranks) % d->ranks;
    int rhs_rank = (d->rank + (i + 1) + d->ranks) % d->ranks;

    /* send them our byte count and receive theirs */
    MPI_Status st;
    MPI_Sendrecv(
      &outgoing,    1, MPI_UNSIGNED_LONG, rhs_rank, 0,
      &incoming[i], 1, MPI_UNSIGNED_LONG, lhs_rank, 0,
      comm, &st
    );

    /* this will track amount of data we have received from each partner */
    received[i] = 0;

    /* tracks starting offset into our redundancy file to store data for each partner */
    if (i == 0) {
      /* first partner file goes right after our header */
      offsets[0] = 0;
    } else {
      /* each partner file is appended after the one before */
      offsets[i] = offsets[i - 1] + incoming[i - 1];
    }
  }

  /* first copy original files for process, then copy partner copies */

  /* TODO: find a more distributed algorithm to spread the load */
  /* for now, assume we have to send if we're the first process with our
   * our left neighbor's files, for each rank to our left */
  if (! need_files) {
    for (i = 0; i < state->replicas; i++) {
      int lhs_rank = (d->rank - (i + 1) + d->ranks) % d->ranks;
      if (lhs_need[i]) {
        /* get number of bytes to send to this partner */
        unsigned long outgoing = incoming[i];

        /* send the data */
        while (received[i] < outgoing) {
          /* compute number of bytes for this step */
          size_t count = (size_t) (outgoing - received[i]);
          if (count > redset_mpi_buf_size) {
            count = redset_mpi_buf_size;
          }

          /* read data from the partner file */
          off_t offset = offsets[i] + received[i] + header_size;
          if (redset_lseek(partner_file, fd_partner, offset, SEEK_SET) != REDSET_SUCCESS) {
            rc = REDSET_FAILURE;
          }
          if (redset_read_attempt(partner_file, fd_partner, send_bufs[0], count) != count) {
            /* read failed, make sure we fail this rebuild */
            rc = REDSET_FAILURE;
          }

          /* send data to left partner */
          MPI_Send(send_bufs[0], count, MPI_BYTE, lhs_rank, 0, d->comm);

          /* sum up the bytes we've sent so far */
          received[i] += count;
        }
      } else {
        /* found someone to our left who has files,
         * so we don't need to send to them and they will
         * send to all others further to the left */
         break;
      }
    }
  }

  /* receive data from right and write out our files */
  if (need_files) {
    for (i = 0; i < state->replicas; i++) {
      /* look for the first rank to our right that has our files */
      int rhs_rank = (d->rank + (i + 1) + d->ranks) % d->ranks;
      if (! rhs_need[i]) {
        /* found someone with our files, let's get them */
        unsigned long offset = 0;
        while (offset < bytes) {
          /* compute number of bytes for this step */
          size_t count = (size_t) (bytes - offset);
          if (count > redset_mpi_buf_size) {
            count = redset_mpi_buf_size;
          }

          /* receive data from right partner */
          MPI_Recv(recv_bufs[i], count, MPI_BYTE, rhs_rank, 0, d->comm, &status[0]);

          /* write data to the logical file */
          if (redset_lofi_pwrite(&rsf, recv_bufs[i], count, offset) != REDSET_SUCCESS)
          {
            /* write failed, make sure we fail this rebuild */
            rc = REDSET_FAILURE;
          }

          offset += count;
        }

        /* we're done after we get them from the first rank */
        break;
      }
    }
  }

  /* reinitialize our received counters */
  for (i = 0; i < state->replicas; i++) {
    received[i] = 0;
  }

  /* determine whether we have to send our file to anyone */
  int need_send = 0;
  for (i = 0; i < state->replicas; i++) {
    if (rhs_need[i]) {
      /* someone to our right failed, so we need to send them our files */
      need_send = 1;
    }
  }

  int done = 0;
  unsigned long send_offset = 0;
  while (! done) {
    /* we'll assume this is the last iteration,
     * and unset this flag if any process still has data at the end */
    done = 1;

    /* use this to track number of outstanding MPI operations */
    k = 0;

    /* post receive buffers for each partner sending to us */
    if (need_files) {
      for (i = 0; i < state->replicas; i++) {
        /* get rank to our left for this step */
        int lhs_rank = (d->rank - (i + 1) + d->ranks) % d->ranks;

        /* compute number of incoming bytes in this step */
        size_t recv_count = (size_t) (incoming[i] - received[i]);
        if (recv_count > redset_mpi_buf_size) {
          recv_count = redset_mpi_buf_size;
        }

        /* post receive for incoming data, if any */
        if (recv_count > 0) {
          MPI_Irecv(recv_bufs[i], recv_count, MPI_BYTE, lhs_rank, 0, d->comm, &request[k]);
          k++;
        }
      }
    }

    /* compute number of outgoing bytes in this step */
    size_t send_count = (size_t) (outgoing - send_offset);
    if (send_count > redset_mpi_buf_size) {
      send_count = redset_mpi_buf_size;
    }

    /* send data out if we need to */
    if (need_send) {
      /* read data from files */
      if (send_count > 0) {
        if (redset_lofi_pread(&rsf, send_bufs[0], send_count, send_offset) != REDSET_SUCCESS)
        {
          rc = REDSET_FAILURE;
        }
      }

      /* send our data to each partner to our right */
      for (i = 0; i < state->replicas; i++) {
        if (rhs_need[i]) {
          /* get rank of right partner in this step */
          int rhs_rank = (d->rank + (i + 1) + d->ranks) % d->ranks;

          /* send data if we have any */
          if (send_count > 0) {
            MPI_Isend(send_bufs[0], send_count, MPI_BYTE, rhs_rank, 0, d->comm, &request[k]);
            k++;
          }
        }
      }
    }

    /* wait for communication to complete */
    MPI_Waitall(k, request, status);

    /* write received data to our partner file */
    if (need_files) {
      for (i = 0; i < state->replicas; i++) {
        /* compute number of incoming bytes in this step */
        size_t recv_count = (size_t) (incoming[i] - received[i]);
        if (recv_count > redset_mpi_buf_size) {
          recv_count = redset_mpi_buf_size;
        }

        /* nothing to do if this partner has sent us everything */
        if (recv_count == 0) {
          continue;
        }

        /* write block to partner file */
        off_t offset = offsets[i] + received[i] + header_size;
        if (redset_lseek(partner_file, fd_partner, offset, SEEK_SET) != REDSET_SUCCESS) {
          rc = REDSET_FAILURE;
        }
        if (redset_write_attempt(partner_file, fd_partner, recv_bufs[i], recv_count) != recv_count) {
          rc = REDSET_FAILURE;
        }

        /* update number of bytes we're received from this partner */
        received[i] += recv_count;

        /* unset done flag if we still need data from this partner */
        if (received[i] < incoming[i]) {
          done = 0;
        }
      }
    }

    /* determine whether we're still sending data */
    if (need_send) {
      /* update number of bytes we have sent */
      send_offset += send_count;

      /* unset done flag if we still need to send data */
      if (send_offset < outgoing) {
        done = 0;
      }
    }
  }

  /* close my partner */
  if (redset_close(partner_file, fd_partner) != REDSET_SUCCESS) {
    rc = REDSET_FAILURE;
  }

  /* close my data files */
  if (redset_lofi_close(&rsf) != REDSET_SUCCESS) {
    rc = REDSET_FAILURE;
  }

  /* reapply metadata properties to file: uid, gid, mode bits, timestamps,
   * we do this on every file instead of just the rebuilt files so that we preserve atime on all files */
  redset_lofi_apply_meta(current_hash);

  /* free the buffers */
  redset_free(&rhs_need);
  redset_free(&lhs_need);

  redset_free(&request);
  redset_free(&status);

  redset_free(&incoming);
  redset_free(&received);
  redset_free(&offsets);

  redset_buffers_free(state->replicas, &recv_bufs);
  redset_buffers_free(1, &send_bufs);

  kvtree_delete(&header);

  return rc;
}

int redset_recover_partner(
  const char* name,
  const redset_base* d)
{
  int rc = REDSET_SUCCESS;
  MPI_Comm comm_world = d->parent_comm;

  /* assume we have our files */
  int have_my_files = 1;

  /* check whether we have our files and our partner's files */
  kvtree* header = kvtree_new();
  if (redset_read_partner_file(name, d, header) == REDSET_SUCCESS) {
    /* get pointer to hash for this rank */
    kvtree* current_hash = kvtree_getf(header, "%s %d", REDSET_KEY_COPY_PARTNER_DESC, d->rank);
    if (redset_lofi_check(current_hash) != REDSET_SUCCESS) {
      have_my_files = 0;
    }
  } else {
    /* failed to read partner file */
    have_my_files = 0;
  }
  kvtree_delete(&header);

  /* attempt to rebuild if any process is missing its files */
  if (! redset_alltrue(have_my_files, comm_world)) {
    rc = redset_recover_partner_rebuild(name, d, have_my_files);
  }

  /* determine whether all ranks have their files */
  if (! redset_alltrue(rc == REDSET_SUCCESS, comm_world)) {
    rc = REDSET_FAILURE;
  }

  return rc;
}

int redset_unapply_partner(
  const char* name,
  const redset_base* d)
{
  /* get name of partner file */
  char partner_file[REDSET_MAX_FILENAME];
  redset_build_partner_filename(name, d, partner_file, sizeof(partner_file));

  int rc = redset_file_unlink(partner_file);

  return rc;
}

/* returns a list of files added by redundancy descriptor */
redset_list* redset_filelist_enc_get_partner(
  const char* name,
  redset_base* d)
{
  char file[REDSET_MAX_FILENAME];
  redset_build_partner_filename(name, d, file, sizeof(file));
  redset_list* list = (redset_list*) REDSET_MALLOC(sizeof(redset_list));
  list->count = 1;
  list->files = (const char**) REDSET_MALLOC(sizeof(char*));
  list->files[0] = strdup(file);
  return list;
}

/* returns a list of original files encoded by redundancy descriptor */
redset_list* redset_filelist_orig_get_partner(
  const char* name,
  const redset_base* d)
{
  redset_list* list = NULL;

  /* check whether we have our files and our partner's files */
  kvtree* header = kvtree_new();
  if (redset_read_partner_file(name, d, header) == REDSET_SUCCESS) {
    /* get pointer to hash for this rank */
    kvtree* current_hash = kvtree_getf(header, "%s %d", REDSET_KEY_COPY_PARTNER_DESC, d->rank);
    list = redset_lofi_filelist(current_hash);
  }
  kvtree_delete(&header);

  return list;
}
