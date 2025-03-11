#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>

#include <limits.h>
#include <unistd.h>

#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "mpi.h"

#include "kvtree.h"
#include "kvtree_util.h"
#include "kvtree_mpi.h"
#include "rankstr_mpi.h"

#include "redset_util.h"
#include "redset.h"
#include "redset_internal.h"
#include "redset_io.h"

#define REDSET_HOSTNAME (255)

/** default set size for redset to use */
int redset_set_size = 8;

int redset_encode_method = REDSET_ENCODE_CPU;

int redset_init(void)
{
  /* read our hostname */
  char hostname[REDSET_HOSTNAME + 1];
  gethostname(hostname, sizeof(hostname));
  redset_hostname = strdup(hostname);

  /* get page size */
  redset_page_size = sysconf(_SC_PAGESIZE);

  /* set MPI buffer size */
  redset_mpi_buf_size = 1024 * 1024;

  const char* val = getenv("REDSET_ENCODE");
  if (val != NULL) {
    if (strcmp(val, "CPU") == 0) {
      redset_encode_method = REDSET_ENCODE_CPU;
    } else if (strcmp(val, "OPENMP") == 0) {
      redset_encode_method = REDSET_ENCODE_OPENMP;
    } else if (strcmp(val, "PTHREADS") == 0) {
      redset_encode_method = REDSET_ENCODE_PTHREADS;
    } else if (strcmp(val, "CUDA") == 0) {
      redset_encode_method = REDSET_ENCODE_CUDA;
    } else {
      redset_abort(-1, "Unknown encode type %s in REDSET_ENCODE @ %s:%d",
        val, __FILE__, __LINE__
      );
    }
  }

  /* set our global rank */
  MPI_Comm_rank(MPI_COMM_WORLD, &redset_rank);

  return REDSET_SUCCESS;
}

int redset_finalize(void)
{
  redset_free(&redset_hostname);
  return REDSET_SUCCESS;
}

kvtree* redset_config_set(const kvtree *config)
{
  kvtree* retval = (kvtree*)config;
  assert(config != NULL);

  static const char* known_options[] = {
    REDSET_KEY_CONFIG_DEBUG,
    REDSET_KEY_CONFIG_SET_SIZE,
    REDSET_KEY_CONFIG_MPI_BUF_SIZE,
    NULL
  };

  /* read out all options we know about */
  kvtree_util_get_int(config,
    REDSET_KEY_CONFIG_DEBUG, &redset_debug);

  kvtree_util_get_int(config,
    REDSET_KEY_CONFIG_SET_SIZE, &redset_set_size);

  unsigned long ul;
  if (kvtree_util_get_bytecount(config,
    REDSET_KEY_CONFIG_MPI_BUF_SIZE, &ul) == KVTREE_SUCCESS)
  {
    redset_mpi_buf_size = (int) ul;
    if (redset_mpi_buf_size != ul) {
      char *value;
      kvtree_util_get_str(config, REDSET_KEY_CONFIG_MPI_BUF_SIZE, &value);
      redset_err("Value '%s' passed for %s exceeds int range",
        value, REDSET_KEY_CONFIG_MPI_BUF_SIZE
      );
      retval = NULL;
    }
  }

  /* report all unknown options (typos?) */
  const kvtree_elem* elem;
  for (elem = kvtree_elem_first(config);
       elem != NULL;
       elem = kvtree_elem_next(elem))
  {
#ifndef NDEBUG
    /* must be only one level deep, ie plain kev = value */
    const kvtree* elem_hash = kvtree_elem_hash(elem);
    assert(kvtree_size(elem_hash) == 1);

    const kvtree* kvtree_first_elem_hash =
      kvtree_elem_hash(kvtree_elem_first(elem_hash));
    assert(kvtree_size(kvtree_first_elem_hash) == 0);
#endif

    /* check against known options */
    const char** opt;
    int found = 0;
    for (opt = known_options; *opt != NULL; opt++) {
      if (strcmp(*opt, kvtree_elem_key(elem)) == 0) {
        found = 1;
        break;
      }
    }
    if (! found) {
      redset_err(
        "Unknown configuration parameter '%s' with value '%s'",
        kvtree_elem_key(elem),
        kvtree_elem_key(kvtree_elem_first(kvtree_elem_hash(elem)))
      );
      retval = NULL;
    }
  }

  return retval;
}

static kvtree* redset_config_get(void)
{
  kvtree* retval = kvtree_new();
  assert(retval != NULL);

  int success = 1;

  if (kvtree_util_set_int(retval, REDSET_KEY_CONFIG_DEBUG, redset_debug) !=
    KVTREE_SUCCESS)
  {
    success = 0;
  }

  if (kvtree_util_set_int(retval, REDSET_KEY_CONFIG_SET_SIZE, redset_set_size) !=
    KVTREE_SUCCESS)
  {
    success = 0;
  }

  if (kvtree_util_set_int(retval, REDSET_KEY_CONFIG_MPI_BUF_SIZE,
    redset_mpi_buf_size) != KVTREE_SUCCESS)
  {
    success = 0;
  }

  if (!success) {
    kvtree_delete(&retval);
  }

  return retval;
}

/* get / set redset configuration options */
kvtree* redset_config(const kvtree *config)
{
  if (config != NULL) {
    return redset_config_set(config);
  } else {
    return redset_config_get();
  }
  return NULL; /* NOTREACHED */
}

/* given a comm as input, find the left and right partner
 * ranks and hostnames */
int redset_set_partners(
  MPI_Comm parent_comm, MPI_Comm comm, int dist,
  int* lhs_rank, int* lhs_rank_world, char** lhs_hostname,
  int* rhs_rank, int* rhs_rank_world, char** rhs_hostname)
{
  /* find our position in the communicator */
  int my_rank, ranks;
  MPI_Comm_rank(comm, &my_rank);
  MPI_Comm_size(comm, &ranks);

  /* shift parter distance to a valid range */
  while (dist > ranks) {
    dist -= ranks;
  }
  while (dist < 0) {
    dist += ranks;
  }

  /* compute ranks to our left and right partners */
  int lhs = (my_rank + ranks - dist) % ranks;
  int rhs = (my_rank + ranks + dist) % ranks;
  (*lhs_rank) = lhs;
  (*rhs_rank) = rhs;

  /* shift hostnames to the right */
  redset_str_sendrecv(redset_hostname, rhs, lhs_hostname, lhs, comm);

  /* shift hostnames to the left */
  redset_str_sendrecv(redset_hostname, lhs, rhs_hostname, rhs, comm);

  MPI_Request request[2];
  MPI_Status  status[2];

  /* get our rank in parent communicator */
  int parent_rank;
  MPI_Comm_rank(parent_comm, &parent_rank);

  /* shift rank in comm_world to the right */
  MPI_Irecv(lhs_rank_world, 1, MPI_INT, lhs, 0, comm, &request[0]);
  MPI_Isend(&parent_rank,    1, MPI_INT, rhs, 0, comm, &request[1]);
  MPI_Waitall(2, request, status);

  /* shift rank in comm_world to the left */
  MPI_Irecv(rhs_rank_world, 1, MPI_INT, rhs, 0, comm, &request[0]);
  MPI_Isend(&parent_rank,    1, MPI_INT, lhs, 0, comm, &request[1]);
  MPI_Waitall(2, request, status);

  return REDSET_SUCCESS;
}

/*
=========================================
Redundancy descriptor functions
=========================================
*/

/* initialize the specified redundancy descriptor */
static int redset_initialize(redset_base* d)
{
  /* check that we got a valid redundancy descriptor */
  if (d == NULL) {
    printf("No redundancy descriptor to fill from hash @ %s:%d",
      __FILE__, __LINE__
    );
    return REDSET_FAILURE;
  }

  /* initialize the descriptor */
  d->enabled        =  0;
  d->type           = REDSET_COPY_NULL;
  d->state          = NULL;
  d->parent_comm    = MPI_COMM_NULL;
  d->comm           = MPI_COMM_NULL;
  d->groups         =  0;
  d->group_id       = -1;
  d->ranks          =  0;
  d->rank           = MPI_PROC_NULL;

  return REDSET_SUCCESS;
}

/* free any memory associated with the specified redundancy
 * descriptor */
int redset_delete(redset* dvp)
{
  /* get pointer to redset structure */
  redset_base* d = (redset_base*) *dvp;

  /* free off copy type specific data */
  switch (d->type) {
  case REDSET_COPY_SINGLE:
    break;
  case REDSET_COPY_PARTNER:
    redset_delete_partner(d);
    break;
  case REDSET_COPY_XOR:
    redset_delete_xor(d);
    break;
  case REDSET_COPY_RS:
    redset_delete_rs(d);
    break;
  }

  /* free the communicator we created */
  if (d->parent_comm != MPI_COMM_NULL) {
    MPI_Comm_free(&d->parent_comm);
  }
  if (d->comm != MPI_COMM_NULL) {
    MPI_Comm_free(&d->comm);
  }

  /* free the redset structure and set caller's pointer to NULL */
  redset_free(dvp);

  return REDSET_SUCCESS;
}

/* convert the specified redundancy descritpor into a corresponding
 * kvtree */
int redset_to_kvtree(const redset_base* d, kvtree* hash)
{
  /* check that we got a valid pointer to a redundancy descriptor and
   * a hash */
  if (d == NULL || hash == NULL) {
    return REDSET_FAILURE;
  }

  /* clear the hash */
  kvtree_unset_all(hash);

  /* set the ENABLED key */
  kvtree_set_kv_int(hash, REDSET_KEY_CONFIG_ENABLED, d->enabled);

  /* set the TYPE key */
  switch (d->type) {
  case REDSET_COPY_SINGLE:
    kvtree_set_kv(hash, REDSET_KEY_CONFIG_TYPE, "SINGLE");
    break;
  case REDSET_COPY_PARTNER:
    kvtree_set_kv(hash, REDSET_KEY_CONFIG_TYPE, "PARTNER");
    break;
  case REDSET_COPY_XOR:
    kvtree_set_kv(hash, REDSET_KEY_CONFIG_TYPE, "XOR");
    break;
  case REDSET_COPY_RS:
    kvtree_set_kv(hash, REDSET_KEY_CONFIG_TYPE, "RS");
    break;
  }

  /* we don't set the LHS or RHS values because they are dependent on
   * runtime environment */

  /* we don't set the COMM because this is dependent on runtime
   * environment */

  /* set the GROUP_ID and GROUP_RANK keys, we use this info to rebuild
   * our communicator later */
  kvtree_set_kv_int(hash, REDSET_KEY_CONFIG_GROUPS,     d->groups);
  kvtree_set_kv_int(hash, REDSET_KEY_CONFIG_GROUP_ID,   d->group_id);
  kvtree_set_kv_int(hash, REDSET_KEY_CONFIG_GROUP_SIZE, d->ranks);
  kvtree_set_kv_int(hash, REDSET_KEY_CONFIG_GROUP_RANK, d->rank);

  return REDSET_SUCCESS;
}

/* given our rank within a set of ranks and minimum group size,
 * divide the set as evenly as possible and return the
 * group id corresponding to our rank */
static int redset_group_id(
  int rank,
  int ranks,
  int minsize,
  int* group_id)
{
  /* compute maximum number of full minsize groups we can fit within
   * ranks */
  int groups = ranks / minsize;

  /* compute number of ranks left over */
  int remainder_ranks = ranks - groups * minsize;

  /* determine base size for each group */
  int size = ranks;
  if (groups > 0) {
    /* evenly distribute remaining ranks over the groups that we have */
    int add_to_each_group = remainder_ranks / groups;
    size = minsize + add_to_each_group;
  }

  /* compute remaining ranks assuming we have groups of the new base
   * size */
  int remainder = ranks % size;

  /* for each remainder rank, we increase the lower groups by a size
   * of one, so that we end up with remainder groups of size+1 followed
   * by (groups - remainder) of size */

  /* cutoff is the first rank for which all groups are exactly size */
  int cutoff = remainder * (size + 1);

  if (rank < cutoff) {
    /* ranks below cutoff are grouped into sets of size+1 */
    *group_id = rank / (size + 1);
  } else {
    /* ranks at cutoff and higher are grouped into sets of size */
    *group_id = (rank - cutoff) / size + remainder;
  }

  return REDSET_SUCCESS;
}

/* given a parent communicator and a communicator representing our group
 * within the parent, split parent into other communicators consisting
 * of all procs with same rank within its group */
static int redset_split_across(
  MPI_Comm comm_parent,
  MPI_Comm comm_group,
  MPI_Comm* comm_across)
{
  /* TODO: this works well if each comm has about the same number of
   * procs, but we need something better to handle unbalanced groups */

  /* get rank of this process within parent communicator */
  int rank_parent;
  MPI_Comm_rank(comm_parent, &rank_parent);

  /* get rank of this process within group communicator */
  int rank_group;
  MPI_Comm_rank(comm_group, &rank_group);

  /* Split procs in parent into groups containing all procs with same
   * rank within group, order by rank in parent */
  MPI_Comm_split(comm_parent, rank_group, rank_parent, comm_across);

  return REDSET_SUCCESS;
}

/* convert copy type string to integer value */
static int redset_type_int_from_str(const char* value, int* outtype)
{
  int rc = REDSET_SUCCESS;

  int type = REDSET_COPY_NULL;
  if (strcasecmp(value, "SINGLE") == 0) {
    type = REDSET_COPY_SINGLE;
  } else if (strcasecmp(value, "PARTNER") == 0) {
    type = REDSET_COPY_PARTNER;
  } else if (strcasecmp(value, "XOR") == 0) {
    type = REDSET_COPY_XOR;
  } else if (strcasecmp(value, "RS") == 0) {
    type = REDSET_COPY_RS;
  } else {
    if (redset_rank == 0) {
      redset_warn("Unknown copy type %s @ %s:%d",
        value, __FILE__, __LINE__
      );
    }
    rc = REDSET_FAILURE;
  }

  *outtype = type;
  return rc;
}

/* build a redundancy descriptor corresponding to the specified hash,
 * this function is collective */
static int redset_create_base(
  int type,
  MPI_Comm comm,
  const char* group_name,
  int set_size,
  int k,
  redset* dvp)
{
  /* allocate a new redset structure */
  redset_base* d = (redset_base*) REDSET_MALLOC(sizeof(redset_base));

  /* set caller's pointer to record address of redset structure */
  *dvp = (void*) d;

  /* initialize the descriptor */
  redset_initialize(d);

  /* assume it's enabled, we may turn this bit off later */
  d->enabled = 1;

  /* record type of descriptor */
  d->type = type;

  /* dup the parent communicator */
  MPI_Comm_dup(comm, &d->parent_comm);

  /* split procs from comm into sub communicators based on group_name,
   * this puts all procs with the same group_name into the same subcomm */
  MPI_Comm comm_fail;
  rankstr_mpi_comm_split(d->parent_comm, group_name, 0, 0, 1, &comm_fail);

  /* build our redundancy communicator based on the copy type
   * and other parameters */
  MPI_Comm comm_across;
  int rank_across, ranks_across, split_id;
  switch (d->type) {
  case REDSET_COPY_SINGLE:
    /* not going to communicate with anyone, so just dup COMM_SELF */
    MPI_Comm_dup(MPI_COMM_SELF, &d->comm);
    break;
  case REDSET_COPY_PARTNER:
  case REDSET_COPY_XOR:
  case REDSET_COPY_RS:
    /* split the communicator across groups based on set size
     * to create our redundancy group communicator */

    /* split comm world across failure groups */
    redset_split_across(comm, comm_fail, &comm_across);

    /* get our rank and the number of ranks in this communicator */
    MPI_Comm_rank(comm_across, &rank_across);
    MPI_Comm_size(comm_across, &ranks_across);

    /* identify which group we're in */
    redset_group_id(rank_across, ranks_across, set_size, &split_id);

    /* split communicator into sets */
    MPI_Comm_split(comm_across, split_id, redset_rank, &d->comm);

    /* free the temporary communicator */
    MPI_Comm_free(&comm_across);
    break;
  }

  /* find our position in the reddesc communicator */
  MPI_Comm_rank(d->comm, &d->rank);
  MPI_Comm_size(d->comm, &d->ranks);

  /* count the number of groups */
  int group_leader = (d->rank == 0) ? 1 : 0;
  MPI_Allreduce(&group_leader, &d->groups, 1, MPI_INT, MPI_SUM, comm);

  /* assign group ids, execute a scan which will increment
   * a running count each time a group leader is found,
   * then bcast the group id from leader to all ranks in its group */
  MPI_Scan(&group_leader, &d->group_id, 1, MPI_INT, MPI_SUM, comm);
  MPI_Bcast(&d->group_id, 1, MPI_INT, 0, d->comm);
  d->group_id--;

  /* fill in state struct depending on copy type */
  switch (d->type) {
  case REDSET_COPY_SINGLE:
    break;
  case REDSET_COPY_PARTNER:
    redset_construct_partner(comm, d, k);
    break;
  case REDSET_COPY_XOR:
    redset_construct_xor(comm, d);
    break;
  case REDSET_COPY_RS:
    redset_construct_rs(comm, d, k);
    break;
  }

  /* free communicator of all procs in the same group */
  MPI_Comm_free(&comm_fail);

  return REDSET_SUCCESS;
}

/* build a redundancy descriptor corresponding to the specified hash,
 * this function is collective */
int redset_create(
  int type,
  MPI_Comm comm,
  const char* group_name,
  redset* dvp)
{
  int set_size = redset_set_size;

  int k = 1;
  if (type == REDSET_COPY_RS) {
    k = 2;
  }

  int rc = redset_create_base(type, comm, group_name, set_size, k, dvp);
  return rc;
}

/* build a redundancy descriptor for SINGLE encoding */
int redset_create_single(
  MPI_Comm comm,
  const char* group_name,
  redset* dvp)
{
  int set_size = redset_set_size;
  int k = 1;
  int rc = redset_create_base(REDSET_COPY_SINGLE, comm, group_name, set_size, k, dvp);
  return rc;
}

/* build a redundancy descriptor for PARTNER encoding */
int redset_create_partner(
  MPI_Comm comm,
  const char* group_name,
  int set_size,
  int replicas,
  redset* dvp)
{
  int rc = redset_create_base(REDSET_COPY_PARTNER, comm, group_name, set_size, replicas, dvp);
  return rc;
}

/* build a redundancy descriptor for XOR encoding */
int redset_create_xor(
  MPI_Comm comm,
  const char* group_name,
  int set_size,
  redset* dvp)
{
  int k = 1;
  int rc = redset_create_base(REDSET_COPY_XOR, comm, group_name, set_size, k, dvp);
  return rc;
}

/* build a redundancy descriptor for Reed-Solomon encoding */
int redset_create_rs(
  MPI_Comm comm,
  const char* group_name,
  int set_size,
  int k,
  redset* dvp)
{
  int rc = redset_create_base(REDSET_COPY_RS, comm, group_name, set_size, k, dvp);
  return rc;
}

/* convert the specified redundancy descritpor into a corresponding
 * kvtree */
int redset_store_to_kvtree(const redset_base* d, kvtree* hash)
{
  /* check that we got a valid pointer to a redundancy descriptor and
   * a hash */
  if (d == NULL || hash == NULL) {
    return REDSET_FAILURE;
  }

  /* clear the hash */
  kvtree_unset_all(hash);

  /* set the ENABLED key */
  kvtree_set_kv_int(hash, REDSET_KEY_CONFIG_ENABLED, d->enabled);

  /* set the TYPE key */
  switch (d->type) {
  case REDSET_COPY_SINGLE:
    kvtree_set_kv(hash, REDSET_KEY_CONFIG_TYPE, "SINGLE");
    break;
  case REDSET_COPY_PARTNER:
    kvtree_set_kv(hash, REDSET_KEY_CONFIG_TYPE, "PARTNER");
    redset_store_to_kvtree_partner(d, hash);
    break;
  case REDSET_COPY_XOR:
    kvtree_set_kv(hash, REDSET_KEY_CONFIG_TYPE, "XOR");
    break;
  case REDSET_COPY_RS:
    kvtree_set_kv(hash, REDSET_KEY_CONFIG_TYPE, "RS");
    redset_store_to_kvtree_rs(d, hash);
    break;
  }

  /* we don't set the LHS or RHS values because they are dependent on
   * runtime environment */

  /* we don't set the COMM because this is dependent on runtime
   * environment */

  /* set the GROUP_ID and GROUP_RANK keys, we use this info to rebuild
   * our communicator later */
  kvtree_set_kv_int(hash, REDSET_KEY_CONFIG_GROUPS,     d->groups);
  kvtree_set_kv_int(hash, REDSET_KEY_CONFIG_GROUP_ID,   d->group_id);
  kvtree_set_kv_int(hash, REDSET_KEY_CONFIG_GROUP_SIZE, d->ranks);
  kvtree_set_kv_int(hash, REDSET_KEY_CONFIG_GROUP_RANK, d->rank);

  /* store our global rank and size */
  int rank, ranks;
  MPI_Comm_rank(d->parent_comm, &rank);  
  MPI_Comm_size(d->parent_comm, &ranks);  
  kvtree_set_kv_int(hash, REDSET_KEY_CONFIG_WORLD_RANK, rank);
  kvtree_set_kv_int(hash, REDSET_KEY_CONFIG_WORLD_SIZE, ranks);

  return REDSET_SUCCESS;
}

/* read values from hash and fill in corresponding descriptor fields */
static int redset_read_from_kvtree(
  const kvtree* hash,
  redset_base* d)
{
  /* enable / disable the descriptor */
  d->enabled = 1;
  kvtree_util_get_int(hash, REDSET_KEY_CONFIG_ENABLED, &(d->enabled));

  /* read the redundancy scheme type from the hash,
   * and build our redundancy communicator */
  char* type;
  if (kvtree_util_get_str(hash, REDSET_KEY_CONFIG_TYPE, &type) == REDSET_SUCCESS) {
    if (redset_type_int_from_str(type, &d->type) != REDSET_SUCCESS) {
      d->enabled = 0;
      if (redset_rank == 0) {
        redset_abort(-1, "Unknown copy type %s in redundancy descriptor hash @ %s:%d",
          type, __FILE__, __LINE__
        );
      }
    }
  } else {
    redset_abort(-1, "Unknown copy type in redundancy descriptor hash @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* build the group communicator */
  if (kvtree_util_get_int(hash, REDSET_KEY_CONFIG_GROUPS, &d->groups) != REDSET_SUCCESS) {
    redset_abort(-1, "Failed to read group count in redundancy descriptor hash @ %s:%d",
      __FILE__, __LINE__
    );
  }
  if (kvtree_util_get_int(hash, REDSET_KEY_CONFIG_GROUP_ID, &d->group_id) != REDSET_SUCCESS) {
    redset_abort(-1, "Failed to read group id in redundancy descriptor hash @ %s:%d",
      __FILE__, __LINE__
    );
  }
  if (kvtree_util_get_int(hash, REDSET_KEY_CONFIG_GROUP_RANK, &d->rank) != REDSET_SUCCESS) {
    redset_abort(-1, "Failed to read group rank in redundancy descriptor hash @ %s:%d",
      __FILE__, __LINE__
    );
  }
  if (kvtree_util_get_int(hash, REDSET_KEY_CONFIG_GROUP_SIZE, &d->ranks) != REDSET_SUCCESS) {
    redset_abort(-1, "Failed to read group size in redundancy descriptor hash @ %s:%d",
      __FILE__, __LINE__
    );
  }

  return REDSET_SUCCESS;
}

/* build a redundancy descriptor corresponding to the specified kvtree,
 * this function is collective, it is the opposite of store_to_kvtree */
int redset_restore_from_kvtree(
  const kvtree* hash,
  redset_base* d)
{
  /* it's required that the caller has already initialized the descriptor
   * and dup'ed the parent comm before calling this function, if we expose
   * this function to the user we should revisit this interface */
  // redset_initialize(d);

  /* fill in fields from hash (no MPI allowed) */
  redset_read_from_kvtree(hash, d);

  // MPI_Comm_dup(comm, &d->parent_comm);
  MPI_Comm comm = d->parent_comm;

  /* build the group communicator */
  MPI_Comm_split(comm, d->group_id, d->rank, &d->comm);

  /* fill in state struct depending on copy type */
  int partner_replicas = 0;
  int rs_encoding = 0;
  switch (d->type) {
  case REDSET_COPY_SINGLE:
    break;
  case REDSET_COPY_PARTNER:
    redset_read_from_kvtree_partner(hash, &partner_replicas);
    redset_construct_partner(comm, d, partner_replicas);
    break;
  case REDSET_COPY_XOR:
    redset_construct_xor(comm, d);
    break;
  case REDSET_COPY_RS:
    /* read number of encoding blocks from hash to pass to create call */
    redset_read_from_kvtree_rs(hash, &rs_encoding);
    redset_construct_rs(comm, d, rs_encoding);
    break;
  }

  /* if anyone has disabled this, everyone needs to */
  if (! redset_alltrue(d->enabled, comm)) {
    d->enabled = 0;
  }

  return REDSET_SUCCESS;
}

/* checks whether specifed file exists, is readable, and is complete */
int redset_bool_have_file(const char* file)
{
  /* if no filename is given return false */
  if (file == NULL || strcmp(file,"") == 0) {
    redset_dbg(2, "File name is null or the empty string @ %s:%d",
      __FILE__, __LINE__
    );
    return 0;
  }

  /* check that we can read the file */
  if (redset_file_is_readable(file) != REDSET_SUCCESS) {
    redset_dbg(2, "Do not have read access to file: %s @ %s:%d",
      file, __FILE__, __LINE__
    );
    return 0;
  }

#if 0
  /* check that the file size matches */
  unsigned long size = scr_file_size(file);
  unsigned long meta_size = 0;
  if (scr_meta_get_filesize(meta, &meta_size) != SCR_SUCCESS) {
    scr_dbg(2, "Failed to read filesize field in meta data: %s @ %s:%d",
      file, __FILE__, __LINE__
    );
    scr_meta_delete(&meta);
    return 0;
  }
  if (size != meta_size) {
    scr_dbg(2, "Filesize is incorrect, currently %lu, expected %lu for %s @ %s:%d",
      size, meta_size, file, __FILE__, __LINE__
    );
    scr_meta_delete(&meta);
    return 0;
  }

  /* TODO: check that crc32 match if set (this would be expensive) */
#endif

  /* if we made it here, assume the file is good */
  return 1;
}

/* check whether we have all files for a given rank of a given dataset */
int redset_bool_have_files(
  int numfiles,
  const char** files,
  const char* name,
  const redset_base* d)
{
  /* check the integrity of each of the files */
  int missing_a_file = 0;
  int i;
  for (i = 0; i < numfiles; i++) {
    const char* file = files[i];
    if (! redset_bool_have_file(file)) {
      missing_a_file = 1;
    }
  }
  if (missing_a_file) {
    return 0;
  }

  /* if we make it here, we have all of our files */
  return 1;
}

static int redset_build_filename(
  const char* name,
  int rank,
  char* file,
  size_t len)
{
  snprintf(file, len, "%s%d.redset", name, rank);
  return REDSET_SUCCESS;
}

/* encode redundancy descriptor and write to a file */
static int redset_encode_reddesc(
  const char* name,
  const redset_base* d)
{
  /* get name of this process */
  int rank_world;
  MPI_Comm comm_world = d->parent_comm;
  MPI_Comm_rank(comm_world, &rank_world);

  /* allocate a structure to record meta data about our files and redundancy descriptor */
  kvtree* current_hash = kvtree_new();

  /* store our redundancy descriptor in hash */
  kvtree* desc_hash = kvtree_new();
  redset_store_to_kvtree(d, desc_hash);

  kvtree_set(current_hash, "DESC", desc_hash);

  /* copy meta data to hash */
  kvtree* meta_hash = kvtree_new();
  kvtree_setf(meta_hash, current_hash, "%d", rank_world);

  /* apply redundancy to hash data according to selected scheme,
   * we need this hash to be recoverable to the same degree that
   * the redundancy scheme protects data */
  int rc = REDSET_FAILURE;
  switch (d->type) {
  case REDSET_COPY_SINGLE:
    rc = redset_encode_reddesc_single(meta_hash, name, d);
    break;
  case REDSET_COPY_PARTNER:
    rc = redset_encode_reddesc_partner(meta_hash, name, d);
    break;
  case REDSET_COPY_XOR:
    rc = redset_encode_reddesc_xor(meta_hash, name, d);
    break;
  case REDSET_COPY_RS:
    rc = redset_encode_reddesc_rs(meta_hash, name, d);
    break;
  }

  /* sort the header to list items alphabetically,
   * this isn't strictly required, but it ensures the kvtrees
   * are stored in the same byte order so that we can reproduce
   * the redundancy file identically on a rebuild */
  redset_sort_kvtree(meta_hash);

  /* write meta data to file */
  char filename[REDSET_MAX_FILENAME];
  redset_build_filename(name, rank_world, filename, sizeof(filename));
  kvtree_write_file(filename, meta_hash);

  /* delete the hash */
  kvtree_delete(&meta_hash);

  return rc;
}

/* delete files added in encode_reddesc */
static int redset_unencode_reddesc(
  const char* name,
  const redset_base* d)
{
  /* get our rank */
  int rank_world;
  MPI_Comm_rank(d->parent_comm, &rank_world);

  /* delete meta data file */
  char filename[REDSET_MAX_FILENAME];
  redset_build_filename(name, rank_world, filename, sizeof(filename));
  int rc = redset_file_unlink(filename);
  return rc;
}

/* attempt to recover and rebuild redundancy descriptor information */
static int redset_recover_reddesc(
  const char* name,
  redset_base* d)
{
  /* get parent communicator (set by caller) */
  MPI_Comm comm_world = d->parent_comm;

  /* get name of this process */
  int rank_world;
  MPI_Comm_rank(comm_world, &rank_world);

  /* read meta data from file */
  kvtree* hash = kvtree_new();
  char filename[REDSET_MAX_FILENAME];
  redset_build_filename(name, rank_world, filename, sizeof(filename));
  kvtree_read_file(filename, hash);

  /* create a hash to exchange redundancy descriptors */
  kvtree* send_hash = kvtree_new();

  /* determine which processes we have redundancy descriptors for,
   * and include them in the send hash */
  kvtree_elem* elem;
  for (elem = kvtree_elem_first(hash);
       elem != NULL;
       elem = kvtree_elem_next(elem))
  {
    /* get rank of this element */
    int rank = kvtree_elem_key_int(elem);

    kvtree* rank_hash = kvtree_elem_hash(elem);

    /* see if we have a descriptor for this rank */
    kvtree* desc_hash = kvtree_get(rank_hash, "DESC");
    if (desc_hash != NULL) {
      kvtree* desc_copy = kvtree_new();
      kvtree_merge(desc_copy, desc_hash);
      kvtree_setf(send_hash, desc_copy, "%d", rank);
//      kvtree_exchange_sendq(send_hash, rank, desc_hash);
    }
  }

  kvtree_delete(&hash);

  /* create an empty hash to receive any incoming descriptors */
  /* exchange descriptors with other ranks */
  kvtree* recv_hash = kvtree_new();
  kvtree_exchange(send_hash, recv_hash, comm_world);

  /* check that everyone can get their descriptor */
  int num_desc = kvtree_size(recv_hash);
  if (! redset_alltrue((num_desc > 0), comm_world)) {
    /* we can't fully recover the reddesc,
     * but if this process has its reddesc,
     * extract the fields so that unapply cleans up */
    if (num_desc > 0) {
      kvtree_elem* desc_elem = kvtree_elem_first(recv_hash);
      kvtree* desc_hash = kvtree_elem_hash(desc_elem);
      redset_read_from_kvtree(desc_hash, d);
    }

    kvtree_delete(&recv_hash);
    kvtree_delete(&send_hash);
    redset_dbg(2, "Cannot find process that has my redundancy descriptor @ %s:%d",
      __FILE__, __LINE__
    );
    return REDSET_FAILURE;
  }

  /* just go with the first redundancy descriptor in our list,
   * they should all be the same */
  kvtree_elem* desc_elem = kvtree_elem_first(recv_hash);
  kvtree* desc_hash = kvtree_elem_hash(desc_elem);

  /* rebuild the redundancy descriptor for this dataset */
  redset_restore_from_kvtree(desc_hash, d);

  /* can free our hashes now that we've rebuilt our redundancy descriptor */
  kvtree_delete(&recv_hash);
  kvtree_delete(&send_hash);

  /* reapply encoding to redundancy descriptor */
  int rc = redset_encode_reddesc(name, d);

  return rc;
}

/* apply redundancy scheme to list of files and given redundancy
 * descriptor, scheme may store redundany data
 * and associate it with given name */
int redset_apply(
  int numfiles,
  const char** files,
  const char* name,
  const redset dvp)
{
  /* get pointer to redset structure */
  redset_base* d = (redset_base*) dvp;

  MPI_Comm comm_world = d->parent_comm;

  int rank_world, nranks_world;
  MPI_Comm_rank(comm_world, &rank_world);
  MPI_Comm_size(comm_world, &nranks_world);

#if 0
  /* start timer */
  double time_start;
  if (rank_world == 0) {
    time_start = MPI_Wtime();
  }
#endif

#if 0
  /* step through each of my files for the specified dataset
   * to scan for any incomplete files */
  double my_bytes = 0.0;
  for (i = 0; i < numfiles; i++) {
    /* get file name of this file */
    const char* file_name = files[i];

    /* TODO: check that file exists */

    /* get file size of this file */
    unsigned long file_size = redset_file_size(file_name);

    /* add up the number of bytes on our way through */
    my_bytes += (double) file_size;
  }
#endif

  /* TODO: determine whether everyone's files are good */

  /* apply encoding to redundancy descriptor */
  int rc = redset_encode_reddesc(name, d);

  /* determine whether all processes saved their redundancy info */
  if (! redset_alltrue((rc == REDSET_SUCCESS), comm_world)) {
    /* at least one process failed to encode its redundancy information */
    return REDSET_FAILURE;
  }

  /* apply the redundancy scheme to data files */
  switch (d->type) {
  case REDSET_COPY_SINGLE:
    rc = redset_apply_single(numfiles, files, name, d);
    break;
  case REDSET_COPY_PARTNER:
    rc = redset_apply_partner(numfiles, files, name, d);
    break;
  case REDSET_COPY_XOR:
    rc = redset_apply_xor(numfiles, files, name, d);
    break;
  case REDSET_COPY_RS:
    rc = redset_apply_rs(numfiles, files, name, d);
    break;
  }

  /* determine whether everyone succeeded in their copy */
  if (! redset_alltrue((rc == REDSET_SUCCESS), comm_world)) {
    /* at least one process failed to apply redundancy scheme */
    return rc = REDSET_FAILURE;
  }

#if 0
  /* add up total number of bytes */
  double bytes = 0.0;
  MPI_Allreduce(&my_bytes, &bytes, 1, MPI_DOUBLE, MPI_SUM, comm_world);

  /* stop timer and report performance info */
  if (rank_world == 0) {
    double time_end = MPI_Wtime();
    double time_diff = time_end - time_start;
    double bw = 0.0;
    if (time_diff > 0) {
      bw = bytes / (1024.0 * 1024.0 * time_diff);
    }
    //printf("redset_apply: %f secs, %e bytes, %f MB/s, %f MB/s per proc\n",
    //        time_diff, bytes, bw, bw/(double)nranks_world
    //);

    // TODO: log or report cost somewhere
  }
#endif

  return rc;
}

/* rebuilds files for specified dataset, return redundancy descriptor,
 * returns REDSET_SUCCESS if successful, REDSET_FAILURE otherwise,
 * the same return code is returned to all processes */
int redset_recover(
  MPI_Comm comm,
  const char* name,
  redset* dvp)
{
  /* allocate a new redset structure */
  redset_base* d = (redset_base*) REDSET_MALLOC(sizeof(redset_base));

  /* set caller's pointer to record address of redset structure */
  *dvp = (void*) d;

  /* initialize the descriptor, we do this so that it's always safe for
   * the user to delete it whether we succeed or fail here */
  redset_initialize(d);

  /* create temporary communicator so as not to trample on user's comm */
  MPI_Comm_dup(comm, &d->parent_comm);
  MPI_Comm comm_world = d->parent_comm;

  /* reapply encoding to redundancy descriptor */
  int rc = redset_recover_reddesc(name, d);

  /* determine whether everyone succeeded */
  if (! redset_alltrue((rc == REDSET_SUCCESS), comm_world)) {
    /* at least one process failed to rebuild its redundancy information */
    return REDSET_FAILURE;
  }

  /* now recover data using redundancy scheme, if necessary */
  switch (d->type) {
  case REDSET_COPY_SINGLE:
    rc = redset_recover_single(name, d);
    break;
  case REDSET_COPY_PARTNER:
    rc = redset_recover_partner(name, d);
    break;
  case REDSET_COPY_XOR:
    rc = redset_recover_xor(name, d);
    break;
  case REDSET_COPY_RS:
    rc = redset_recover_rs(name, d);
    break;
  }

  /* determine whether everyone succeeded */
  if (! redset_alltrue((rc == REDSET_SUCCESS), comm_world)) {
    /* at least one process failed to rebuild its data */
    return REDSET_FAILURE;
  }

  return REDSET_SUCCESS;
}

/* deletes redundancy data that was added in redset_apply,
 * which is useful when cleaning up */
int redset_unapply(
  const char* name,
  const redset dvp)
{
  int rc = REDSET_SUCCESS;

  /* get pointer to redset structure */
  redset_base* d = (redset_base*) dvp;

  MPI_Comm comm_world = d->parent_comm;

  /* now remove redset encoding data depending on type, if necessary */
  switch (d->type) {
  case REDSET_COPY_SINGLE:
    rc = redset_unapply_single(name, d);
    break;
  case REDSET_COPY_PARTNER:
    rc = redset_unapply_partner(name, d);
    break;
  case REDSET_COPY_XOR:
    rc = redset_unapply_xor(name, d);
    break;
  case REDSET_COPY_RS:
    rc = redset_unapply_rs(name, d);
    break;
  }

  /* determine whether everyone succeeded */
  if (! redset_alltrue((rc == REDSET_SUCCESS), comm_world)) {
    /* at least one process failed to clean up */
    return REDSET_FAILURE;
  }

  /* remove files encoding the redundancy scheme */
  rc = redset_unencode_reddesc(name, d);

  /* determine whether everyone succeeded */
  if (! redset_alltrue((rc == REDSET_SUCCESS), comm_world)) {
    /* at least one process failed to clean up */
    return REDSET_FAILURE;
  }

  return REDSET_SUCCESS;
}

#if 0
static int redset_from_dir(
  MPI_Comm comm_world,
  const char* name,
  redset_base* d)
{
  /* get name of this process */
  int rank;
  MPI_Comm_rank(comm_world, &rank);

  char rankstr[100];
  snprintf(rankstr, sizeof(rankstr), "%d", rank);

  /* read meta data from file */
  kvtree* hash = kvtree_new();
  char filename[REDSET_MAX_FILENAME];
  redset_build_filename(name, rank, filename, sizeof(filename));
  kvtree_read_file(filename, hash);

  /* get pointer to descriptor hash for this rank */
  kvtree* desc_hash = kvtree_get_kv(hash, rankstr, "DESC");

  /* rebuild the redundancy descriptor for this dataset */
  int rc = redset_restore_from_kvtree(desc_hash, d);

  /* free the hash */
  kvtree_delete(&hash);

  return rc;
}
#endif

/* returns a list of files added by redundancy descriptor */
redset_filelist redset_filelist_enc_get(
  const char* name,
  const redset dvp)
{
  /* get pointer to redset structure */
  redset_base* d = (redset_base*) dvp;

  /* build redundancy descriptor from name */
  //redset d;
  //redset_from_dir(name, &d);

  /* create a temporary file list to record files from redundancy scheme */
  redset_list* tmp = NULL;

  /* get files added by redundancy method */
  switch (d->type) {
  case REDSET_COPY_SINGLE:
    tmp = redset_filelist_enc_get_single(name, d);
    break;
  case REDSET_COPY_PARTNER:
    tmp = redset_filelist_enc_get_partner(name, d);
    break;
  case REDSET_COPY_XOR:
    tmp = redset_filelist_enc_get_xor(name, d);
    break;
  case REDSET_COPY_RS:
    tmp = redset_filelist_enc_get_rs(name, d);
    break;
  }

  /* get our world rank */
  int rank_world;
  MPI_Comm_rank(d->parent_comm, &rank_world);

  /* we have a top level redundancy file in addition to anything added by scheme,
   * allocate space for the full file list */
  int count = tmp->count + 1;
  const char** files = (const char**) REDSET_MALLOC(count * sizeof(char*));

  /* record name of top level file */
  char filename[REDSET_MAX_FILENAME];
  redset_build_filename(name, rank_world, filename, sizeof(filename));
  files[0] = strdup(filename);

  /* record each redundancy file */
  int i;
  for (i = 0; i < tmp->count; i++) {
    files[i+1] = strdup(tmp->files[i]);
  }

  /* free the temporary list */
  redset_filelist listvp = tmp;
  redset_filelist_release(&listvp);

  /* free the descriptor */
  //redset_free(&d);

  /* update fields of return list */
  redset_list* list = (redset_list*) REDSET_MALLOC(sizeof(redset_list));
  list->count = count;
  list->files = files;

  return list;
}

/* returns a list of original files encoded by redundancy descriptor */
redset_filelist redset_filelist_orig_get(
  const char* name,
  const redset dvp)
{
  /* get pointer to redset structure */
  redset_base* d = (redset_base*) dvp;

  /* create a temporary file list to record files from redundancy scheme */
  redset_list* tmp = NULL;

  /* get files added by redundancy method */
  switch (d->type) {
  case REDSET_COPY_SINGLE:
    tmp = redset_filelist_orig_get_single(name, d);
    break;
  case REDSET_COPY_PARTNER:
    tmp = redset_filelist_orig_get_partner(name, d);
    break;
  case REDSET_COPY_XOR:
    tmp = redset_filelist_orig_get_xor(name, d);
    break;
  case REDSET_COPY_RS:
    tmp = redset_filelist_orig_get_rs(name, d);
    break;
  }

  return tmp;
}
