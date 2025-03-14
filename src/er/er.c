#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "mpi.h"

#include "kvtree.h"
#include "kvtree_util.h"
#include "redset.h"
#include "shuffile.h"

#include "er.h"
#include "er_util.h"

#define ER_DIRECTION_NULL (0)

#define ER_STATE_NULL    (0)
#define ER_STATE_CORRUPT (1)
#define ER_STATE_ENCODED (2)

/* names of API state transitions to ensure caller is invoking
 * functions in the correct order */
typedef enum {
  ER_API_STATE_NULL = 0,
  ER_API_STATE_CREATED,
  ER_API_STATE_DISPATCHED,
  ER_API_STATE_COMPLETED
} er_api_state;

/* structure to define a set object */
typedef struct {
  int type;
  er_api_state api_state;
  const char* name;
  int scheme_id;
  MPI_Comm comm_world;
  MPI_Comm comm_store;
  kvtree* files;
  int rc;
} erset;

static int er_scheme_counter = 0;
static int er_set_counter = 0;

static kvtree* er_schemes = NULL;
static kvtree* er_sets = NULL;

static erset* erset_new(int type)
{
  /* allocate a new object */
  erset* set = (erset*) ER_MALLOC(sizeof(erset));

  set->type       = type;
  set->api_state  = ER_API_STATE_NULL;
  set->name       = NULL;
  set->scheme_id  = 0;
  set->comm_world = MPI_COMM_NULL;
  set->comm_store = MPI_COMM_NULL;
  set->files      = kvtree_new();
  set->rc         = ER_FAILURE;

  return set;
}

static void erset_delete(erset** ptr)
{
  if (ptr != NULL) {
    /* get pointer to set structure */
    erset* set = *ptr;

    /* free the name of the set */
    if (set->name != NULL) {
      er_free(&set->name);
    }

    /* TODO: since we haven't dup'd these, we don't need to free them */
    /* free communicators */
    if (set->comm_world != MPI_COMM_NULL) {
      //MPI_Comm_free(&set->comm_world);
      set->comm_world = MPI_COMM_NULL;
    }
    if (set->comm_store != MPI_COMM_NULL) {
      //MPI_Comm_free(&set->comm_store);
      set->comm_store = MPI_COMM_NULL;
    }

    /* free the list of files */
    kvtree_delete(&set->files);

    /* free the object */
    er_free(ptr);
  }

  return;
}

static erset* erset_get(int set_id)
{
  /* lookup set id */
  erset* setptr = NULL;
  kvtree* set = kvtree_get_kv_int(er_sets, "SETS", set_id);
  kvtree_util_get_ptr(set, "STRUCT", (void**)&setptr);
  return setptr;
}

static redset* erscheme_get(int scheme_id)
{
  /* look up entry for this scheme id */
  redset* d = NULL;
  kvtree* scheme = kvtree_get_kv_int(er_schemes, "SCHEMES", scheme_id);
  kvtree_util_get_ptr(scheme, "PTR", (void**)&d);
  return d;
}

/* define the path to the er file */
static void build_er_path(char* file, size_t len, const char* path)
{
  snprintf(file, len, "%s.er", path);
}

/* define the path to the shuffile file */
static void build_shuffile_path(char* file, size_t len, const char* path)
{
  snprintf(file, len, "%s.shuffile", path);
}

/* define the path to the redset file for the specified rank */
static void build_redset_path(char* file, size_t len, const char* path, int rank)
{
  snprintf(file, len, "%s.", path);
}

static void er_state_write(MPI_Comm comm_world, MPI_Comm comm_store, const char* path, int state)
{
  /* get our rank in our storage group */
  int rank_store;
  MPI_Comm_rank(comm_store, &rank_store);

  if (rank_store == 0) {
    /* build name of er state file */
    char er_file[1024];
    build_er_path(er_file, sizeof(er_file), path);

    /* write state to file */
    kvtree* data = kvtree_new();
    kvtree_util_set_int(data, "STATE", state);
    kvtree_write_file(er_file, data);
    kvtree_delete(&data);
  }

  /* wait for everyone to update state */
  MPI_Barrier(comm_world);

  return;
}

static void er_state_read(MPI_Comm comm_world, MPI_Comm comm_store, const char* path, int* state)
{
  /* intialize state to NULL */
  *state = ER_STATE_NULL;

  /* get our rank in our storage group */
  int rank_store;
  MPI_Comm_rank(comm_store, &rank_store);

  if (rank_store == 0) {
    /* build name of er state file */
    char er_file[1024];
    build_er_path(er_file, sizeof(er_file), path);

    /* read state from file */
    kvtree* data = kvtree_new();
    kvtree_read_file(er_file, data);
    kvtree_util_get_int(data, "STATE", state);
    kvtree_delete(&data);
  }

  /* TODO: we could end up with stale state files under cases like:
   * 1) job runs and writes file (file version 1), then dies
   * 2) job runs on different nodes and writes file (file version 2), then dies again
   * 3) job runs back on original nodes (some of which have v1 and some v2)
   *
   * right now, we're just picking the lowest rank that has some version */

  /* agree on state across processes,
   * just go with the lowest rank who has the value */
  int rank_world, ranks_world;
  MPI_Comm_rank(comm_world, &rank_world);
  MPI_Comm_size(comm_world, &ranks_world);

  /* if we have a state value, this process is eligible */
  int valid_rank = ranks_world;
  if (*state != ER_STATE_NULL) {
    valid_rank = rank_world;
  }

  /* compute lowest rank that has a valid value */
  int lowest_rank;
  MPI_Allreduce(&valid_rank, &lowest_rank, 1, MPI_INT, MPI_MIN, comm_world);

  /* get value from lowest rank with valid value,
   * if there was to valid value, state still be set to ER_STATE_NULL */
  if (lowest_rank < ranks_world) {
    MPI_Bcast(state, 1, MPI_INT, lowest_rank, comm_world);
  }

  return;
}

int ER_Init(const char* conf_file)
{
  /* initialize the redundancy library */
  if (redset_init() != REDSET_SUCCESS) {
    /* clean up and return */
    er_err("ER_Init redset_init failed @ %s:%d",
      __FILE__, __LINE__);
    return ER_FAILURE;
  }

  /* initialize the shuffile library */
  if (shuffile_init() != SHUFFILE_SUCCESS) {
    /* clean up and return */
    er_err("ER_Init shuffile_init failed @ %s:%d",
      __FILE__, __LINE__);
    redset_finalize();
    return ER_FAILURE;
  }

  /* allocate maps to track scheme and set data */
  er_schemes = kvtree_new();
  er_sets    = kvtree_new();

  return ER_SUCCESS;
}

int ER_Finalize(void)
{
  /* TODO: free descriptors for any outstanding schemes,
   * probably need to do this in same order on all procs,
   * for now, force user to clean up */
  kvtree* schemes = kvtree_get(er_schemes, "SCHEMES");
  if (kvtree_size(schemes) > 0) {
    er_err("ER_Finalize called before schemes freed @ %s:%d",
      __FILE__, __LINE__);
    return ER_FAILURE;
  }

  /* free outstanding sets */
  kvtree* sets = kvtree_get(er_sets, "SETS");
  if (kvtree_size(sets) > 0) {
    er_err("ER_Finalize called before sets freed @ %s:%d",
      __FILE__, __LINE__);
    return ER_FAILURE;
  }

  /* free maps */
  kvtree_delete(&er_schemes);
  kvtree_delete(&er_sets);

  /* shut down shuffile library */
  if (shuffile_finalize() != SHUFFILE_SUCCESS) {
    er_err("ER_Finalize shuffile_finalize() failed @ %s:%d",
      __FILE__, __LINE__);
    return ER_FAILURE;
  }

  /* shut down redundancy library */
  if (redset_finalize() != REDSET_SUCCESS) {
    er_err("ER_Finalize redset_finalize() failed @ %s:%d",
       __FILE__, __LINE__);
    return ER_FAILURE;
  }

  return ER_SUCCESS;
}

/** Set ER config parameters */
static kvtree* ER_Config_Set(const kvtree* config)
{
  kvtree* retval = (kvtree*)config;
  assert(retval != NULL);

  static const char* known_options[] = {
    ER_KEY_CONFIG_DEBUG,
    ER_KEY_CONFIG_SET_SIZE,
    ER_KEY_CONFIG_MPI_BUF_SIZE,
    ER_KEY_CONFIG_CRC_ON_COPY,
    NULL
  };

  /* read out all options we know about */
  /* TODO: this could be turned into a list of structs */
  kvtree_util_get_int(config, ER_KEY_CONFIG_DEBUG, &er_debug);

  kvtree_util_get_int(config, ER_KEY_CONFIG_SET_SIZE, &er_set_size);

  unsigned long ul;
  if (kvtree_util_get_bytecount(config, ER_KEY_CONFIG_MPI_BUF_SIZE, &ul) ==
      KVTREE_SUCCESS)
  {
    er_mpi_buf_size = (int) ul;
    if (er_mpi_buf_size != ul) {
      char *value;
      kvtree_util_get_str(config, ER_KEY_CONFIG_MPI_BUF_SIZE, &value);
      er_err("Value '%s' passed for %s exceeds int range @ %s:%d",
        value, ER_KEY_CONFIG_MPI_BUF_SIZE, __FILE__, __LINE__
      );
      retval = NULL;
    }
  }
  /* TODO: handle ER_KEY_CONFIG_CRC_ON_COPY */

  /* pass options to redset */
  kvtree* redset_config_values = kvtree_new();

  kvtree_util_set_int(redset_config_values,
    REDSET_KEY_CONFIG_DEBUG, er_debug);

  kvtree_util_set_int(redset_config_values,
    REDSET_KEY_CONFIG_SET_SIZE, er_set_size);

  kvtree_util_set_int(redset_config_values,
    REDSET_KEY_CONFIG_MPI_BUF_SIZE, er_mpi_buf_size);

  if (redset_config(redset_config_values) == NULL) {
    retval = NULL;
  }

  kvtree_delete(&redset_config_values);

  /* pass options to shuffile */
  kvtree* shuffile_config_values = kvtree_new();

  kvtree_util_set_int(shuffile_config_values,
    SHUFFILE_KEY_CONFIG_MPI_BUF_SIZE, er_mpi_buf_size);

  kvtree_util_set_int(shuffile_config_values,
    SHUFFILE_KEY_CONFIG_DEBUG, er_debug);

  if (shuffile_config(shuffile_config_values) == NULL) {
    retval = NULL;
  }

  kvtree_delete(&shuffile_config_values);

  /* report all unknown options (typos?) */
  const kvtree_elem* elem;
  for (elem = kvtree_elem_first(config);
       elem != NULL;
       elem = kvtree_elem_next(elem))
  {
    /* must be only one level deep, ie plain kev = value */
    const kvtree* elem_hash = kvtree_elem_hash(elem);
    assert(kvtree_size(elem_hash) == 1);

    const kvtree* kvtree_first_elem_hash =
      kvtree_elem_hash(kvtree_elem_first(elem_hash));
    assert(kvtree_size(kvtree_first_elem_hash) == 0);

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
      er_err("Unknown configuration parameter '%s' with value '%s' @ %s:%d",
        kvtree_elem_key(elem),
        kvtree_elem_key(kvtree_elem_first(kvtree_elem_hash(elem))),
        __FILE__, __LINE__
      );
      retval = NULL;
    }
  }

  return retval;
}

/** Get ER config parameters */
static kvtree* ER_Config_Get(void)
{
  kvtree* retval = kvtree_new();
  assert(retval != NULL);

  int success = 1;

  if (kvtree_util_set_int(retval, ER_KEY_CONFIG_DEBUG, er_debug) !=
    KVTREE_SUCCESS)
  {
    success = 0;
  }

  if (kvtree_util_set_int(retval, ER_KEY_CONFIG_SET_SIZE, er_set_size) !=
    KVTREE_SUCCESS)
  {
    success = 0;
  }

  if (kvtree_util_set_int(retval, ER_KEY_CONFIG_MPI_BUF_SIZE,
    er_mpi_buf_size) != KVTREE_SUCCESS)
  {
    success = 0;
  }

  if (!success) {
    kvtree_delete(&retval);
  }

  return retval;
}

/** Get / Set ER config parameters */
kvtree* ER_Config(const kvtree* config)
{
  if (config != NULL) {
    return ER_Config_Set(config);
  } else {
    return ER_Config_Get();
  }
  return NULL; /* NOTREACHED */
}

int ER_Create_Scheme(
  MPI_Comm comm,
  const char* failure_domain,
  int data_blocks,
  int erasure_blocks)
{
  if (comm == MPI_COMM_NULL) {
    er_err("ER_Create_scheme comm parameter is MPI_COMM_NULL @ %s:%d",
      __FILE__, __LINE__);
    return -1;
  }

  if (failure_domain == NULL) {
    er_err("ER_Create_scheme failure_domain parameter is null @ %s:%d",
      __FILE__, __LINE__);
    return -1;
  }

  /* check that we can support the scheme the caller is asking for */
  if (data_blocks < 1) {
    /* no data to be encoded, don't know what to do */
    er_err("ER_Create_scheme invalid for data_blocks to be less than 1 @ %s:%d",
      __FILE__, __LINE__);
    return -1;
  }

  /* allocate a new redundancy descriptor */
  redset* d = ER_MALLOC(sizeof(redset));

  int redset_rc = REDSET_SUCCESS;
  if (erasure_blocks == 0) {
    /* SINGLE */
    redset_rc = redset_create_single(comm, failure_domain, d);
  } else if (erasure_blocks == 1) {
    /* XOR */
    redset_rc = redset_create_xor(comm, failure_domain, er_set_size, d);
  } else if (erasure_blocks < data_blocks) {
    /* Reed-Solomon */
    redset_rc = redset_create_rs(comm, failure_domain, er_set_size, erasure_blocks, d);
  } else if (erasure_blocks % data_blocks == 0) {
    /* PARTNER */
    int replicas = erasure_blocks / data_blocks;
    redset_rc = redset_create_partner(comm, failure_domain, er_set_size, replicas, d);
  } else {
    /* some form of Reed-Solomon that we don't support yet */
    er_free(&d);
    return -1;
  }

  /* create the scheme */
  if (redset_rc == REDSET_SUCCESS) {
    /* bump our internal counter */
    er_scheme_counter++;

    /* create a new record in our scheme map */
    kvtree* scheme = kvtree_set_kv_int(er_schemes, "SCHEMES", er_scheme_counter);

    /* record pointer to reddesc in our map */
    kvtree_util_set_ptr(scheme, "PTR", (void*)d);
  } else {
    /* clean up and return */
    kvtree_unset_kv_int(er_schemes, "SCHEMES", er_scheme_counter);
    redset_delete(d);
    er_free(&d);
    return -1;
  }

  return er_scheme_counter;
}

int ER_Free_Scheme(int scheme_id)
{
  int rc = ER_SUCCESS;

  /* look up entry for this scheme id */
  redset* d = erscheme_get(scheme_id);
  if (d != NULL) {
    /* free reddesc */
    if (redset_delete(d) != REDSET_SUCCESS) {
      /* failed to free redundancy descriptor */
      er_err("ER_Free_scheme failed to free redundancy descriptor @ %s:%d",
        __FILE__, __LINE__);
      rc = ER_FAILURE;
    }

    /* TODO: what to do here if redset_delete fails? */

    /* free the pointer to the redundancy descriptor */
    er_free(&d);
  } else {
    /* failed to find pointer to reddesc */
    er_err("ER_Free_scheme failed to find pointer to reddesc @ %s:%d",
      __FILE__, __LINE__);
    rc = ER_FAILURE;
  }

  /* drop scheme entry from our map */
  kvtree_unset_kv_int(er_schemes, "SCHEMES", scheme_id);

  return rc;
}

/* create a named set, and specify whether it should be encoded or recovered */
int ER_Create(MPI_Comm comm_world, MPI_Comm comm_store, const char* name, int direction, int scheme_id)
{
  if ((comm_world == MPI_COMM_NULL) || (comm_store == MPI_COMM_NULL)) {
    er_err("ER_Create comm_store or comm_world parameter is MPI_COMM_NULL @ %s:%d",
      __FILE__, __LINE__);
    return -1;
  }

  /* check that we got a name */
  if (name == NULL || strcmp(name, "") == 0) {
    er_err("ER_Create name parameter is null @ %s:%d",
      __FILE__, __LINE__);
    return -1;
  }

  /* check that we got a valid value for direction */
  if (direction != ER_DIRECTION_ENCODE  &&
      direction != ER_DIRECTION_REBUILD &&
      direction != ER_DIRECTION_REMOVE)
  {
    er_err("ER_Create invalid direction @ %s:%d",
      __FILE__, __LINE__);
    return -1;
  }

  /* allocate object for this set */
  erset* setptr = erset_new(direction);

  /* update our API state on this set */
  setptr->api_state = ER_API_STATE_CREATED;

  /* record operation path */
  setptr->name = strdup(name);

  /* record comms */
  setptr->comm_world = comm_world;
  setptr->comm_store = comm_store;

  /* record scheme id (only valid if DIRECTION is ENCODE) */
  setptr->scheme_id = scheme_id;

  /* bump set counter */
  er_set_counter++;

  /* add an entry for this set */
  kvtree* set = kvtree_set_kv_int(er_sets, "SETS", er_set_counter);

  /* record pointer to our structure */
  kvtree_util_set_ptr(set, "STRUCT", setptr);

  /* when encoding, we need to remember the scheme,
   * it's implied by name on rebuild */
  if (direction == ER_DIRECTION_ENCODE) {
    /* look up entry for this scheme id */
    kvtree* scheme = kvtree_get_kv_int(er_schemes, "SCHEMES", scheme_id);
    if (! scheme) {
      /* failed to find scheme id in map */
      er_err("ER_Create failed to find scheme id in map @ %s:%d",
        __FILE__, __LINE__);
      kvtree_unset_kv_int(er_sets, "SETS", er_set_counter);
      erset_delete(&setptr);
      return -1;
    }
  }

  return er_set_counter;
}

/* adds file to specified set id */
int ER_Add(int set_id, const char* file)
{
  /* check that we got a file name */
  if (file == NULL || strcmp(file, "") == 0) {
    er_err("ER_Add file parameter is NULL @ %s:%d",
      __FILE__, __LINE__);
    return ER_FAILURE;
  }

  /* lookup set id */
  erset* set = erset_get(set_id);
  if (set) {
    /* check that we're in the right state */
    if (set->api_state != ER_API_STATE_CREATED) {
      /* wrong state */
      er_err("ER_Add called in wrong order @ %s:%d",
        __FILE__, __LINE__);
      return ER_FAILURE;
    }

    /* add file to set */
    kvtree_set_kv(set->files, "FILE", file);

    /* TODO: capture current working dir? */
  } else {
    /* failed to find set id */
    er_err("ER_Add failed to find set id %d @ %s:%d",
      set_id, __FILE__, __LINE__);
    return ER_FAILURE;
  }

  return ER_SUCCESS;
}

static int er_encode(MPI_Comm comm_world, MPI_Comm comm_store, int num_files, const char** filenames, const char* path, redset d)
{
  int rc = ER_SUCCESS;

  int rank_world;
  MPI_Comm_rank(comm_world, &rank_world);

  int rank_store;
  MPI_Comm_rank(comm_store, &rank_store);

  /* build name of shuffile file */
  char shuffile_file[1024];
  build_shuffile_path(shuffile_file, sizeof(shuffile_file), path);

  /* TODO: read process name from scheme? */
  char redset_path[1024];
  build_redset_path(redset_path, sizeof(redset_path), path, rank_world);

  /* update data state to CORRUPT */
  er_state_write(comm_world, comm_store, path, ER_STATE_CORRUPT);

  /* apply redundancy */
  if (redset_apply(num_files, filenames, redset_path, d) != REDSET_SUCCESS) {
    /* failed to apply redundancy descriptor */
    rc = ER_FAILURE;
  }

  /* get list of files recording redudancy data */
  redset_filelist red_list = redset_filelist_enc_get(redset_path, d);

  /* allocate space for a new file list to include both app files and redundancy files */
  int red_count = redset_filelist_count(red_list);
  int count = num_files + red_count;
  const char** filenames2 = (const char**) ER_MALLOC(count * sizeof(char*));

  /* fill in list of file names */
  int i;
  for (i = 0; i < num_files; i++) {
    /* application files */
    filenames2[i] = filenames[i];
  }
  for (i = 0; i < red_count; i++) {
    /* redundancy files */
    filenames2[num_files + i] = redset_filelist_file(red_list, i);
  }

  /* associate list of both app files and redundancy files with calling process */
  if (shuffile_create(comm_world, comm_store, count, filenames2, shuffile_file) != SHUFFILE_SUCCESS) {
    /* failed to register files with shuffile */
    rc = ER_FAILURE;
  }

  /* free the new file list */
  er_free(&filenames2);
  redset_filelist_release(&red_list);

  /* TODO: if successful, update state to ENCODED, otherwise CORRUPT */
  if (rc == ER_SUCCESS) {
    er_state_write(comm_world, comm_store, path, ER_STATE_ENCODED);
  }

  return rc;
}

static int er_rebuild(MPI_Comm comm_world, MPI_Comm comm_store, const char* path)
{
  int rc = ER_SUCCESS;

  /* read state file and ensure data is encoded */
  int state;
  er_state_read(comm_world, comm_store, path, &state);
  if (state != ER_STATE_ENCODED) {
    /* if it's not encoded, we can't attempt rebuild */
    return ER_FAILURE;
  }

  /* TODO: read process name from scheme? */
  int rank_world;
  MPI_Comm_rank(comm_world, &rank_world);

  /* build name of shuffile file */
  char shuffile_file[1024];
  build_shuffile_path(shuffile_file, sizeof(shuffile_file), path);

  /* build path to redset file for this process */
  char redset_path[1024];
  build_redset_path(redset_path, sizeof(redset_path), path, rank_world);

  /* update data state to CORRUPT */
  er_state_write(comm_world, comm_store, path, ER_STATE_CORRUPT);

  /* TODO: update state to SHUFFLE */

  /* migrate files back to ranks in case of new rank-to-node mapping */
  shuffile_migrate(comm_world, comm_store, shuffile_file);

  /* TODO: update state to RECOVER */

  /* rebuild files */
  redset d;
  if (redset_recover(comm_world, redset_path, &d) != REDSET_SUCCESS) {
    /* rebuild failed, rc is same value across comm_world */
    rc = ER_FAILURE;
  }

  /* rebuild shuffile file in case we are on a new node */

  /* if the rebuild succeeds, rebuild the shuffile file */
  if (rc == ER_SUCCESS) {
    /* get list of app files and files added by redudancy scheme */
    redset_filelist app_list = redset_filelist_orig_get(redset_path, d);
    redset_filelist red_list  = redset_filelist_enc_get(redset_path, d);

    /* allocate space for a new file list to include both app files and redundancy files */
    int app_count = redset_filelist_count(app_list);
    int red_count = redset_filelist_count(red_list);
    int count = app_count + red_count;
    const char** filenames = (const char**) ER_MALLOC(count * sizeof(char*));

    /* fill in list of file names */
    int i;
    for (i = 0; i < app_count; i++) {
      /* application files */
      filenames[i] = redset_filelist_file(app_list, i);
    }
    for (i = 0; i < red_count; i++) {
      /* redundancy files */
      filenames[app_count + i] = redset_filelist_file(red_list, i);
    }

    /* associate list of both app files and redundancy files with calling process */
    if (shuffile_create(comm_world, comm_store, count, filenames, shuffile_file) != SHUFFILE_SUCCESS) {
      /* failed to register files with shuffile */
      rc = ER_FAILURE;
    }

    /* free the new file list */
    er_free(&filenames);
    redset_filelist_release(&red_list);
    redset_filelist_release(&app_list);
  }

  redset_delete(&d);

  /* if successful, update state to ENCODED, otherwise leave as CORRUPT */
  if (rc == ER_SUCCESS) {
    er_state_write(comm_world, comm_store, path, ER_STATE_ENCODED);
  }

  return rc;
}

static int er_remove(MPI_Comm comm_world, MPI_Comm comm_store, const char* path)
{
  int rc = ER_SUCCESS;

  /* get process name */
  int rank_world;
  MPI_Comm_rank(comm_world, &rank_world);

  /* get rank within storage group */
  int rank_store;
  MPI_Comm_rank(comm_store, &rank_store);

  /* build name of er state file */
  char er_file[1024];
  build_er_path(er_file, sizeof(er_file), path);

  /* build name of shuffile file */
  char shuffile_file[1024];
  build_shuffile_path(shuffile_file, sizeof(shuffile_file), path);

  /* build path to redset file for this process */
  char redset_path[1024];
  build_redset_path(redset_path, sizeof(redset_path), path, rank_world);

  /* update data state to CORRUPT */
  er_state_write(comm_world, comm_store, path, ER_STATE_CORRUPT);

  /* delete association information */
  shuffile_remove(comm_world, comm_store, shuffile_file);

  /* delete redundancy data, could avoid recover step if
   * the descriptor is cached somewhere */
  redset d;
  redset_recover(comm_world, redset_path, &d);
  redset_unapply(redset_path, d);
  redset_delete(&d);

  /* delete er state file */
  if (rank_store == 0) {
    unlink(er_file);
  }

  return rc;
}

/* initiate encode/rebuild operation on specified set id */
int ER_Dispatch(int set_id)
{
  int rc = ER_SUCCESS;

  /* lookup set id */
  erset* set = erset_get(set_id);
  if (set) {
    /* check that we're in the right state */
    if (set->api_state != ER_API_STATE_CREATED) {
      /* wrong state */
      er_err("ER_Dispatch called in wrong state @ %s:%d",
        __FILE__, __LINE__);
      return ER_FAILURE;
    }

    /* get name of set */
    const char* name = set->name;

    /* TODO: allow caller to specify this prefix? */
    /* define prefix to use on all metadata files */
    char path[1024];
    snprintf(path, sizeof(path), "%s.er", name);

    /* get operation (encode, rebuild, remove) */
    int direction = set->type;

    /* get world and store communicators */
    MPI_Comm comm_world = set->comm_world;
    MPI_Comm comm_store = set->comm_store;

    if (direction == ER_DIRECTION_ENCODE) {
      /* determine number of files */
      kvtree* files_hash = kvtree_get(set->files, "FILE");
      int num_files = kvtree_size(files_hash);

      /* allocate space for file names */
      const char** filenames = (const char**) ER_MALLOC(num_files * sizeof(char*));

      /* copy pointers to filenames */
      int i = 0;
      kvtree_elem* elem;
      for (elem = kvtree_elem_first(files_hash);
           elem != NULL;
           elem = kvtree_elem_next(elem))
      {
        const char* file = kvtree_elem_key(elem);
        filenames[i] = file;
        i++;
      }

      /* get scheme id */
      int scheme_id = set->scheme_id;

      /* get scheme */
      redset* dptr = erscheme_get(scheme_id);
      if (dptr) {
        /* apply redundancy to files */
        rc = er_encode(comm_world, comm_store, num_files, filenames, path, *dptr);
      } else {
        /* failed to find scheme id for this set */
        rc = ER_FAILURE;
      }

      /* free list of file names */
      er_free(&filenames);
    } else if (direction == ER_DIRECTION_REBUILD) {
      /* migrate files to new rank locations (if needed),
       * and rebuild missing files (if needed) */
      rc = er_rebuild(comm_world, comm_store, path);
    } else {
      /* delete metadata added when encoding files */
      rc = er_remove(comm_world, comm_store, path);
    }

    /* update our state */
    set->api_state = ER_API_STATE_DISPATCHED;

    /* save rc for TEST and WAIT calls */
    set->rc = rc;
  } else {
    /* failed to find set id */
    er_err("ER_Dispatch failed to find set id @ %s:%d",
      __FILE__, __LINE__);
    rc = ER_FAILURE;
  }

  return rc;
}

/* tests whether ongoing dispatch operation to finish,
 * returns 1 if done, 0 otherwise */
int ER_Test(int set_id)
{
  /* lookup our set */
  erset* set = erset_get(set_id);
  if (set) {
    /* check that we're in the right state */
    if (set->api_state != ER_API_STATE_DISPATCHED) {
      /* wrong state */
      er_err("ER_Test called in wrong order");
      return 0;
    }

    /* TODO: test if done */

    /* update our state to completed */
    set->api_state = ER_API_STATE_COMPLETED;
  } else {
    /* failed to find set id */
    return 0;
  }

  return 1;
}

/* wait for ongoing dispatch operation to finish */
int ER_Wait(int set_id)
{
  int rc = ER_SUCCESS;

  /* lookup our set */
  erset* set = erset_get(set_id);
  if (set) {
    /* check that we're in the right state */
    if (set->api_state != ER_API_STATE_DISPATCHED) {
      /* wrong state */
      er_err("ER_Wait called in wrong order @ %s:%d",
        __FILE__, __LINE__);
      return ER_FAILURE;
    }

    /* TODO: wait on operation */

    /* update our state to completed */
    set->api_state = ER_API_STATE_COMPLETED;

    /* pass return code back to caller */
    rc = set->rc;
  } else {
    /* failed to find set id */
    er_err("ER_Wait failed to find set id @ %s:%d",
      __FILE__, __LINE__);
    return ER_FAILURE;
  }

  return rc;
}

/* free internal resources associated with set id */
int ER_Free(int set_id)
{
  /* lookup our set */
  erset* set = erset_get(set_id);
  if (set) {
    /* check that we're in the right state */
    if (set->api_state != ER_API_STATE_COMPLETED) {
      /* wrong state */
      er_err("ER_Free called in wrong order @ %s:%d",
        __FILE__, __LINE__);
      return ER_FAILURE;
    }

    /* free struct attached to set, if any */
    erset_delete(&set);
  } else {
    /* failed to find set id */
    er_err("ER_Free failed to find set id @ %s:%d",
      __FILE__, __LINE__);
    return ER_FAILURE;
  }

  /* delete the set from our list */
  kvtree_unset_kv_int(er_sets, "SETS", set_id);

  return ER_SUCCESS;
}
