/* to get nsec fields in stat structure */
#define _GNU_SOURCE

/* TODO: ugly hack until we get a configure test */
#if defined(__APPLE__)
#define HAVE_STRUCT_STAT_ST_MTIMESPEC_TV_NSEC 1
#else
#define HAVE_STRUCT_STAT_ST_MTIM_TV_NSEC 1
#endif
// HAVE_STRUCT_STAT_ST_MTIME_N
// HAVE_STRUCT_STAT_ST_UMTIME
// HAVE_STRUCT_STAT_ST_MTIME_USEC

#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include <errno.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <limits.h>
#include <unistd.h>

#include "mpi.h"

#include "kvtree.h"
#include "kvtree_util.h"
#include "kvtree_mpi.h"

#include "shuffile_util.h"
#include "shuffile_io.h"
#include "shuffile.h"

#define SHUFFILE_HOSTNAME (255)

int shuffile_init()
{
  /* read our hostname */
  char hostname[SHUFFILE_HOSTNAME + 1];
  gethostname(hostname, sizeof(hostname));
  shuffile_hostname = strdup(hostname);

  /* get page size */
  shuffile_page_size = sysconf(_SC_PAGESIZE);

  /* set MPI buffer size */
  shuffile_mpi_buf_size = 1024 * 1024;

  /* set our global rank */
  MPI_Comm_rank(MPI_COMM_WORLD, &shuffile_rank);

  return SHUFFILE_SUCCESS;
}

int shuffile_finalize()
{
  shuffile_free(&shuffile_hostname);
  return SHUFFILE_SUCCESS;
}

/* set configuration options */
static kvtree* shuffile_config_set(const kvtree* config)
{
  kvtree* retval = (kvtree*)config;
  assert(retval != NULL);

  static const char* known_options[] = {
    SHUFFILE_KEY_CONFIG_MPI_BUF_SIZE,
    SHUFFILE_KEY_CONFIG_DEBUG,
    NULL
  };

  /* read out all options we know about */
  /* TODO: this could be turned into a list of structs */
  unsigned long ul;
  if (kvtree_util_get_bytecount(config, SHUFFILE_KEY_CONFIG_MPI_BUF_SIZE,
                                &ul) == KVTREE_SUCCESS)
  {
    shuffile_mpi_buf_size = (int) ul;
    if (shuffile_mpi_buf_size != ul) {
      char *value;
      kvtree_util_get_str(config, SHUFFILE_KEY_CONFIG_MPI_BUF_SIZE, &value);
      shuffile_err("Value '%s' passed for %s exceeds int range @ %s:%d",
        value, SHUFFILE_KEY_CONFIG_MPI_BUF_SIZE, __FILE__, __LINE__
      );
      retval = NULL;
    }
  }

  kvtree_util_get_int(config, SHUFFILE_KEY_CONFIG_DEBUG, &shuffile_debug);

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
      shuffile_err("Unknown configuration parameter '%s' with value '%s' @ %s:%d",
        kvtree_elem_key(elem),
        kvtree_elem_key(kvtree_elem_first(kvtree_elem_hash(elem))),
        __FILE__, __LINE__
      );
      retval = NULL;
    }
  }

  return retval;
}

/* get configuration options */
static kvtree* shuffile_config_get(void)
{
  int success = 1;

  kvtree* retval = kvtree_new();
  assert(retval != NULL);

  if (kvtree_util_set_int(retval, SHUFFILE_KEY_CONFIG_MPI_BUF_SIZE,
    shuffile_mpi_buf_size) != KVTREE_SUCCESS)
  {
    success = 0;
  }

  if (kvtree_util_set_int(retval, SHUFFILE_KEY_CONFIG_DEBUG, shuffile_debug) !=
    KVTREE_SUCCESS)
  {
    success = 0;
  }

  if (!success) {
    kvtree_delete(&retval);
  }

  return retval;
}

/** Set a shuffile config parameters */
kvtree* shuffile_config(const kvtree* config)
{
  if (config != NULL) {
    return shuffile_config_set(config);
  } else {
    return shuffile_config_get();
  }
  return NULL; /* NOTREACHED */
}

/* delete each file for given process name, and remove entries from hash */
static int shuffile_unlink(kvtree* hash, int rank)
{
  /* loop over files for given process name and delete them */
  kvtree_elem* elem;
  kvtree* rank_hash = kvtree_get_kv_int(hash, "RANK", rank);
  kvtree* file_hash = kvtree_get(rank_hash, "FILE");
  for (elem = kvtree_elem_first(file_hash);
       elem != NULL;
       elem = kvtree_elem_next(elem))
  {
    /* delete the file */
    const char* file = kvtree_elem_key(elem);
    shuffile_file_unlink(file);
  }

  /* delete entries from hash for this rank */
  kvtree_unset_kv_int(hash, "RANK", rank);

  return 0;
}

/* gather list data to rank 0 in storage communicator,
 * and write data to the specified file name */
static int shuffile_write_file(
  MPI_Comm comm_storage,
  const char* name,
  const kvtree* list)
{
  /* assume we'll succeed */
  int rc = SHUFFILE_SUCCESS;

  /* make a copy of the input data */
  kvtree* list_copy = kvtree_new();
  kvtree_merge(list_copy, list);

  /* send data to root process for our storage group */
  kvtree* send_hash = kvtree_new();
  kvtree* recv_hash = kvtree_new();

  /* send our data to rank 0 of our storage group */
  kvtree_setf(send_hash, list_copy, "%d", 0);
  kvtree_exchange(send_hash, recv_hash, comm_storage);

  /* have rank 0 on in the storage group write the file */
  int rank_storage;
  MPI_Comm_rank(comm_storage, &rank_storage);
  if (rank_storage == 0) {
    /* create object to hold data from receive hash */
    kvtree* filedata = kvtree_new();

    /* merge data from each rank into our list */
    kvtree_elem* elem;
    for (elem = kvtree_elem_first(recv_hash);
         elem != NULL;
         elem = kvtree_elem_next(elem))
    {
      kvtree* elem_hash = kvtree_elem_hash(elem);
      kvtree_merge(filedata, elem_hash);
    }

    /* store list to shuffle file */
    int rc_kvtree = kvtree_write_file(name, filedata);
    if (rc_kvtree != KVTREE_SUCCESS) {
      shuffile_err("shuffile_write_file kvtree_write_file() call failed @ %s:%d",
        __FILE__, __LINE__);
      rc = SHUFFILE_FAILURE;
    }

    /* release temporary object */
    kvtree_delete(&filedata);
  }

  /* release send and receive hashes */
  kvtree_delete(&recv_hash);
  kvtree_delete(&send_hash);

  /* check whether any ranks failed */
  if (! shuffile_alltrue(rc == SHUFFILE_SUCCESS, comm_storage)) {
    rc = SHUFFILE_FAILURE;
  }

  /* no need to free list_copy, as it was attached to send_hash */
  return rc;
}

/* scatter items in file to processes in storage communicator,
 * if owning process is in the communicator, it ends up with
 * its own data, otherwise data is evenly spread among processes */
static int shuffile_read_file(
  MPI_Comm comm,
  MPI_Comm comm_storage,
  const char* name,
  kvtree* list)
{
  /* assume we'll succeed */
  int rc = SHUFFILE_SUCCESS;

  /* get process name */
  int rank_world;
  MPI_Comm_rank(comm, &rank_world);

  /* get size of our storage group */
  int ranks_storage;
  MPI_Comm_size(comm_storage, &ranks_storage);

  /* this will map ranks in storage comm to global process name */
  kvtree* map_hash = kvtree_new();

  /* send a message to rank 0 to tell it our global name */
  kvtree* send_hash = kvtree_new();
  kvtree_setf(send_hash, NULL, "%d %d", 0, rank_world);
  kvtree_exchange(send_hash, map_hash, comm_storage);

  /* reuse our send hash */
  kvtree_delete(&send_hash);
  send_hash = kvtree_new();

  /* have rank 0 on in the storage group read the file */
  int rank_storage;
  MPI_Comm_rank(comm_storage, &rank_storage);
  if (rank_storage == 0) {
    /* read data from shuffile file */
    kvtree* filedata = kvtree_new();
    int rc_kvtree = kvtree_read_file(name, filedata);
    if (rc_kvtree != KVTREE_SUCCESS) {
      /* failed to read the file */
      rc = SHUFFILE_FAILURE;
    }

    int target_rank = 0;
    kvtree* ranks_hash = kvtree_get(filedata, "RANK");
    kvtree_elem* elem;
    for (elem = kvtree_elem_first(ranks_hash);
         elem != NULL;
         elem = kvtree_elem_next(elem))
    {
      /* get the process name */
      const char* name = kvtree_elem_key(elem);

      /* get data for this process */
      kvtree* data = kvtree_elem_hash(elem);

      /* copy data for outgoing send */
      kvtree* tmp = kvtree_new();
      kvtree* tmp_data = kvtree_set_kv(tmp, "RANK", name);
      kvtree_merge(tmp_data, data);

      /* if this process is in our storage group,
       * send its data to it */
      int i;
      int local_rank = -1;
      for (i = 0; i < ranks_storage; i++) {
        if (kvtree_getf(map_hash, "%d %s", i, name) != NULL) {
          /* found it, proc with with name is our local ith process */
          local_rank = i;
          break;
        }
      }

      /* pick a rank to send this to */
      if (local_rank == -1) {
        /* round robin to next task */
        local_rank = target_rank;
        target_rank = (target_rank + 1) % ranks_storage;
      }

      /* append data heading to this rank */
      kvtree* local_data = kvtree_getf(send_hash, "%d", local_rank);
      if (local_data == NULL) {
        kvtree_setf(send_hash, tmp, "%d", local_rank);
      } else {
        kvtree_merge(local_data, tmp);
        kvtree_delete(&tmp);
      }
    }

    /* free data read from file */
    kvtree_delete(&filedata);
  }

  /* send data to processes in the storage group */
  kvtree* recv_hash = kvtree_new();
  kvtree_exchange(send_hash, recv_hash, comm_storage);

  /* get pointer to any data that we got from rank 0,
   * and copy to our outupt hash */
  kvtree* data = kvtree_get(recv_hash, "0");
  kvtree_merge(list, data);

  kvtree_delete(&recv_hash);
  kvtree_delete(&map_hash);
  kvtree_delete(&send_hash);

  /* check whether any ranks failed */
  if (! shuffile_alltrue(rc == SHUFFILE_SUCCESS, comm)) {
    rc = SHUFFILE_FAILURE;
  }

  /* no need to free list_copy, as it was attached to send_hash */
  return rc;
}

/* associate a set of files with a named process */
int shuffile_create(
  MPI_Comm comm,
  MPI_Comm comm_storage,
  int numfiles,
  const char** files,
  const char* name)
{
  if ((comm == MPI_COMM_NULL) || (comm_storage == MPI_COMM_NULL)) {
    shuffile_err("shuffile_create comm or comm_storage parameter is MPI_COMM_NULL @ %s:%d",
      __FILE__, __LINE__);
    return SHUFFILE_FAILURE; 
  }

  if (name == NULL) {
    shuffile_err("shuffile_create name parameter is null @ %s:%d",
      __FILE__, __LINE__);
    return SHUFFILE_FAILURE; 
  }

  int rc = SHUFFILE_SUCCESS;
  MPI_Comm comm_world = comm;

  /* get name of process */
  int rank_world;
  MPI_Comm_rank(comm_world, &rank_world);

  /* create an empty list */
  kvtree* list = kvtree_new();

  /* create a space for data for this process */
  kvtree* rank_hash = kvtree_set_kv_int(list, "RANK", rank_world);

  /* record number of files */
  kvtree_set_kv_int(rank_hash, "FILES", numfiles);

  /* record each file name */
  int i;
  for (i = 0; i < numfiles; i++) {
    const char* file = files[i];
    kvtree_set_kv(rank_hash, "FILE", file);
  }

  /* write data to file */
  rc = shuffile_write_file(comm_storage, name, list);

  /* free the list */
  kvtree_delete(&list);

  return rc;
}

/* drop association information,
 * which is useful when cleaning up */
int shuffile_remove(
  MPI_Comm comm,
  MPI_Comm comm_storage,
  const char* name)
{
  if ((comm == MPI_COMM_NULL) || (comm_storage == MPI_COMM_NULL)) {
    shuffile_err("shuffile_remove name parameter is MPI_COMM_NULL @ %s:%d",
      __FILE__, __LINE__);
    return SHUFFILE_FAILURE; 
  }

  /* have rank 0 on in the storage group delete the file */
  if (name == NULL) {
    shuffile_err("shuffile_remove comm or comm_storage parameter is MPI_COMM_NULL @ %s:%d",
      __FILE__, __LINE__);
    return SHUFFILE_FAILURE; 
  }

  int rank_storage;
  MPI_Comm_rank(comm_storage, &rank_storage);
  if (rank_storage == 0) {
    shuffile_file_unlink(name);
  }
  return SHUFFILE_SUCCESS;
}

static void shuffile_stat_get_atimes(const struct stat* sb, uint64_t* secs, uint64_t* nsecs)
{
    *secs = (uint64_t) sb->st_atime;

#if HAVE_STRUCT_STAT_ST_MTIMESPEC_TV_NSEC
    *nsecs = (uint64_t) sb->st_atimespec.tv_nsec;
#elif HAVE_STRUCT_STAT_ST_MTIM_TV_NSEC
    *nsecs = (uint64_t) sb->st_atim.tv_nsec;
#elif HAVE_STRUCT_STAT_ST_MTIME_N
    *nsecs = (uint64_t) sb->st_atime_n;
#elif HAVE_STRUCT_STAT_ST_UMTIME
    *nsecs = (uint64_t) sb->st_uatime * 1000;
#elif HAVE_STRUCT_STAT_ST_MTIME_USEC
    *nsecs = (uint64_t) sb->st_atime_usec * 1000;
#else
    *nsecs = 0;
#endif
}

static void shuffile_stat_get_mtimes (const struct stat* sb, uint64_t* secs, uint64_t* nsecs)
{
    *secs = (uint64_t) sb->st_mtime;

#if HAVE_STRUCT_STAT_ST_MTIMESPEC_TV_NSEC
    *nsecs = (uint64_t) sb->st_mtimespec.tv_nsec;
#elif HAVE_STRUCT_STAT_ST_MTIM_TV_NSEC
    *nsecs = (uint64_t) sb->st_mtim.tv_nsec;
#elif HAVE_STRUCT_STAT_ST_MTIME_N
    *nsecs = (uint64_t) sb->st_mtime_n;
#elif HAVE_STRUCT_STAT_ST_UMTIME
    *nsecs = (uint64_t) sb->st_umtime * 1000;
#elif HAVE_STRUCT_STAT_ST_MTIME_USEC
    *nsecs = (uint64_t) sb->st_mtime_usec * 1000;
#else
    *nsecs = 0;
#endif
}

static void shuffile_stat_get_ctimes (const struct stat* sb, uint64_t* secs, uint64_t* nsecs)
{
    *secs = (uint64_t) sb->st_ctime;

#if HAVE_STRUCT_STAT_ST_MTIMESPEC_TV_NSEC
    *nsecs = (uint64_t) sb->st_ctimespec.tv_nsec;
#elif HAVE_STRUCT_STAT_ST_MTIM_TV_NSEC
    *nsecs = (uint64_t) sb->st_ctim.tv_nsec;
#elif HAVE_STRUCT_STAT_ST_MTIME_N
    *nsecs = (uint64_t) sb->st_ctime_n;
#elif HAVE_STRUCT_STAT_ST_UMTIME
    *nsecs = (uint64_t) sb->st_uctime * 1000;
#elif HAVE_STRUCT_STAT_ST_MTIME_USEC
    *nsecs = (uint64_t) sb->st_ctime_usec * 1000;
#else
    *nsecs = 0;
#endif
}

static int shuffile_meta_encode(const char* file, kvtree* meta)
{
  struct stat statbuf;
  int rc = shuffile_stat(file, &statbuf);
  if (rc == 0) {
    kvtree_util_set_unsigned_long(meta, "MODE", (unsigned long) statbuf.st_mode);
    kvtree_util_set_unsigned_long(meta, "UID",  (unsigned long) statbuf.st_uid);
    kvtree_util_set_unsigned_long(meta, "GID",  (unsigned long) statbuf.st_gid);
    kvtree_util_set_unsigned_long(meta, "SIZE", (unsigned long) statbuf.st_size);

    uint64_t secs, nsecs;
    shuffile_stat_get_atimes(&statbuf, &secs, &nsecs);
    kvtree_util_set_unsigned_long(meta, "ATIME_SECS",  (unsigned long) secs);
    kvtree_util_set_unsigned_long(meta, "ATIME_NSECS", (unsigned long) nsecs);

    shuffile_stat_get_ctimes(&statbuf, &secs, &nsecs);
    kvtree_util_set_unsigned_long(meta, "CTIME_SECS",  (unsigned long) secs);
    kvtree_util_set_unsigned_long(meta, "CTIME_NSECS", (unsigned long) nsecs);

    shuffile_stat_get_mtimes(&statbuf, &secs, &nsecs);
    kvtree_util_set_unsigned_long(meta, "MTIME_SECS",  (unsigned long) secs);
    kvtree_util_set_unsigned_long(meta, "MTIME_NSECS", (unsigned long) nsecs);

    return SHUFFILE_SUCCESS;
  }
  return SHUFFILE_FAILURE;
}

static int shuffile_meta_apply(const char* file, const kvtree* meta)
{
  int rc = SHUFFILE_SUCCESS;

  /* set permission bits on file */
  unsigned long mode_val;
  if (kvtree_util_get_unsigned_long(meta, "MODE", &mode_val) == KVTREE_SUCCESS) {
    mode_t mode = (mode_t) mode_val;

    /* TODO: mask some bits here */

    int chmod_rc = chmod(file, mode);
    if (chmod_rc != 0) {
      /* failed to set permissions */
      shuffile_err("chmod(%s) failed: errno=%d %s @ %s:%d",
        file, errno, strerror(errno), __FILE__, __LINE__
      );
      rc = SHUFFILE_FAILURE;
    }
  }

  /* set uid and gid on file */
  unsigned long uid_val = -1;
  unsigned long gid_val = -1;
  kvtree_util_get_unsigned_long(meta, "UID", &uid_val);
  kvtree_util_get_unsigned_long(meta, "GID", &gid_val);
  if (uid_val != -1 || gid_val != -1) {
    /* got a uid or gid value, try to set them */
    int chown_rc = chown(file, (uid_t) uid_val, (gid_t) gid_val);
    if (chown_rc != 0) {
      /* failed to set uid and gid */
      shuffile_err("chown(%s, %lu, %lu) failed: errno=%d %s @ %s:%d",
        file, uid_val, gid_val, errno, strerror(errno), __FILE__, __LINE__
      );
      rc = SHUFFILE_FAILURE;
    }
  }

  /* can't set the size at this point, but we can check it */
  unsigned long size;
  if (kvtree_util_get_unsigned_long(meta, "SIZE", &size) == KVTREE_SUCCESS) {
    /* got a size field in the metadata, stat the file */
    struct stat statbuf;
    int stat_rc = shuffile_stat(file, &statbuf);
    if (stat_rc == 0) {
      /* stat succeeded, check that sizes match */
      if (size != statbuf.st_size) {
        /* file size is not correct */
        shuffile_err("file `%s' size is %lu expected %lu @ %s:%d",
          file, (unsigned long) statbuf.st_size, size, __FILE__, __LINE__
        );
        rc = SHUFFILE_FAILURE;
      }
    } else {
      /* failed to stat file */
      shuffile_err("stat(%s) failed: errno=%d %s @ %s:%d",
        file, errno, strerror(errno), __FILE__, __LINE__
      );
      rc = SHUFFILE_FAILURE;
    }
  }

  /* set timestamps on file as last step */
  unsigned long atime_secs  = 0;
  unsigned long atime_nsecs = 0;
  kvtree_util_get_unsigned_long(meta, "ATIME_SECS",  &atime_secs);
  kvtree_util_get_unsigned_long(meta, "ATIME_NSECS", &atime_nsecs);

  unsigned long mtime_secs  = 0;
  unsigned long mtime_nsecs = 0;
  kvtree_util_get_unsigned_long(meta, "MTIME_SECS",  &mtime_secs);
  kvtree_util_get_unsigned_long(meta, "MTIME_NSECS", &mtime_nsecs);

  if (atime_secs != 0 || atime_nsecs != 0 ||
      mtime_secs != 0 || mtime_nsecs != 0)
  {
    /* fill in time structures */
    struct timespec times[2];
    times[0].tv_sec  = (time_t) atime_secs;
    times[0].tv_nsec = (long)   atime_nsecs;
    times[1].tv_sec  = (time_t) mtime_secs;
    times[1].tv_nsec = (long)   mtime_nsecs;

    /* set times with nanosecond precision using utimensat,
     * assume path is relative to current working directory,
     * if it's not absolute, and set times on link (not target file)
     * if dest_path refers to a link */
    int utime_rc = utimensat(AT_FDCWD, file, times, AT_SYMLINK_NOFOLLOW);
    if (utime_rc != 0) {
      shuffile_err("Failed to change timestamps on `%s' utimensat() errno=%d %s @ %s:%d",
        file, errno, strerror(errno), __FILE__, __LINE__
      );
      rc = SHUFFILE_FAILURE;
    }
  }

  return rc;
}

static int shuffile_have_files(const kvtree* hash, int rank)
{
  /* failed to find rank in hash */
  kvtree* rank_hash = kvtree_get_kv_int(hash, "RANK", rank);
  if (rank_hash == NULL) {
    return 0;
  }

  /* get number of files */
  int num_files;
  if (kvtree_util_get_int(rank_hash, "FILES", &num_files) != KVTREE_SUCCESS) {
    /* failed to read number of files */
    return 0;
  }

  /* check that we can read file */
  int found_files = 0;
  kvtree* files_hash = kvtree_get(rank_hash, "FILE");
  kvtree_elem* elem;
  for (elem = kvtree_elem_first(files_hash);
       elem != NULL;
       elem = kvtree_elem_next(elem))
  {
    /* get file name */
    const char* name = kvtree_elem_key(elem);

    /* check that file exists */
    if (shuffile_file_is_readable(name) == SHUFFILE_SUCCESS) {
      found_files++;
    }
  }

  if (found_files != num_files) {
    /* number of readable files not the same as expected number */
    return 0;
  }

  /* seem to have all of our files */
  return 1;
}

/* migrate files to owner process, if necessary */
int shuffile_migrate(
  MPI_Comm comm,
  MPI_Comm comm_storage,
  const char* name)
{
  if ((comm == MPI_COMM_NULL) || (comm_storage == MPI_COMM_NULL)) {
    shuffile_err("shuffile_migrate comm or comm_storage parameter is MPI_COMM_NULL @ %s:%d",
      __FILE__, __LINE__);
    return SHUFFILE_FAILURE; 
  }

  if (name == NULL) {
    shuffile_err("shuffile_migrate name parameter is null @ %s:%d",
      __FILE__, __LINE__);
    return SHUFFILE_FAILURE; 
  }

  int i, round;
  int rc = SHUFFILE_SUCCESS;

  /* get name of this process */
  int rank_world;
  MPI_Comm comm_world = comm;
  MPI_Comm_rank(comm_world, &rank_world);

  /* allocate an object to record new shuffle file info */
  kvtree* new_hash = kvtree_new();
  kvtree* new_rank_hash = kvtree_set_kv_int(new_hash, "RANK", rank_world);

  /* read data from shuffile file */
  kvtree* hash = kvtree_new();
  shuffile_read_file(comm, comm_storage, name, hash);

  /* TODO: support arbitrary process names */

  /* get pointer to ranks hash */
  kvtree* ranks_hash = kvtree_get(hash, "RANK");

  /* get list of ranks we have data for */
  int  nranks = 0;
  int* ranks = NULL;
  //kvtree_sort_int(ranks_hash, KVTREE_SORT_ASCENDING);
  kvtree_list_int(ranks_hash, &nranks, &ranks);

  /* walk backwards through the list of ranks, and set our start index
   * to the rank which is the first rank that is equal to or higher
   * than our own rank -- when we assign round ids below, this offsetting
   * helps distribute the load */
  int start_index = 0;
  for (i = nranks-1; i >= 0; i--) {
    /* pick the first rank whose rank id is equal to or higher than our own */
    int rank = ranks[i];
    if (rank >= rank_world) {
      start_index = i;
    }
  }

  /* TODO: check that a process accounts for every name */

  /* allocate array to record the rank we can send to in each round */
  int* have_rank_by_round = (int*) SHUFFILE_MALLOC(sizeof(int) * nranks);
  int* send_flag_by_round = (int*) SHUFFILE_MALLOC(sizeof(int) * nranks);

  /* check that we have all of the files for each rank,
   * and determine the round we can send them */
  kvtree* send_hash = kvtree_new();
  kvtree* recv_hash = kvtree_new();
  for (round = 0; round < nranks; round++) {
    /* get the rank id */
    int index = (start_index + round) % nranks;
    int rank = ranks[index];

    /* record the rank indexed by the round number */
    have_rank_by_round[round] = rank;

    /* assume we won't be sending to this rank in this round */
    send_flag_by_round[round] = 0;

    /* if we have files for this rank, specify the round we can
     * send those files in */
    if (shuffile_have_files(hash, rank)) {
      kvtree_setf(send_hash, NULL, "%d %d", rank, round);
    }
  }
  kvtree_exchange(send_hash, recv_hash, comm_world);

  /* search for the minimum round we can get our files */
  int retrieve_rank  = -1;
  int retrieve_round = -1;
  kvtree_elem* elem = NULL;
  for (elem = kvtree_elem_first(recv_hash);
       elem != NULL;
       elem = kvtree_elem_next(elem))
  {
    /* get the rank id */
    int rank = kvtree_elem_key_int(elem);

    /* get the round id */
    kvtree* round_hash = kvtree_elem_hash(elem);
    kvtree_elem* round_elem = kvtree_elem_first(round_hash);
    char* round_str = kvtree_elem_key(round_elem);
    int round = atoi(round_str);

    /* record this round and rank number if it's less than the current round */
    if (round < retrieve_round || retrieve_round == -1) {
      retrieve_round = round;
      retrieve_rank  = rank;
    }
  }

  /* done with the round hashes, free them off */
  kvtree_delete(&recv_hash);
  kvtree_delete(&send_hash);

  /* free off our list of ranks */
  shuffile_free(&ranks);

  /* get the maximum retrieve round */
  int max_rounds = 0;
  MPI_Allreduce(&retrieve_round, &max_rounds, 1, MPI_INT, MPI_MAX, comm_world);

  /* tell destination which round we'll take our files in */
  send_hash = kvtree_new();
  recv_hash = kvtree_new();
  if (retrieve_rank != -1) {
    kvtree_setf(send_hash, NULL, "%d %d", retrieve_rank, retrieve_round);
  }
  kvtree_exchange(send_hash, recv_hash, comm_world);

  /* determine which ranks want to fetch their files from us */
  for(elem = kvtree_elem_first(recv_hash);
      elem != NULL;
      elem = kvtree_elem_next(elem))
  {
    /* get the round id */
    kvtree* round_hash = kvtree_elem_hash(elem);
    kvtree_elem* round_elem = kvtree_elem_first(round_hash);
    char* round_str = kvtree_elem_key(round_elem);
    int round = atoi(round_str);

    /* record whether this rank wants its files from us */
    if (round >= 0 && round < nranks) {
      send_flag_by_round[round] = 1;
    }
  }

  /* done with the round hashes, free them off */
  kvtree_delete(&recv_hash);
  kvtree_delete(&send_hash);

  /* run through rounds and exchange files */
  for (round = 0; round <= max_rounds; round++) {
    /* assume we don't need to send or receive any files this round */
    int send_rank = MPI_PROC_NULL;
    int recv_rank = MPI_PROC_NULL;
    int send_num  = 0;
    int recv_num  = 0;

    /* check whether I can potentially send to anyone in this round */
    if (round < nranks) {
      /* have someone's files, check whether they are asking
       * for them this round */
      if (send_flag_by_round[round]) {
        /* need to send files this round, remember to whom and how many */
        int dst_rank = have_rank_by_round[round];
        send_rank = dst_rank;
        kvtree* rank_hash = kvtree_get_kv_int(hash, "RANK", dst_rank);
        kvtree_util_get_int(rank_hash, "FILES", &send_num);

        /* don't bother transferring files to ourself */
        if (send_rank == rank_world) {
          kvtree_merge(new_rank_hash, rank_hash);
          continue;
        }
      }
    }

    /* if I'm supposed to get my files this round, set the recv_rank */
    if (retrieve_round == round) {
      recv_rank = retrieve_rank;
    }

    /* if we have files for this round, but the correspdonding
     * rank doesn't need them, delete the files */
    if (round < nranks && send_rank == MPI_PROC_NULL) {
      int dst_rank = have_rank_by_round[round];
      shuffile_unlink(hash, dst_rank);
    }

    /* sending to and/or recieving from another node */
    if (send_rank != MPI_PROC_NULL || recv_rank != MPI_PROC_NULL) {
      /* have someone to send to or receive from */
      int have_outgoing = 0;
      int have_incoming = 0;
      if (send_rank != MPI_PROC_NULL) {
        have_outgoing = 1;
      }
      if (recv_rank != MPI_PROC_NULL) {
        have_incoming = 1;
      }

      /* first, determine how many files I will be receiving and
       * tell how many I will be sending */
      MPI_Request request[2];
      MPI_Status  status[2];
      int num_req = 0;
      if (have_incoming) {
        MPI_Irecv(
          &recv_num, 1, MPI_INT, recv_rank, 0,
          comm_world, &request[num_req]
        );
        num_req++;
      }
      if (have_outgoing) {
        MPI_Isend(
          &send_num, 1, MPI_INT, send_rank, 0,
          comm_world, &request[num_req]
        );
        num_req++;
      }
      if (num_req > 0) {
        MPI_Waitall(num_req, request, status);
      }

      /* record how many files I will receive (need to distinguish
       * between 0 files and not knowing) */
      if (have_incoming) {
        kvtree_util_set_int(new_rank_hash, "FILES", recv_num);
      }

      /* turn off send or receive flags if the file count is 0,
       * nothing else to do */
      if (send_num == 0) {
        have_outgoing = 0;
        send_rank = MPI_PROC_NULL;
      }
      if (recv_num == 0) {
        have_incoming = 0;
        recv_rank = MPI_PROC_NULL;
      }

      /* TODO: since we overwrite files in place in order to avoid
       * running out of storage space, we should sort files in order
       * of descending size for the next step */

      /* get our file list for the destination */
      int numfiles = 0;
      const char** files = NULL;
      if (have_outgoing) {
        files = (const char**) SHUFFILE_MALLOC(send_num * sizeof(char*));
        kvtree_elem* elem;
        kvtree* rank_hash = kvtree_get_kv_int(hash, "RANK", send_rank);
        kvtree* file_hash = kvtree_get(rank_hash, "FILE");
        for (elem = kvtree_elem_first(file_hash);
             elem != NULL;
             elem = kvtree_elem_next(elem))
        {
          const char* filename = kvtree_elem_key(elem);
          files[numfiles] = filename;
          numfiles++;
        }
      }

      /* while we have a file to send or receive ... */
      while (have_incoming || have_outgoing) {
        /* create empty kvtrees used to exchange file metadata */
        kvtree* send_meta = kvtree_new();
        kvtree* recv_meta = kvtree_new();

        /* get the filename */
        const char* file = NULL;
        if (have_outgoing) {
          file = files[numfiles - send_num];

          /* snapshot metadata for this file */
          shuffile_meta_encode(file, send_meta);
        }

        /* exhange file names with partners,
         * building full path of incoming file */
        char file_partner[SHUFFILE_MAX_FILENAME];
        shuffile_swap_file_names(
          file, send_rank, file_partner, sizeof(file_partner), recv_rank,
          ".", comm_world
        );

        /* if we'll receive a file, record the name of our file
         * in the filemap and write it to disk */
        if (recv_rank != MPI_PROC_NULL) {
          kvtree_set_kv(new_rank_hash, "FILE", file_partner);
          //scr_filemap_write(scr_map_file, map);
        }

        /* either sending or receiving a file this round, since we move files,
         * it will be deleted or overwritten */
        if (shuffile_swap_files(MOVE_FILES, file, send_meta, send_rank,
            file_partner, recv_meta, recv_rank, comm_world) != SHUFFILE_SUCCESS)
        {
          shuffile_err("Swapping files: %s to %d, %s from %d @ %s:%d",
            file, send_rank, file_partner, recv_rank, __FILE__, __LINE__
          );
          rc = SHUFFILE_FAILURE;
        }

        /* if we received a file, record its meta data and decrement
         * our receive count */
        if (have_incoming) {
          /* apply metadata to received file */
          shuffile_meta_apply(file_partner, recv_meta);

          /* decrement receive count */
          recv_num--;
          if (recv_num == 0) {
            have_incoming = 0;
            recv_rank = MPI_PROC_NULL;
          }
        }

        /* if we sent a file, remove it from the filemap and decrement
         * our send count */
        if (have_outgoing) {
          /* decrement our send count */
          send_num--;
          if (send_num == 0) {
            have_outgoing = 0;
            send_rank = MPI_PROC_NULL;
          }
        }

        /* release metadata */
        kvtree_delete(&recv_meta);
        kvtree_delete(&send_meta);
      }

      /* free our file list */
      shuffile_free(&files);
    }
  }

  /* if we have more rounds than max rounds, delete the remainder of our files */
  for (round = max_rounds+1; round < nranks; round++) {
    /* have someone's files for this round, so delete them */
    int dst_rank = have_rank_by_round[round];
    shuffile_unlink(hash, dst_rank);
  }

  shuffile_free(&send_flag_by_round);
  shuffile_free(&have_rank_by_round);

  /* write out new filemap and free the memory resources */
  rc = shuffile_write_file(comm_storage, name, new_hash);

  /* free our hash objects */
  kvtree_delete(&hash);
  kvtree_delete(&new_hash);

  /* return whether distribute succeeded, it does not ensure we have
   * all of our files, only that the transfer completed without failure */
  return rc;
}
