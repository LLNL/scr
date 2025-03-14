#include <stdio.h>
#include <string.h>

#include "mpi.h"

#include "kvtree.h"
#include "kvtree_util.h"

#include "redset_util.h"
#include "redset.h"
#include "redset_io.h"
#include "redset_internal.h"

/* Produces a metadata file to track info about files.
 *
 * CURRENT
 *   FILES
 *     <numfiles>
 *   FILE
 *     0
 *       <filepath>
 *         SIZE
 *           <file_size>
 *     1
 *       <filepath>
 *         SIZE
 *           <file_size>
 *
 * On rebuild, this metadata file is read, and its values
 * are checked against the specified files.
 */

#define REDSET_KEY_COPY_SINGLE_DESC "DESC"
#define REDSET_KEY_COPY_SINGLE_GROUP_RANK  "RANK"

/* set single filename */
static void redset_build_single_filename(
  const char* name,
  const redset_base* d,
  char* file,
  size_t len)
{
  int rank_world;
  MPI_Comm_rank(d->parent_comm, &rank_world);
  snprintf(file, len, "%s%d.single.grp_%d_of_%d.mem_%d_of_%d.redset",
    name, rank_world, d->group_id+1, d->groups, d->rank+1, d->ranks
  );
}

/* returns 1 if we successfully read redundancy file, 0 otherwise */
static int redset_read_single_file(
  const char* name,
  const redset_base* d,
  kvtree* header)
{
  /* get single filename */
  char file[REDSET_MAX_FILENAME];
  redset_build_single_filename(name, d, file, sizeof(file));

  /* check that we can read the file */
  if (redset_file_is_readable(file) != REDSET_SUCCESS) {
    redset_dbg(2, "Do not have read access to file: %s @ %s:%d",
      file, __FILE__, __LINE__
    );
    return REDSET_FAILURE;
  }

  /* read header from file */
  if (kvtree_read_file(file, header) != KVTREE_SUCCESS) {
    return REDSET_FAILURE;
  }

  return REDSET_SUCCESS;
}

int redset_encode_reddesc_single(
  kvtree* hash,
  const char* name,
  const redset_base* d)
{
  /* nothing more to add for single */
  return REDSET_SUCCESS;
}

int redset_apply_single(
  int numfiles,
  const char** files,
  const char* name,
  const redset_base* d)
{
  /* allocate a structure to record meta data about our files and redundancy descriptor */
  kvtree* current_hash = kvtree_new();

  /* encode file info into hash */
  redset_lofi_encode_kvtree(current_hash, numfiles, files);

  /* store our redundancy descriptor in hash */
  kvtree* desc_hash = kvtree_new();
  redset_store_to_kvtree(d, desc_hash);
  kvtree_set(current_hash, REDSET_KEY_COPY_SINGLE_DESC, desc_hash);

  /* define header we'll write to our redundancy file */
  kvtree* header = kvtree_new();

  /* record our rank within our redundancy group */
  kvtree_set_kv_int(header, REDSET_KEY_COPY_SINGLE_GROUP_RANK, d->rank);

  /* record our redundancy descriptor and file info */
  kvtree_setf(header, current_hash, "%s %d", REDSET_KEY_COPY_SINGLE_DESC, d->rank);

  /* sort the header to list items alphabetically,
   * this isn't strictly required, but it ensures the kvtrees
   * are stored in the same byte order so that we can reproduce
   * the redundancy file identically on a rebuild */
  redset_sort_kvtree(header);

  /* define file name for our redudancy file */
  char filename[REDSET_MAX_FILENAME];
  redset_build_single_filename(name, d, filename, sizeof(filename));

  /* write the redundancy file */
  kvtree_write_file(filename, header);
  kvtree_delete(&header);

  return REDSET_SUCCESS;
}

int redset_recover_single(
  const char* name,
  const redset_base* d)
{
  int rc = REDSET_SUCCESS;

  MPI_Comm comm_world = d->parent_comm;

  /* assume files exist for this process */
  int have_my_files = 1;

  /* check whether we have our files */
  kvtree* header = kvtree_new();
  if (redset_read_single_file(name, d, header) == REDSET_SUCCESS) {
    /* get pointer to hash for this rank */
    kvtree* current_hash = kvtree_getf(header, "%s %d", REDSET_KEY_COPY_SINGLE_DESC, d->rank);
    if (redset_lofi_check(current_hash) != REDSET_SUCCESS) {
      /* some data file is bad */
      have_my_files = 0;
    }
  } else {
    /* failed to read redundancy file */
    have_my_files = 0;
  }
  kvtree_delete(&header);

  /* determine whether all ranks have their files */
  if (! redset_alltrue(have_my_files, comm_world)) {
    rc = REDSET_FAILURE;
  }

  return rc;
}

int redset_unapply_single(
  const char* name,
  const redset_base* d)
{
  char filename[REDSET_MAX_FILENAME];
  redset_build_single_filename(name, d, filename, sizeof(filename));
  int rc = redset_file_unlink(filename);
  return rc;
}

/* returns a list of files added by redundancy descriptor */
redset_list* redset_filelist_enc_get_single(
  const char* name,
  redset_base* d)
{
  char file[REDSET_MAX_FILENAME];
  redset_build_single_filename(name, d, file, sizeof(file));
  redset_list* list = (redset_list*) REDSET_MALLOC(sizeof(redset_list));
  list->count = 1;
  list->files = (const char**) REDSET_MALLOC(sizeof(char*));
  list->files[0] = strdup(file);
  return list;
}

/* returns a list of original files encoded by redundancy descriptor */
redset_list* redset_filelist_orig_get_single(
  const char* name,
  const redset_base* d)
{
  redset_list* list = NULL;

  /* check whether we have our files and our partner's files */
  kvtree* header = kvtree_new();
  if (redset_read_single_file(name, d, header) == REDSET_SUCCESS) {
    /* get pointer to hash for this rank */
    kvtree* current_hash = kvtree_getf(header, "%s %d", REDSET_KEY_COPY_SINGLE_DESC, d->rank);
    list = redset_lofi_filelist(current_hash);
  }
  kvtree_delete(&header);

  return list;
}
