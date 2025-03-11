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

/* Utility to rebuild a missing file given the file names of the
 * remaining N-1 data files and the N-1 XOR segments. */

#include "kvtree.h"
#include "kvtree_util.h"

#include "redset.h"
#include "redset_internal.h"
#include "redset_util.h"

#include <stdlib.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>

#define REDSET_KEY_COPY_PARTNER_DESC    "DESC"
#define REDSET_KEY_COPY_PARTNER_RANKS   "RANKS"
#define REDSET_KEY_COPY_PARTNER_RANK    "RANK"
#define REDSET_KEY_COPY_PARTNER_FILES   "FILES"
#define REDSET_KEY_COPY_PARTNER_FILE    "FILE"
#define REDSET_KEY_COPY_PARTNER_SIZE    "SIZE"
#define REDSET_KEY_COPY_PARTNER_REPLICAS "REPLICAS"

#ifdef REDSET_GLOBALS_H
#error "globals.h accessed from tools"
#endif

static int buffer_size = 128*1024;

/* given a header, get group rank of process that owns this header */
static int lookup_group_rank(const kvtree* header, const char* file)
{
  int rank = -1;
  if (kvtree_util_get_int(header, REDSET_KEY_COPY_PARTNER_RANK, &rank) != KVTREE_SUCCESS) {
    redset_err("Failed to read group rank from PARTNER file header in %s @ %s:%d",
      file, __FILE__, __LINE__
    );
  }
  return rank;
}

/* given a header, lookup and return the size of the redundancy group */
static int lookup_group_size(const kvtree* header, const char* file)
{
  int group_rank = lookup_group_rank(header, file);

  int ranks = 0;
  kvtree* rank_hash = kvtree_get_kv_int(header, REDSET_KEY_COPY_PARTNER_DESC, group_rank);
  kvtree* desc_hash = kvtree_get(rank_hash, REDSET_KEY_COPY_PARTNER_DESC);
  if (kvtree_util_get_int(desc_hash, REDSET_KEY_CONFIG_GROUP_SIZE, &ranks) != KVTREE_SUCCESS) {
    redset_err("Failed to read group size from PARTNER file header in %s @ %s:%d",
      file, __FILE__, __LINE__
    );
  }
  return ranks;
}

/* given a header, lookup and return the number of replicas */
static int lookup_replica_count(const kvtree* header, const char* file)
{
  int group_rank = lookup_group_rank(header, file);

  int replicas = 0;
  kvtree* rank_hash = kvtree_get_kv_int(header, REDSET_KEY_COPY_PARTNER_DESC, group_rank);
  kvtree* desc_hash  = kvtree_get(rank_hash, REDSET_KEY_COPY_PARTNER_DESC);
  if (kvtree_util_get_int(desc_hash, REDSET_KEY_COPY_PARTNER_REPLICAS, &replicas) != KVTREE_SUCCESS) {
    redset_err("Failed to read replica count from PARTNER file header in %s @ %s:%d",
      file, __FILE__, __LINE__
    );
  }
  return replicas;
}

/* given a header, lookup and return global rank of a process given rank in its group */
static int lookup_world_rank(const kvtree* hash, int group_rank)
{
  int rank = -1;
  kvtree* desc_hash = kvtree_get(hash, REDSET_KEY_COPY_PARTNER_DESC);
  if (kvtree_util_get_int(desc_hash, REDSET_KEY_CONFIG_WORLD_RANK, &rank) != KVTREE_SUCCESS) {
    redset_err("Failed to read world rank from PARTNER file header in @ %s:%d",
      __FILE__, __LINE__
    );
  }
  return rank;
}

/* given a descriptor, lookup and return group info */
static int lookup_group_info(
  const kvtree* hash,
  int* group_num,
  int* group_id,
  int* group_size,
  int* group_rank,
  int* world_size,
  int* world_rank)
{
  kvtree* desc_hash = kvtree_get(hash, REDSET_KEY_COPY_PARTNER_DESC);

  if (kvtree_util_get_int(desc_hash, REDSET_KEY_CONFIG_GROUPS, group_num) != KVTREE_SUCCESS) {
    redset_err("Failed to read number of groups from descriptor @ %s:%d",
      __FILE__, __LINE__
    );
  }

  if (kvtree_util_get_int(desc_hash, REDSET_KEY_CONFIG_GROUP, group_id) != KVTREE_SUCCESS) {
    redset_err("Failed to read group id from descriptor @ %s:%d",
      __FILE__, __LINE__
    );
  }

  if (kvtree_util_get_int(desc_hash, REDSET_KEY_CONFIG_GROUP_SIZE, group_size) != KVTREE_SUCCESS) {
    redset_err("Failed to read group size from descriptor @ %s:%d",
      __FILE__, __LINE__
    );
  }

  if (kvtree_util_get_int(desc_hash, REDSET_KEY_CONFIG_GROUP_RANK, group_rank) != KVTREE_SUCCESS) {
    redset_err("Failed to read group rank from descriptor @ %s:%d",
      __FILE__, __LINE__
    );
  }

  if (kvtree_util_get_int(desc_hash, REDSET_KEY_CONFIG_WORLD_SIZE, world_size) != KVTREE_SUCCESS) {
    redset_err("Failed to read world size from descriptor @ %s:%d",
      __FILE__, __LINE__
    );
  }

  if (kvtree_util_get_int(desc_hash, REDSET_KEY_CONFIG_WORLD_RANK, world_rank) != KVTREE_SUCCESS) {
    redset_err("Failed to read world rank from descriptor @ %s:%d",
      __FILE__, __LINE__
    );
  }

  return 0;
}

static int redset_recover_partner_rebuild_serial_redundancy(
  int missing_rank,
  const char* filename,
  int fd,
  off_t header_size,
  int replicas,
  int total_ranks,
  redset_lofi* rsfs)
{
  int rc = REDSET_SUCCESS;

  /* allocate buffer to read data */
  char* buffer = REDSET_MALLOC(buffer_size * sizeof(char));
  if (buffer == NULL) {
    redset_err("Failed to allocate buffer memory @ %s:%d",
      __FILE__, __LINE__
    );
    return REDSET_FAILURE;
  }

  /* compute offset into partner file where data for missing rank starts,
   * skip the header to begin with */
  unsigned long offset = header_size;

  /* seek to position in partner file where we should start to write data */
  if (redset_lseek(filename, fd, offset, SEEK_SET) != REDSET_SUCCESS) {
    rc = REDSET_FAILURE;
  }

  /* iterate over each partner rank for the given number of replicas */
  int dist;
  for (dist = 1; dist <= replicas; dist++) {
    /* get partner rank within the group */
    int lhs_rank = (missing_rank - dist + total_ranks) % total_ranks;

    /* copy data from this partner into our partner file */
    unsigned long pos = 0;
    unsigned long bytes = redset_lofi_bytes(&rsfs[lhs_rank]);
    while (pos < bytes && rc == REDSET_SUCCESS) {
      /* read upto buffer_size bytes at a time */
      size_t count = bytes - pos;
      if (count > buffer_size) {
        count = buffer_size;
      }
  
      /* at this point, we have the data from the missing rank, write it out */
      /* write chunk to logical file for the missing rank */
      if (redset_lofi_pread(&rsfs[lhs_rank], buffer, count, pos) != REDSET_SUCCESS)
      {
        /* our write failed, set the return code to an error */
        rc = REDSET_FAILURE;
      }

      /* read a segment from each rank and XOR it into our buffer */
      /* read chunk from the XOR file for this rank */
      if (redset_write_attempt(filename, fd, buffer, count) != count) {
        /* our read failed, set the return code to an error */
        rc = REDSET_FAILURE;
      }
  
      pos += count;
    }
  }

  redset_free(&buffer);

  return rc;
}

static int redset_recover_partner_rebuild_serial(
  int missing_rank,
  int partner_rank,
  const char* filename,
  int fd,
  off_t header_size,
  int total_ranks,
  redset_lofi* rsfs)
{
  int rc = REDSET_SUCCESS;

  /* allocate buffer to read data */
  char* buffer = REDSET_MALLOC(buffer_size * sizeof(char));
  if (buffer == NULL) {
    redset_err("Failed to allocate buffer memory @ %s:%d",
      __FILE__, __LINE__
    );
    return REDSET_FAILURE;
  }

  /* compute offset into partner file where data for missing rank starts,
   * skip the header to begin with */
  unsigned long offset = header_size;

  /* iterate over each rank we're saving until we reach the target */
  int dist;
  for (dist = 1; dist < total_ranks; dist++) {
    /* compute group rank of this process */
    int lhs_rank = (partner_rank - dist + total_ranks) % total_ranks;

    /* check whether the current rank is the target */
    if (lhs_rank == missing_rank) {
      /* reached the target, we can break */
      break;
    }

    /* this data is for a rank other than the one we're rebuilding,
     * skip over it */
    unsigned long bytes = redset_lofi_bytes(&rsfs[lhs_rank]);
    offset += bytes;
  }

  /* seek to position in partner file where data for missing rank starts */
  if (redset_lseek(filename, fd, offset, SEEK_SET) != REDSET_SUCCESS) {
    rc = REDSET_FAILURE;
  }

  unsigned long write_pos = 0;
  unsigned long bytes = redset_lofi_bytes(&rsfs[missing_rank]);
  while (write_pos < bytes && rc == REDSET_SUCCESS) {
    /* read upto buffer_size bytes at a time */
    size_t count = bytes - write_pos;
    if (count > buffer_size) {
      count = buffer_size;
    }

    /* read a segment from each rank and XOR it into our buffer */
    /* read chunk from the XOR file for this rank */
    if (redset_read_attempt(filename, fd, buffer, count) != count) {
      /* our read failed, set the return code to an error */
      rc = REDSET_FAILURE;
      count = 0;
    }

    /* at this point, we have the data from the missing rank, write it out */
    /* write chunk to logical file for the missing rank */
    if (redset_lofi_pwrite(&rsfs[missing_rank], buffer, count, write_pos) != REDSET_SUCCESS)
    {
      /* our write failed, set the return code to an error */
      rc = REDSET_FAILURE;
    }
    write_pos += count;
  }

  redset_free(&buffer);

  return rc;
}

int redset_rebuild_partner(
  int num,
  const char** files,
  const char* prefix,
  const kvtree* map)
{
  int rc = REDSET_SUCCESS;

  int total_ranks = 0;
  int replicas    = 0;
  int* missing    = NULL;
  kvtree** hashes = NULL;

  int i;
  for (i = 0; i < num; i++) {
    /* open the current file */
    const char* file = files[i];
    int fd = redset_open(file, O_RDONLY);
    if (fd < 0) {
      redset_warn("Opening PARTNER file for reading: redset_open(%s) errno=%d %s @ %s:%d",
        file, errno, strerror(errno), __FILE__, __LINE__
      );
      continue;
    }

    /* read header from the file */
    kvtree* header = kvtree_new();
    if (kvtree_read_fd(file, fd, header) < 0) {
      /* failed to read header from this file */
      redset_warn("Failed to read header from PARTNER file `%s' @ %s:%d",
        file, __FILE__, __LINE__
      );
      kvtree_delete(&header);
      redset_close(file, fd);
      continue;
    }

    /* if this is our first file, get number of ranks in the redudancy group */
    if (hashes == NULL) {
      /* read number of items in the redudancy group */
      total_ranks = lookup_group_size(header, file);

      /* read number of replicas */
      replicas = lookup_replica_count(header, file);

      /* we'll track which ranks we actually have redundancy files for */
      missing = (int*) REDSET_MALLOC(total_ranks * sizeof(int));

      /* allocate a spot to hold the file info for each member */
      hashes = (kvtree**) REDSET_MALLOC(total_ranks * sizeof(kvtree*));

      /* initialize all spots to NULL so we know whether we've already read it in */
      int j;
      for (j = 0; j < total_ranks; j++) {
        missing[j] = 1;
        hashes[j]  = NULL;
      }
    }

    /* assume if we have the redundancy file that it's not missing */
    int group_rank = lookup_group_rank(header, file);
    missing[group_rank] = 0;

    /* get file info for each rank we can pull from this header */
    kvtree* desc_hash = kvtree_get(header, REDSET_KEY_COPY_PARTNER_DESC);
    kvtree_elem* rank_elem;
    for (rank_elem = kvtree_elem_first(desc_hash);
         rank_elem != NULL;
         rank_elem = kvtree_elem_next(rank_elem))
    {
      /* get the rank of the file info */
      int rank = kvtree_elem_key_int(rank_elem);

      /* copy to our array if it's not already set */
      if (hashes[rank] == NULL) {
        /* not set, get pointer to file info */
        kvtree* rank_hash = kvtree_elem_hash(rank_elem);

        /* allocate an empty kvtree and copy the file info */
        hashes[rank] = kvtree_new();
        kvtree_merge(hashes[rank], rank_hash);
      }
    }

    kvtree_delete(&header);
    redset_close(file, fd);
  }

  /* check that we opened at least one file to get a rank count */
  if (total_ranks == 0) {
    /* failed to read rank count from any file */
    redset_err("Failed to get group size from redudancy files @ %s:%d",
      __FILE__, __LINE__
    );
    for (i = 0; i < total_ranks; i++) {
      kvtree_delete(&hashes[i]);
    }
    redset_free(&hashes);
    redset_free(&missing);
    return REDSET_FAILURE;
  }

  /* check that we have a hash for every member in the group */
  int invalid = 0;
  for (i = 0; i < total_ranks; i++) {
    if (hashes[i] == NULL) {
      /* missing file info for some member */
      invalid = 1;
    }
  }
  if (invalid) {
    redset_err("Insufficient data to rebuild group @ %s:%d",
      __FILE__, __LINE__
    );
    for (i = 0; i < total_ranks; i++) {
      kvtree_delete(&hashes[i]);
    }
    redset_free(&hashes);
    redset_free(&missing);
    return REDSET_FAILURE;
  }

  /* check that we can identify set of files for all procs in the set */
  for (i = 0; i < total_ranks; i++) {
    const kvtree* current_hash = hashes[i];
    if (redset_lofi_check_mapped(current_hash, map) != REDSET_SUCCESS) {
      missing[i] = 1;
    }
  }

  /* count number of ranks we're missing */
  int missing_count = 0;
  for (i = 0; i < total_ranks; i++) {
    if (missing[i]) {
      missing_count++;
    }
  }

  /* if nothing is missing, nothing else to do, we can exit early with success */
  if (missing_count == 0) {
    for (i = 0; i < total_ranks; i++) {
      kvtree_delete(&hashes[i]);
    }
    redset_free(&hashes);
    redset_free(&missing);
    return REDSET_SUCCESS;
  }

  /* if there is a consecutive string of missing ranks that is
   * larger than the replica count, then we can't rebuild */
  int can_rebuild = 1;
  int missing_consecutive = 0;
  for (i = 0; i < total_ranks + replicas; i++) {
    /* we need to wrap from the end back to the beginning for up to replicas ranks,
     * to check that ranks at the end also have a copy */
    int rank = i % total_ranks;
    if (missing[rank]) {
      /* we are missing files for this rank, increment the coutner */
      missing_consecutive++;
      if (missing_consecutive > replicas) {
        /* found a string of consecutive missing ranks
         * which exceeds the replica count */
        can_rebuild = 0;
      }
    } else {
      /* we have files for this rank, so reset out counter */
      missing_consecutive = 0;
    }
  }
  if (! can_rebuild) {
    redset_err("Insufficient data to rebuild group @ %s:%d",
      __FILE__, __LINE__
    );
    for (i = 0; i < total_ranks; i++) {
      kvtree_delete(&hashes[i]);
    }
    redset_free(&hashes);
    redset_free(&missing);
    return REDSET_FAILURE;
  }

  /* allocate a logical file for each member */
  redset_lofi* rsfs = (redset_lofi*) REDSET_MALLOC(total_ranks * sizeof(redset_lofi));

  /* allocate an array to hold strdup of name of each redundancy file */
  char** filenames = (char**) REDSET_MALLOC(total_ranks * sizeof(char*));

  /* allocate a file descriptor for each redundancy file for each member */
  int* fds = (int*) REDSET_MALLOC(total_ranks * sizeof(int));

  /* allocate a file descriptor for each redundancy file for each member */
  off_t* header_sizes = (off_t*) REDSET_MALLOC(total_ranks * sizeof(off_t));

  /* initialize all spots to NULL so we know whether we've already read it in */
  for (i = 0; i < total_ranks; i++) {
    filenames[i]    = NULL;
    fds[i]          = -1;
    header_sizes[i] = 0;
  }

  /* open data and partner files */
  for (i = 0; i < total_ranks; i++) {
    /* get file info for the current process */
    kvtree* current_hash = hashes[i];

    /* lookup group membership info for this process */
    int group_num, group_id, group_size, group_rank, world_size, world_rank;
    lookup_group_info(
      current_hash, &group_num, &group_id, &group_size, &group_rank, &world_size, &world_rank
    );

    /* define name for partner file */
    char partner_name[1024];
    snprintf(partner_name, sizeof(partner_name), "%s%d.partner.grp_%d_of_%d.mem_%d_of_%d.redset",
      prefix, world_rank, group_id+1, group_num, group_rank+1, group_size
    );
    filenames[i] = strdup(partner_name);

    /* if this process is missing, open its files for writing,
     * otherwise open them for reading */
    if (missing[i]) {
      /* open our data files for writing */
      mode_t mode_file = redset_getmode(1, 1, 0);
      if (redset_lofi_open_mapped(current_hash, map, O_RDWR | O_CREAT | O_TRUNC, mode_file, &rsfs[i]) != REDSET_SUCCESS) {
        redset_err("Opening user data files for writing @ %s:%d",
          __FILE__, __LINE__
        );
        /* TODO: would be nice to clean up memory */
        return REDSET_FAILURE;
      }

      /* open partner file for writing */
      fds[i] = redset_open(filenames[i], O_WRONLY | O_CREAT | O_TRUNC, mode_file);
      if (fds[i] < 0) {
        redset_err("Opening partner file to be reconstructed: redset_open(%s) errno=%d %s @ %s:%d",
          filenames[i], errno, strerror(errno), __FILE__, __LINE__
        );
        /* TODO: would be nice to clean up memory */
        return REDSET_FAILURE;
      }
    } else {
      /* we have these user data files, open them for reading */
      if (redset_lofi_open_mapped(current_hash, map, O_RDONLY, (mode_t)0, &rsfs[i]) != REDSET_SUCCESS) {
        redset_err("Opening user data files for reading @ %s:%d",
          __FILE__, __LINE__
        );
        /* TODO: would be nice to clean up memory */
        return REDSET_FAILURE;
      }

      /* open partner file for reading */
      fds[i] = redset_open(filenames[i], O_RDONLY);
      if (fds[i] < 0) {
        redset_err("Opening partner file to be read: redset_open(%s) errno=%d %s @ %s:%d",
          filenames[i], errno, strerror(errno), __FILE__, __LINE__
        );
        /* TODO: would be nice to clean up memory */
        return REDSET_FAILURE;
      }

      /* read header, throw it away, and make note of offset */
      kvtree* header = kvtree_new();
      kvtree_read_fd(filenames[i], fds[i], header);
      kvtree_delete(&header);
      header_sizes[i] = lseek(fds[i], 0, SEEK_CUR);
    }
  }

  /* rebuild data files for all missing ranks */
  for (i = 0; i < total_ranks; i++) {
    if (missing[i]) {
      /* this rank is missing, build its header */
      kvtree* header = kvtree_new();

      /* copy file info for each process to our left */
      int dist;
      for (dist = 0; dist <= replicas; dist++) {
        int rank = (i - dist + total_ranks) % total_ranks;
        kvtree* desc_hash = kvtree_new();
        kvtree_merge(desc_hash, hashes[rank]);
        kvtree_setf(header, desc_hash, "%s %d", REDSET_KEY_COPY_PARTNER_DESC, rank);
      }

      /* set our rank within the group */
      kvtree_util_set_int(header, REDSET_KEY_COPY_PARTNER_RANK, i);

      /* sort header to list keys in alphabetical order */
      redset_sort_kvtree(header);

      /* write the header to the partner redundancy file of the missing rank */
      if (kvtree_write_fd(filenames[i], fds[i], header) < 0) {
        rc = REDSET_FAILURE;
      }
      kvtree_delete(&header);

      /* make note of header size */
      header_sizes[i] = lseek(fds[i], 0, SEEK_CUR);

      /* recover our data files from first process to our right that is not missing */
      for (dist = 1; dist <= replicas; dist++) {
        /* get rank of process to our right */
        int rhs_rank = (i + dist + total_ranks) % total_ranks;

        /* see if this process has its files */
        if (! missing[rhs_rank]) {
          /* found a process that has our files, recover them */
          int rebuild_rc = redset_recover_partner_rebuild_serial(
            i, rhs_rank, filenames[rhs_rank], fds[rhs_rank], header_sizes[rhs_rank], total_ranks, rsfs
          );
          if (rebuild_rc != REDSET_SUCCESS) {
            rc = rebuild_rc;
          }

          /* we rebuild files for this process, no need to keep looking */
          break;
        }
      }
    }
  }

  /* we have now recovered data files for all procs,
   * rebuild partner files for all missing ranks */
  for (i = 0; i < total_ranks; i++) {
    if (missing[i]) {
      /* we need to copy partner data to our redudancy files */
      int rebuild_rc = redset_recover_partner_rebuild_serial_redundancy(
        i, filenames[i], fds[i], header_sizes[i], replicas, total_ranks, rsfs
      );
      if (rebuild_rc != REDSET_SUCCESS) {
        rc = rebuild_rc;
      }
    }
  }

  /* close data files */
  for (i = 0; i < total_ranks; i++) {
    redset_lofi_close(&rsfs[i]);
  }

  /* close redundancy files */
  for (i = 0; i < total_ranks; i++) {
    redset_close(filenames[i], fds[i]);
  }

  /* copy meta data properties to new file (uid, gid, mode, atime, mtime),
   * and reset atime on existing files */
  if (rc == REDSET_SUCCESS) {
    for (i = 0; i < total_ranks; i++) {
      int apply_rc = redset_lofi_apply_meta_mapped(hashes[i], map);
      if (apply_rc != REDSET_SUCCESS) {
      }
    }
  }

  /* if the write failed, delete the files we just wrote */
  if (rc != REDSET_SUCCESS) {
    // TODO: unlink new files we just created
  }

  for (i = 0; i < total_ranks; i++) {
    redset_free(&filenames[i]);
  }
  redset_free(&filenames);

  redset_free(&rsfs);
  redset_free(&fds);
  redset_free(&header_sizes);

  for (i = 0; i < total_ranks; i++) {
    kvtree_delete(&hashes[i]);
  }
  redset_free(&hashes);
  redset_free(&missing);

  return rc;
}

/* given a (partial) list of redundancy files,
 * return redset_filelist of all source data files recorded in redundancy files,
 * and returns the number of ranks and the global rank of each process in the set */
redset_filelist redset_filelist_get_data_partner(
  int num,
  const char** files,
  int* groupsize,
  int** groupranks)
{
  /* initialize output parameters */
  *groupsize  = 0;
  *groupranks = NULL;

  int total_ranks = 0;
  int total_files = 0;
  int* global_ranks = NULL;
  kvtree** hashes = NULL;

  int i;
  for (i = 0; i < num; i++) {
    /* open the current file */
    const char* file = files[i];
    int fd = redset_open(file, O_RDONLY);
    if (fd < 0) {
      /* failed to open this file, print error and skip it */
      redset_warn("Opening PARTNER file for reading: redset_open(%s) errno=%d %s @ %s:%d",
        file, errno, strerror(errno), __FILE__, __LINE__
      );
      continue;
    }

    /* read header from the file */
    kvtree* header = kvtree_new();
    if (kvtree_read_fd(file, fd, header) < 0) {
      /* failed to read header from this file, print error and skip it */
      redset_warn("Failed to read header from `%s' @ %s:%d",
        file, __FILE__, __LINE__
      );
      kvtree_delete(&header);
      redset_close(file, fd);
      continue;
    }

    /* if this is our first file, get number of ranks in the redudancy group */
    if (hashes == NULL) {
      /* read number of items in the redudancy group */
      total_ranks = lookup_group_size(header, file);

      /* allocate space to record global rank of each member */
      global_ranks = (int*) REDSET_MALLOC(total_ranks * sizeof(int));

      /* allocate a spot to hold the file info for each member */
      hashes = (kvtree**) REDSET_MALLOC(total_ranks * sizeof(kvtree*));

      /* initialize all spots to NULL so we know whether we've already read it in */
      int j;
      for (j = 0; j < total_ranks; j++) {
        hashes[j] = NULL;
      }
    }

    /* get file info for each rank we can pull from this header */
    kvtree* desc_hash = kvtree_get(header, REDSET_KEY_COPY_PARTNER_DESC);
    kvtree_elem* rank_elem;
    for (rank_elem = kvtree_elem_first(desc_hash);
         rank_elem != NULL;
         rank_elem = kvtree_elem_next(rank_elem))
    {
      /* get the rank of the file info */
      int rank = kvtree_elem_key_int(rank_elem);

      /* copy to our array if it's not already set */
      if (hashes[rank] == NULL) {
        /* not set, get pointer to file info */
        kvtree* rank_hash = kvtree_elem_hash(rank_elem);

        /* allocate an empty kvtree and copy the file info */
        hashes[rank] = kvtree_new();
        kvtree_merge(hashes[rank], rank_hash);

        /* record global rank of this member */
        global_ranks[rank] = lookup_world_rank(hashes[rank], rank);

        /* get number of files for this rank */
        int numfiles = 0;
        kvtree_util_get_int(rank_hash, "FILES", &numfiles);

        /* sum the files to our running total across all ranks */
        total_files += numfiles;
      }
    }

    kvtree_delete(&header);
    redset_close(file, fd);
  }

  /* check that we opened at least one file to get a rank count */
  if (total_ranks == 0) {
    /* failed to read rank count from any file */
    redset_err("Failed to get group size from redudancy files @ %s:%d",
      __FILE__, __LINE__
    );
    return NULL;
  }

  /* check that we have a hash for every member in the group */
  int invalid = 0;
  for (i = 0; i < total_ranks; i++) {
    if (hashes[i] == NULL) {
      /* missing file info for some member */
      invalid = 1;
    }
  }
  if (invalid) {
    redset_err("Insufficient data to rebuild group @ %s:%d",
      __FILE__, __LINE__
    );
    redset_free(&global_ranks);
    for (i = 0; i < total_ranks; i++) {
      kvtree_delete(&hashes[i]);
    }
    redset_free(&hashes);
    return NULL;
  }

  /* allocate a list to hold files for all ranks */
  redset_list* list = (redset_list*) REDSET_MALLOC(sizeof(redset_list));
  list->count = total_files;
  list->files = (const char**) REDSET_MALLOC(total_files * sizeof(char*));

  int idx = 0;
  for (i = 0; i < total_ranks; i++) {
    /* get number of files for this rank */
    int numfiles = 0;
    kvtree_util_get_int(hashes[i], "FILES", &numfiles);

    int j;
    kvtree* files_hash = kvtree_get(hashes[i], "FILE");
    for (j = 0; j < numfiles; j++) {
      /* get file name of this file */
      kvtree* index_hash = kvtree_getf(files_hash, "%d", j);
      kvtree_elem* elem = kvtree_elem_first(index_hash);
      const char* filename = kvtree_elem_key(elem);
      list->files[idx] = strdup(filename);
      idx++;
    }

    kvtree_delete(&hashes[i]);
  }

  redset_free(&hashes);

  *groupsize  = total_ranks;
  *groupranks = global_ranks;

  return list;
}
