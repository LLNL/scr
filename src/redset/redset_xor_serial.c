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

#define REDSET_KEY_COPY_XOR_DESC    "DESC"
#define REDSET_KEY_COPY_XOR_RANKS   "RANKS"
#define REDSET_KEY_COPY_XOR_RANK    "RANK"
#define REDSET_KEY_COPY_XOR_GROUPS  "GROUPS"
#define REDSET_KEY_COPY_XOR_GROUP   "GROUP"
#define REDSET_KEY_COPY_XOR_FILES   "FILES"
#define REDSET_KEY_COPY_XOR_FILE    "FILE"
#define REDSET_KEY_COPY_XOR_SIZE    "SIZE"
#define REDSET_KEY_COPY_XOR_CHUNK   "CHUNK"

#ifdef REDSET_GLOBALS_H
#error "globals.h accessed from tools"
#endif

static int buffer_size = 128*1024;

/* execute xor operation with N-1 files and xor file: 
     open each XOR file and read header to get info for user files
     open each user file
     open missing user file
     open missing XOR file
     for all chunks
       read a chunk from missing file (xor file) into memory buffer A
       for each other file i
         read chunk from file i into memory buffer B
         merge chunks and store in memory buffer A
       write chunk in memory buffer A to missing file
     close all files
*/

/* given a header, get group rank of process that owns this header */
static int lookup_group_rank(const kvtree* header, const char* file)
{
  int rank = -1;
  if (kvtree_util_get_int(header, REDSET_KEY_COPY_XOR_RANK, &rank) != KVTREE_SUCCESS) {
    redset_err("Failed to read group rank from redundancy file header in %s @ %s:%d",
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
  kvtree* rank_hash = kvtree_get_kv_int(header, REDSET_KEY_COPY_XOR_DESC, group_rank);
  kvtree* desc_hash = kvtree_get(rank_hash, REDSET_KEY_COPY_XOR_DESC);
  if (kvtree_util_get_int(desc_hash, REDSET_KEY_CONFIG_GROUP_SIZE, &ranks) != KVTREE_SUCCESS) {
    redset_err("Failed to read group size from redundancy file header in %s @ %s:%d",
      file, __FILE__, __LINE__
    );
  }
  return ranks;
}

static unsigned long lookup_chunk_size(const kvtree* header, const char* file)
{
  /* read the chunk size */
  unsigned long chunk_size = 0;
  if (kvtree_util_get_unsigned_long(header, REDSET_KEY_COPY_XOR_CHUNK, &chunk_size) != KVTREE_SUCCESS) {
    redset_err("Failed to read chunk size from XOR file header in %s @ %s:%d",
      file, __FILE__, __LINE__
    );
  }
  return chunk_size;
}

/* given a header, lookup and return global rank of a process given rank in its group */
static int lookup_world_rank(const kvtree* hash, int group_rank)
{
  int rank = -1;
  kvtree* desc_hash = kvtree_get(hash, REDSET_KEY_COPY_XOR_DESC);
  if (kvtree_util_get_int(desc_hash, REDSET_KEY_CONFIG_WORLD_RANK, &rank) != KVTREE_SUCCESS) {
    redset_err("Failed to read world rank from file header in @ %s:%d",
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
  kvtree* desc_hash = kvtree_get(hash, REDSET_KEY_COPY_XOR_DESC);

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

static int redset_recover_xor_rebuild_serial(
  int xor_set_size,
  int root,
  char* xor_files[],
  int xor_fds[],
  redset_lofi* rsfs,
  size_t chunk_size)
{
  int i, j;
  int rc = REDSET_SUCCESS;

  /* allocate buffers */
  char* buffer_A = REDSET_MALLOC(buffer_size * sizeof(char));
  char* buffer_B = REDSET_MALLOC(buffer_size * sizeof(char));
  if (buffer_A == NULL) {
    redset_err("Failed to allocate buffer memory @ %s:%d",
      __FILE__, __LINE__
    );
    return REDSET_FAILURE;
  }
  if (buffer_B == NULL) {
    redset_err("Failed to allocate buffer memory @ %s:%d",
      __FILE__, __LINE__
    );
    redset_free(&buffer_A);
    return REDSET_FAILURE;
  }

  /* this offset array records the current position we are in the logical file for each rank */
  unsigned long* offset = REDSET_MALLOC(xor_set_size * sizeof(unsigned long));
  if (offset == NULL) {
    redset_err("Failed to allocate buffer memory @ %s:%d",
      __FILE__, __LINE__
    );
    return REDSET_FAILURE;
  }
  for (i = 0; i < xor_set_size; i++) {
    offset[i] = 0;
  }

  unsigned long write_pos = 0;
  int chunk_id;
  for (chunk_id = 0; chunk_id < xor_set_size && rc == 0; chunk_id++) {
    size_t nread = 0;
    while (nread < chunk_size && rc == 0) {
      /* read upto buffer_size bytes at a time */
      size_t count = chunk_size - nread;
      if (count > buffer_size) {
        count = buffer_size;
      }

      /* clear our buffer */
      memset(buffer_A, 0, count);

      /* read a segment from each rank and XOR it into our buffer */
      for (i = 0; i < xor_set_size; i++) {
        /* don't need to read from root */
        if (i == root) {
          continue;
        }

        /* read the next set of bytes for this chunk from my file into send_buf */
        if (chunk_id != i) {
          /* read chunk from the logical file for this rank */
          if (redset_lofi_pread(&rsfs[i], buffer_B, count, offset[i]) != REDSET_SUCCESS)
          {
            /* our read failed, set the return code to an error */
            rc = REDSET_FAILURE;
            count = 0;
          }
          offset[i] += count;
        } else {
          /* read chunk from the XOR file for this rank */
          if (redset_read_attempt(xor_files[i], xor_fds[i], buffer_B, count) != count) {
            /* our read failed, set the return code to an error */
            rc = REDSET_FAILURE;
            count = 0;
          }
        }

        /* TODO: XORing with unsigned long would be faster here (if chunk size is multiple of this size) */
        /* merge the blocks via xor operation */
        for (j = 0; j < count; j++) {
          buffer_A[j] ^= buffer_B[j];
        }
      }

      /* at this point, we have the data from the missing rank, write it out */
      if (chunk_id != root) {
        /* write chunk to logical file for the missing rank */
        if (redset_lofi_pwrite(&rsfs[root], buffer_A, count, write_pos) != REDSET_SUCCESS)
        {
          /* our write failed, set the return code to an error */
          rc = REDSET_FAILURE;
        }
        write_pos += count;
      } else {
        /* write chunk to xor file for the missing rank */
        if (redset_write_attempt(xor_files[root], xor_fds[root], buffer_A, count) != count) {
          /* our write failed, set the return code to an error */
          rc = REDSET_FAILURE;
        }
      }

      nread += count;
    }
  }

  redset_free(&offset);

  redset_free(&buffer_B);
  redset_free(&buffer_A);

  return rc;
}

int redset_rebuild_xor(
  int num,
  const char** files,
  const char* prefix,
  const kvtree* map)
{
  int rc = REDSET_SUCCESS;

  size_t chunk_size = 0;
  kvtree* group_map = kvtree_new();

  int total_ranks = 0;
  int* missing    = NULL;
  kvtree** hashes = NULL;

  int i;
  for (i = 0; i < num; i++) {
    /* open the current file */
    const char* file = files[i];
    int fd = redset_open(file, O_RDONLY);
    if (fd < 0) {
      redset_warn("Opening redundancy file for reading: redset_open(%s) errno=%d %s @ %s:%d",
        file, errno, strerror(errno), __FILE__, __LINE__
      );
      continue;
    }

    /* read header from the file */
    kvtree* header = kvtree_new();
    if (kvtree_read_fd(file, fd, header) < 0) {
      /* failed to read header from this file */
      redset_warn("Failed to read header from redundancy file `%s' @ %s:%d",
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

      /* get chunk size used for this set */
      chunk_size = (size_t) lookup_chunk_size(header, file);

      /* copy group map */
      kvtree* group_hash = kvtree_get(header, REDSET_KEY_COPY_XOR_GROUP);
      kvtree_merge(group_map, group_hash);

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
    kvtree* desc_hash = kvtree_get(header, REDSET_KEY_COPY_XOR_DESC);
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
    redset_err("Failed to get group size from redundancy files @ %s:%d",
      __FILE__, __LINE__
    );
    redset_free(&hashes);
    redset_free(&missing);
    kvtree_delete(&group_map);
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
    kvtree_delete(&group_map);
    return REDSET_FAILURE;
  }

  /* check that we can identify set of files for all procs in the set */
  for (i = 0; i < total_ranks; i++) {
    const kvtree* current_hash = hashes[i];
    if (redset_lofi_check_mapped(current_hash, map) != REDSET_SUCCESS) {
      missing[i] = 1;
    }
  }

  /* count number of ranks we're missing, and record the group rank of the root */
  int missing_count = 0;
  int root = -1;
  for (i = 0; i < total_ranks; i++) {
    if (missing[i]) {
      missing_count++;
      root = i;
    }
  }

  /* if nothing is missing, nothing else to do, we can exit early with success */
  if (missing_count == 0) {
    for (i = 0; i < total_ranks; i++) {
      kvtree_delete(&hashes[i]);
    }
    redset_free(&hashes);
    redset_free(&missing);
    kvtree_delete(&group_map);
    return REDSET_SUCCESS;
  }

  /* check that we're not missing data from too many processes */
  if (missing_count > 1) {
    redset_err("Insufficient data to rebuild group @ %s:%d",
      __FILE__, __LINE__
    );
    for (i = 0; i < total_ranks; i++) {
      kvtree_delete(&hashes[i]);
    }
    redset_free(&hashes);
    redset_free(&missing);
    kvtree_delete(&group_map);
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

  /* read in the xor filenames (expected to be in order of XOR segment number) */
  /* we order ranks so that root is index 0, the rank to the right of root is index 1, and so on */
  for (i = 0; i < total_ranks; i++) {
    /* get file info for the current process */
    kvtree* current_hash = hashes[i];

    /* lookup group membership info for this process */
    int group_num, group_id, group_size, group_rank, world_size, world_rank;
    lookup_group_info(
      current_hash, &group_num, &group_id, &group_size, &group_rank, &world_size, &world_rank
    );

    /* define name for partner file */
    char redfile_name[1024];
    snprintf(redfile_name, sizeof(redfile_name), "%s%d.xor.grp_%d_of_%d.mem_%d_of_%d.redset",
      prefix, world_rank, group_id+1, group_num, group_rank+1, group_size
    );
    filenames[i] = strdup(redfile_name);

    /* if this process is missing, open its files for writing,
     * otherwise open them for reading */
    if (missing[i]) {
      /* open our data files for writing */
      mode_t mode_file = redset_getmode(1, 1, 0);
      if (redset_lofi_open_mapped(current_hash, map, O_WRONLY | O_CREAT | O_TRUNC, mode_file, &rsfs[i]) != REDSET_SUCCESS) {
        redset_err("Opening user data files for writing @ %s:%d",
          __FILE__, __LINE__
        );
        /* TODO: would be nice to clean up memory */
        return REDSET_FAILURE;
      }

      /* open redundancy file for writing */
      fds[i] = redset_open(filenames[i], O_WRONLY | O_CREAT | O_TRUNC, mode_file);
      if (fds[i] < 0) {
        redset_err("Opening redundancy file to be reconstructed: redset_open(%s) errno=%d %s @ %s:%d",
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

      /* open redundancy file for reading */
      fds[i] = redset_open(filenames[i], O_RDONLY);
      if (fds[i] < 0) {
        redset_err("Opening redundancy file to be read: redset_open(%s) errno=%d %s @ %s:%d",
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
      for (dist = 0; dist <= 1; dist++) {
        int rank = (root - dist + total_ranks) % total_ranks;
        kvtree* desc_hash = kvtree_new();
        kvtree_merge(desc_hash, hashes[rank]);
        kvtree_setf(header, desc_hash, "%s %d", REDSET_KEY_COPY_XOR_DESC, rank);
      }
    
      /* set our rank within the group */
      kvtree_util_set_int(header, REDSET_KEY_COPY_XOR_RANK, i);
    
      /* write out the chunk size */
      kvtree_util_set_unsigned_long(header, REDSET_KEY_COPY_XOR_CHUNK, chunk_size);

      /* write out the group map */
      kvtree* group_hash = kvtree_new();
      kvtree_merge(group_hash, group_map);
      kvtree_set(header, REDSET_KEY_COPY_XOR_GROUP, group_hash);

      /* sort header before writing */
      redset_sort_kvtree(header);
    
      /* write the header to the XOR file of the missing rank */
      if (kvtree_write_fd(filenames[i], fds[i], header) < 0) {
        rc = REDSET_FAILURE;
      }
      kvtree_delete(&header);
    
      /* make note of header size */
      header_sizes[i] = lseek(fds[i], 0, SEEK_CUR);
    }
  }

  /* apply xor encoding */
  if (rc == REDSET_SUCCESS) {
    rc = redset_recover_xor_rebuild_serial(total_ranks, root, filenames, fds, rsfs, chunk_size);
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

  /* if the write failed, delete the files we just wrote, and return an error */
  if (rc != REDSET_SUCCESS) {
    /* TODO: unlink files */
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
  kvtree_delete(&group_map);

  return rc;
}

redset_filelist redset_filelist_get_data_xor(
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
      redset_err("Opening XOR file for reading: redset_open(%s) errno=%d %s @ %s:%d",
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
    kvtree* desc_hash = kvtree_get(header, REDSET_KEY_COPY_XOR_DESC);
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
    if (hashes[i] == NULL) {
      /* ERROR! */
    }

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
