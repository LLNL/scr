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

#include "scr_globals.h"
#include "scr_cache_index.h"
#include "scr_storedesc.h"

#include "spath.h"
#include "kvtree.h"
#include "dtcmp.h"

#include <sys/types.h>
#include <dirent.h>

/* delete named dataset from index file in prefix directory */
static int scr_prefix_remove_index(const char* name)
{
  /* delete from index file */
  if (scr_my_rank_world == 0) {
      /* read the index file */
      kvtree* index_hash = kvtree_new();
      if (scr_index_read(scr_prefix_path, index_hash) == SCR_SUCCESS) {
        /* if there is an entry for this dataset, remove it */
        int id;
        if (scr_index_get_id_by_name(index_hash, name, &id) == SCR_SUCCESS) {
          /* found an entry, remove it and update the index file */
          scr_index_remove(index_hash, name);
          scr_index_write(scr_prefix_path, index_hash);
        }
      }
      kvtree_delete(&index_hash);
  }

  /* hold everyone until delete is complete */
  MPI_Barrier(scr_comm_world);

  return SCR_SUCCESS;
}

/* open dirname, scan entries, and delete them */
static int scr_prefix_rmscan(const char* dirname)
{
  int rc = SCR_SUCCESS;

  /* scan over all items in the directory and delete them */
  DIR* dirp = opendir(dirname);
  if (dirp != NULL) {
    /* opened the directory, now scan over each item */
    struct dirent* de;
    while ((de = readdir(dirp))) {
      /* get name of the current item */
      char* name = de->d_name;

      /* skip "." and ".." */
      if (strcmp(name, ".")  == 0 ||
          strcmp(name, "..") == 0)
      {
        continue;
      }

      /* got an item, build full path to it */
      spath* path = spath_from_str(dirname);
      spath_append_str(path, name);
      char* item = spath_strdup(path);
      spath_delete(&path);

      /* delete the item */
      scr_file_unlink(item);

      scr_free(&item);
    }

    /* done deleting stuff from this directory */
    closedir(dirp);
  }

  /* delete scr dataset directory itself */
  scr_rmdir(dirname);

  return rc;
}

/* deletes user data files from prefix directory for named dataset */
static int scr_prefix_delete_data(int id)
{
  int rc = SCR_SUCCESS;

  /* build path to dataset directory under prefix */
  spath* dataset_path = spath_from_str(scr_prefix_scr);
  spath_append_strf(dataset_path, "scr.dataset.%d", id);

  /* TODO: this is stepping on Filo internals */
  /* build path to rank2file */
  spath* rank2file_path = spath_dup(dataset_path);
  spath_append_str(rank2file_path, "rank2file");
  const char* rank2file = spath_strdup(rank2file_path);
  spath_delete(&rank2file_path);
  spath_delete(&dataset_path);

  /* get the list of files to read */
  kvtree* filelist = kvtree_new();
  if (kvtree_read_scatter(rank2file, filelist, scr_comm_world) != KVTREE_SUCCESS) {
    /* failed to read list of files in this dataset */
    scr_free(&rank2file);  
    kvtree_delete(&filelist);
    return SCR_FAILURE;
  }

  /* done with rank2file */
  scr_free(&rank2file);  

  /* allocate list of file names */
  kvtree* files = kvtree_get(filelist, "FILE");

  /* delete files and count up number of directories */
  int num_dirs = 0;
  int min_depth = -1;
  int max_depth = -1;
  kvtree_elem* elem;
  for (elem = kvtree_elem_first(files);
       elem != NULL;
       elem = kvtree_elem_next(elem))
  {
    /* get the file name */
    char* file = kvtree_elem_key(elem);

    /* build full path to the file under the prefix directory */
    spath* file_path = spath_dup(scr_prefix_path);
    spath_append_str(file_path, file);
    spath_reduce(file_path);
    char* src_file = spath_strdup(file_path);

    /* delete the file */
    scr_file_unlink(src_file);

    /* free file path string */
    scr_free(&src_file);

    /* now get the directory portion */
    spath_dirname(file_path);
    if (spath_is_child(scr_prefix_path, file_path)) {
      int parent_components = spath_components(scr_prefix_path);
      int target_components = spath_components(file_path);
      num_dirs += target_components - parent_components;

      if (min_depth == -1 || parent_components < min_depth) {
        min_depth = parent_components;
      }
      if (max_depth == -1 || target_components > max_depth) {
        max_depth = target_components;
      }
    }
    spath_delete(&file_path);
  }

  /* identify minimum rank with a valid value */
  int source;
  int source_rank = scr_ranks_world;
  if (min_depth != -1) {
    source_rank = scr_my_rank_world;
  }
  MPI_Allreduce(&source_rank, &source, 1, MPI_INT, MPI_MIN, scr_comm_world);

  /* delete directories for user dataset files if any rank found them */
  if (source < scr_ranks_world) {
    /* some rank has defined min/max values,
     * get min_depth from that rank */
    int min_source = min_depth;
    MPI_Bcast(&min_source, 1, MPI_INT, source, scr_comm_world);

    /* initialize our own min/max if needed */
    if (min_depth == -1) {
      min_depth = min_source;
    }
    if (max_depth == -1) {
      max_depth = min_source;
    }

    /* get global min and max values across all procs */
    int min_global, max_global;
    MPI_Allreduce(&min_depth, &min_global, 1, MPI_INT, MPI_MIN, scr_comm_world);
    MPI_Allreduce(&max_depth, &max_global, 1, MPI_INT, MPI_MAX, scr_comm_world);

    /* allocate memory to hold list of each of our directories */
    char** dirs = (char**) SCR_MALLOC(num_dirs * sizeof(char*));
    int* depths = (int*)   SCR_MALLOC(num_dirs * sizeof(int));

    /* get list of directories */
    int i = 0;
    for (elem = kvtree_elem_first(files);
         elem != NULL;
         elem = kvtree_elem_next(elem))
    {
      /* get the file name */
      char* file = kvtree_elem_key(elem);

      /* build full path to the file under the prefix directory */
      spath* file_path = spath_dup(scr_prefix_path);
      spath_append_str(file_path, file);
      spath_reduce(file_path);

      /* now get the directory portion */
      spath_dirname(file_path);
      if (spath_is_child(scr_prefix_path, file_path)) {
        /* work back for each directory component from the file
         * to the prefix directory */
        int parent_components = spath_components(scr_prefix_path);
        int target_components = spath_components(file_path);
        while (target_components > parent_components) {
          /* get a copy of this directory string and its depth */
          char* dir = spath_strdup(file_path);
          dirs[i]   = dir;
          depths[i] = target_components;
          i++;

          /* chop off another component and try again */
          spath_dirname(file_path);
          target_components--;
        }
      }

      /* release the path object for this file */
      spath_delete(&file_path);
    }

    /* compute union of directories to identify leaders */
    uint64_t groups;
    uint64_t* group_id    = (uint64_t*) SCR_MALLOC(sizeof(uint64_t) * num_dirs);
    uint64_t* group_ranks = (uint64_t*) SCR_MALLOC(sizeof(uint64_t) * num_dirs);
    uint64_t* group_rank  = (uint64_t*) SCR_MALLOC(sizeof(uint64_t) * num_dirs);
    int dtcmp_rc = DTCMP_Rankv_strings(
      num_dirs, (const char**) dirs, &groups, group_id, group_ranks, group_rank,
      DTCMP_FLAG_NONE, scr_comm_world
    );
    if (dtcmp_rc != DTCMP_SUCCESS) {
      rc = SCR_FAILURE;
    }

    /* delete directories from bottom level to top */
    int depth;
    for (depth = max_global; depth >= min_global; depth--) {
      /* iterate over each directory we have,
       * delete it if it's at the right level and
       * if we are the designated leader */
      for (i = 0; i < num_dirs; i++) {
        if (depths[i] == depth &&
            group_rank[i] == 0)
        {
          /* will naturally fail to delete non-empty directories */
          scr_rmdir(dirs[i]);
        }
      }

      /* execute barrier to ensure everyone is done with this level
       * before we move a level up */
      MPI_Barrier(scr_comm_world);
    }

    /* free dtcmp buffers */
    scr_free(&group_id);
    scr_free(&group_ranks);
    scr_free(&group_rank);

    /* free memory allocated for directory list */
    for (i = 0; i < num_dirs; i++) {
      /* free directory name strings */
      scr_free(&dirs[i]);
    }
    scr_free(&dirs);
    scr_free(&depths);
  }

  /* done with the list of files */
  kvtree_delete(&filelist);

  return rc;
}

/* delete named dataset from the prefix directory */
int scr_prefix_delete(int id, const char* name)
{
  int rc = SCR_SUCCESS;

  /* print a debug messages */
  if (scr_my_rank_world == 0) {
    scr_dbg(1, "Deleting dataset %d `%s' from `%s'", id, name, scr_prefix);
  }
  
  /* first, delete user data files from prefix directory */
  scr_prefix_delete_data(id);

  /* delete files within scr.dataset.id directory,
   * this is most likely just the summary and rank2file files,
   * but we do this by scanning and deleting items
   * in case we happened to execute a scavenge in which case
   * we'll also have lots of redundancy and filemap files */
  if (scr_my_rank_world == 0) {
    /* build path to dataset directory under prefix */
    spath* dataset_path = spath_from_str(scr_prefix_scr);
    spath_append_strf(dataset_path, "scr.dataset.%d", id);
    char* dataset_dir = spath_strdup(dataset_path);
    spath_delete(&dataset_path);

    /* scan over all items in the directory and delete them */
    scr_prefix_rmscan(dataset_dir);

    /* free dataset directory name */
    scr_free(&dataset_dir);
  }

  /* drop the entry from the index file */
  scr_prefix_remove_index(name);

  /* hold everyone until delete is complete */
  MPI_Barrier(scr_comm_world);

  return rc;
}

/* keep a sliding window of checkpoints in the prefix directory,
 * delete any pure checkpoints that fall outside of the window
 * defined by the given dataset id and the window width,
 * excludes checkpoints that are marked as output */
int scr_prefix_delete_sliding(int id, int window)
{
  /* rank 0 reads the index file */
  kvtree* index_hash = NULL;
  int read_index_file = 0;
  if (scr_my_rank_world == 0) {
    /* create an empty hash to store our index */
    index_hash = kvtree_new();

    /* read the index file */
    if (scr_index_read(scr_prefix_path, index_hash) == SCR_SUCCESS) {
      read_index_file = 1;
    }
  }

  /* don't enter while loop below if rank 0 failed to read index file */
  int continue_deleting = 1;
  MPI_Bcast(&read_index_file, 1, MPI_INT, 0, scr_comm_world);
  if (! read_index_file) {
    continue_deleting = 0;
  }

  /* we count the current checkpoint as a member of the window */
  window--;

  /* iterate over all checkpoints in the prefix directory,
   * deleting any pure checkpoints that fall outside of the window */
  int target_id = id;
  while (continue_deleting) {
    /* rank 0 determines the directory to fetch from */
    char target[SCR_MAX_FILENAME];
    if (scr_my_rank_world == 0) {
      /* TODO: delete checkpoint if not valid, even if in window? */

      /* get the most recent complete checkpoint older than the target id */
      int next_id = -1;
      scr_index_get_most_recent_complete(index_hash, target_id, &next_id, target);
      target_id = next_id;

      /* found the next most recent checkpoint,
       * consider whether we should keep it */
      if (target_id >= 0) {
        /* keep this checkpoint if we're still in the window */
        if (window > 0) {
          /* saved by the window, look for something older */
          window--;
          continue;
        }

        /* not in window, but we also keep any checkpoints
         * that are marked as output */
        scr_dataset* dataset = scr_dataset_new();
        if (scr_index_get_dataset(index_hash, target_id, target, dataset) == SCR_SUCCESS) {
          /* get output flag for this dataset */
          int is_output = scr_dataset_is_output(dataset);
          if (is_output) {
            /* this checkpoint is also marked as output, so don't delete it */
            scr_dataset_delete(&dataset);
            continue;
          }
        }
        scr_dataset_delete(&dataset);
      }
    }

    /* broadcast target id from rank 0 */
    MPI_Bcast(&target_id, 1, MPI_INT, 0, scr_comm_world);

    /* if we got an id, delete it, otherwise we're done */
    if (target_id >= 0) {
      /* get name from rank 0 */
      scr_strn_bcast(target, sizeof(target), 0, scr_comm_world);

      /* delete this dataset from the prefix directory */
      scr_prefix_delete(target_id, target);

      /* remove dataset from index hash */
      if (scr_my_rank_world == 0) {
        scr_index_remove(index_hash, target);
      }
    } else {
      /* ran out of checkpoints to consider */
      continue_deleting = 0;
    }
  }

  /* delete the index hash */
  if (scr_my_rank_world == 0) {
    kvtree_delete(&index_hash);
  }

  /* hold everyone until delete is complete */
  MPI_Barrier(scr_comm_world);

  return SCR_SUCCESS;
}

/* delete all datasets listed in the index file,
 * both checkpoint and output */
int scr_prefix_delete_all(void)
{
  /* rank 0 reads the index file */
  kvtree* index_hash = NULL;
  int read_index_file = 0;
  if (scr_my_rank_world == 0) {
    /* create an empty hash to store our index */
    index_hash = kvtree_new();

    /* read the index file */
    if (scr_index_read(scr_prefix_path, index_hash) == SCR_SUCCESS) {
      read_index_file = 1;
    }
  }

  /* don't enter while loop below if rank 0 failed to read index file */
  int continue_deleting = 1;
  MPI_Bcast(&read_index_file, 1, MPI_INT, 0, scr_comm_world);
  if (! read_index_file) {
    continue_deleting = 0;
  }

  /* iterate and delete each dataset in the prefix directory */
  while (continue_deleting) {
    /* rank 0 determines the directory to fetch from */
    int target_id;
    char target[SCR_MAX_FILENAME];
    if (scr_my_rank_world == 0) {
      /* get the oldest dataset id */
      scr_index_get_oldest(index_hash, &target_id, target);
    }

    /* broadcast target id from rank 0 */
    MPI_Bcast(&target_id, 1, MPI_INT, 0, scr_comm_world);

    /* if we got an id, delete it, otherwise we're done */
    if (target_id >= 0) {
      /* get name from rank 0 */
      scr_strn_bcast(target, sizeof(target), 0, scr_comm_world);

      /* delete this dataset from the prefix directory */
      scr_prefix_delete(target_id, target);

      /* remove dataset from index hash */
      if (scr_my_rank_world == 0) {
        scr_index_remove(index_hash, target);
      }
    } else {
      /* ran out of checkpoints to consider */
      continue_deleting = 0;
    }
  }

  /* delete the index hash */
  if (scr_my_rank_world == 0) {
    kvtree_delete(&index_hash);
  }

  /* hold everyone until delete is complete */
  MPI_Barrier(scr_comm_world);

  return SCR_SUCCESS;
}
