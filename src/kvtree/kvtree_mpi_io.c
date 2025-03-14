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
#include "kvtree_util.h"

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include <limits.h>

#define KVTREE_MAP_COUNT (0)
#define KVTREE_MAP_RANKS (1)
#define KVTREE_MAP_RANK  (2)
static int pick_writer(
  int level,
  unsigned long count,
  int* outwriter,
  int* outallranks,
  MPI_Comm comm)
{
  /* use a segment size of 1MB */
  unsigned long segsize = 1024*1024;

  /* get communicator info */
  int rank, ranks;
  MPI_Comm_rank(comm, &rank);
  MPI_Comm_size(comm, &ranks);

  /* first find our offset */
  unsigned long offset;
  MPI_Scan(&count, &offset, 1, MPI_UNSIGNED_LONG, MPI_SUM, comm);
  offset -= count;

  /* determine whether we start a new segment */
  int starts_new = 0;
  if (rank == 0) {
    /* force rank 0 to start a segment, even if it has no bytes */
    starts_new = 1;
  } else if (count > 0) {
    /* otherwise we only start a segment if our bytes overflow the boundary */
    unsigned long segindex = offset / segsize;
    unsigned long seglastbyte = (segindex + 1) * segsize - 1;
    unsigned long mylastbyte  = offset + count - 1;
    if (mylastbyte > seglastbyte) {
      starts_new = 1;
    }
  }

  /* initialize our send data */
  int send[3], recv[3];
  send[KVTREE_MAP_COUNT] = starts_new;
  send[KVTREE_MAP_RANKS] = 1;
  send[KVTREE_MAP_RANK]  = MPI_PROC_NULL;
  if (starts_new) {
    send[KVTREE_MAP_RANK] = rank;
  }

  /* first execute the segmented scan */
  int step = 1;
  MPI_Request request[4];
  MPI_Status  status[4];
  while (step < ranks) {
    int k = 0;

    /* if we have a left partner, recv its right-going data */
    int left = rank - step;
    if (left >= 0) {
      MPI_Irecv(recv, 3, MPI_INT, left, 0, comm, &request[k]);
      k++;
    }

    /* if we have a right partner, send it our right-going data */
    int right = rank + step;
    if (right < ranks) {
      MPI_Isend(send, 3, MPI_INT, right, 0, comm, &request[k]);
      k++;
    }

    /* wait for all communication to complete */
    if (k > 0) {
      MPI_Waitall(k, request, status);
    }

    /* if we have a left partner, merge its data with our result */
    if (left >= 0) {
      /* reduce data into right-going buffer */
      send[KVTREE_MAP_COUNT] += recv[KVTREE_MAP_COUNT];
      if (send[KVTREE_MAP_RANK] == MPI_PROC_NULL) {
        send[KVTREE_MAP_RANKS] += recv[KVTREE_MAP_RANKS];
        send[KVTREE_MAP_RANK]   = recv[KVTREE_MAP_RANK];
      }
    }

    /* go to next round */
    step *= 2;
  }

  /* we don't use this for now, but keep it in case we go back to it */
/* int writer_id = send[KVTREE_MAP_COUNT] - 1; */

  /* set output parameters */
  *outwriter = send[KVTREE_MAP_RANK];

  /* determine whether we've finished,
   * only the last rank knows the total */
  *outallranks = (send[KVTREE_MAP_RANKS] == ranks);
  MPI_Bcast(outallranks, 1, MPI_INT, ranks-1, comm);

  return KVTREE_SUCCESS;
}

static unsigned long kvtree_write_gather_map(
  const char* prefix,
  const char* file,
  unsigned long offset,
  int level,
  int valid,
  MPI_Comm comm)
{
  int rc = KVTREE_SUCCESS;

  if (offset != 0) {
    /*
     * While we technically can write to a non-zero offset, there were issues
     * with doing so with NFS, and so we disable it for now.  It will break
     * kvtree_read_scatter_single() if we enable it too.
     */
    kvtree_abort(1, "Attempt to write a non-zero offset (got %lu) for %s/%s @ %s:%d",
      offset, prefix, file, __FILE__, __LINE__
    );
    return KVTREE_FAILURE;
  }

  /* get name of this process */
  int rank_world, ranks_world;
  MPI_Comm_rank(comm, &rank_world);
  MPI_Comm_size(comm, &ranks_world);

  /* if this process will write a file, record file name and offset */
  kvtree* hash = kvtree_new();
  unsigned long pack_size = 0;
  if (valid) {
    kvtree_util_set_str(hash, "FILE", file);
    kvtree_util_set_bytecount(hash, "OFFSET", offset);
    pack_size = (unsigned long) kvtree_pack_size(hash);
  }

  /* pick writers so that we send roughly 1MB of data to each */
  int writer, allranks;
  pick_writer(level, pack_size, &writer, &allranks, comm);

  /* create new hashes to send and receive data */
  kvtree* send = kvtree_new();
  kvtree* recv = kvtree_new();

  /* if we have valid values, queue it to send to writer */
  if (valid) {
    kvtree_exchange_sendq(send, writer, hash);
  }

  /* delete data hash */
  kvtree_delete(&hash);

  /* gather hash to writers */
  kvtree_exchange_direction(send, recv, comm, KVTREE_EXCHANGE_LEFT);

  /* we record incoming data in save hash for writing */
  kvtree* save = kvtree_new();
  if (rank_world == writer) {
    /* record hash containing file names indexed by rank,
     * attach received hash to save hash and set recv to NULL
     * since we no longer need to free it */
    kvtree_set(save, "RANK", recv);
    recv = NULL;
  }

  /* free send and receive hashes */
  kvtree_delete(&recv);
  kvtree_delete(&send);

  /* define file name */
  char mapfile[1024];
  if (allranks) {
    /* at the top most level, simplify the name so we can find it */
    snprintf(mapfile, sizeof(mapfile), "%s", "");
  } else {
    /* for all lower level maps we append a level id and writer id */
    snprintf(mapfile, sizeof(mapfile), ".%d.%d", level, writer);
  }

  char mappath[1024];
  snprintf(mappath, sizeof(mappath), "%s%s", prefix, mapfile);

  /* call gather recursively if there's another level */
  if (! allranks) {
     /* gather file names to higher level */
     unsigned long newoffset = 0;
     int newlevel = level + 1;
     int newvalid = 0;
     if (rank_world == writer) {
       newvalid = 1;
     }
     if (kvtree_write_gather_map(prefix, mapfile, newoffset, newlevel, newvalid, comm) != KVTREE_SUCCESS)
     {
       rc = KVTREE_FAILURE;
     }
  }

  /* write hash to file */
  if (rank_world == writer) {
    /* open the file if we need to */
    mode_t mode = kvtree_getmode(1, 1, 0);
    int fd = kvtree_open(mappath, O_WRONLY | O_CREAT | O_TRUNC, mode);
    if (fd >= 0) {
      /* store level value in hash */
      kvtree_util_set_int(save, "LEVEL", level);

      /* record global number of ranks */
      kvtree_util_set_int(save, "RANKS", ranks_world);

      /* persist hash */
      void* buf = NULL;
      size_t bufsize = 0;
      kvtree_write_persist(&buf, &bufsize, save);

      /* write data to file */
      kvtree_lseek(mappath, fd, offset, SEEK_SET);
      ssize_t write_rc = kvtree_write_attempt(mappath, fd, buf, bufsize);
      if (write_rc < 0) {
        rc = KVTREE_FAILURE;
      }

      /* free the buffer holding the persistent hash */
      kvtree_free(&buf);

      /* close the file */
      kvtree_close(mappath, fd);
    } else {
      kvtree_err("Opening file for write: %s @ %s:%d",
        mappath, __FILE__, __LINE__
      );
      rc = KVTREE_FAILURE;
    }
  }

  /* free the hash */
  kvtree_delete(&save);

  return rc;
}

static int kvtree_alltrue(int valid, MPI_Comm comm)
{
  int all_valid;
  MPI_Allreduce(&valid, &all_valid, 1, MPI_INT, MPI_LAND, comm);
  return all_valid;
}

/* write summary file for flush */
int kvtree_write_gather(
  const char* prefix,
  kvtree* data,
  MPI_Comm comm)
{
  int rc = KVTREE_SUCCESS;

  /* get name of this process */
  int rank_world, ranks_world;
  MPI_Comm_rank(comm, &rank_world);
  MPI_Comm_size(comm, &ranks_world);

  /* pick our writer so that we send roughly 1MB of data to each */
  int writer, allranks;
  unsigned long pack_size = (unsigned long) kvtree_pack_size(data);
  pick_writer(1, pack_size, &writer, &allranks, comm);

  /* create send and receive hashes */
  kvtree* send = kvtree_new();
  kvtree* recv = kvtree_new();

  /* copy data into send hash */
  kvtree_exchange_sendq(send, writer, data);

  /* gather hashes to writers */
  kvtree_exchange_direction(send, recv, comm, KVTREE_EXCHANGE_LEFT);

  /* persist received hash */
  kvtree* save = kvtree_new();
  if (rank_world == writer) {
    /* record hash containing file names indexed by rank,
     * attach received hash to save hash and set recv to NULL
     * since we no longer need to free it */
    kvtree_set(save, "RANK", recv);
    recv = NULL;
  }

  /* free our hash data */
  kvtree_delete(&recv);
  kvtree_delete(&send);

  /* define file name */
  char mapfile[1024];
  char mappath[1024];
  snprintf(mapfile, sizeof(mapfile), ".0.%d", writer);
  snprintf(mappath, sizeof(mappath), "%s%s", prefix, mapfile);

  /* write map to files */
  unsigned long offset = 0;
  int level = 1;
  int valid = 0;
  if (rank_world == writer) {
    valid = 1;
  }
  if (kvtree_write_gather_map(prefix, mapfile, offset, level, valid, comm) != KVTREE_SUCCESS)
  {
    rc = KVTREE_FAILURE;
  }

  /* write blocks of summary data */
  if (rank_world == writer) {
    /* open the file if we need to */
    mode_t mode = kvtree_getmode(1, 1, 0);
    int fd = kvtree_open(mappath, O_WRONLY | O_CREAT | O_TRUNC, mode);
    if (fd >= 0) {
      /* store level value in hash */
      kvtree_util_set_int(save, "LEVEL", 0);

      /* record global number of ranks */
      kvtree_util_set_int(save, "RANKS", ranks_world);

      /* persist and compress hash */
      void* buf;
      size_t bufsize;
      kvtree_write_persist(&buf, &bufsize, save);

      /* write data to file */
      kvtree_lseek(mappath, fd, offset, SEEK_SET);
      ssize_t write_rc = kvtree_write_attempt(mappath, fd, buf, bufsize);
      if (write_rc < 0) {
        rc = KVTREE_FAILURE;
      }

      /* free memory */
      kvtree_free(&buf);

      /* close the file */
      kvtree_close(mappath, fd);
    } else {
      kvtree_err( "Opening file for write: %s @ %s:%d",
        mappath, __FILE__, __LINE__
      );
      rc = KVTREE_FAILURE;
    }
  }

  /* free the hash */
  kvtree_delete(&save);

  /* determine whether everyone wrote their files ok */
  if (kvtree_alltrue(rc == KVTREE_SUCCESS, comm)) {
    return KVTREE_SUCCESS;
  }
  return KVTREE_FAILURE;
}

static int kvtree_read_scatter_map(
  const char*     prefix,
  int             depth,
  int*            ptr_valid,
  char**          ptr_file,
  unsigned long*  ptr_offset,
  MPI_Comm        comm)
{
  int rc = KVTREE_SUCCESS;

  /* get local variables so we don't have to deference everything */
  int valid            = *ptr_valid;
  char* file           = *ptr_file;
  unsigned long offset = *ptr_offset;

  /* create a hash to hold section of file */
  kvtree* hash = kvtree_new();

  /* if we can read from file do it */
  if (valid) {
    /* open file if we haven't already */
    int fd = kvtree_open(file, O_RDONLY);
    if (fd >= 0) {
      /* read our segment from the file */
      kvtree_lseek(file, fd, offset, SEEK_SET);
      ssize_t read_rc = kvtree_read_fd(file, fd, hash);
      if (read_rc < 0) {
        kvtree_err("Failed to read from %s @ %s:%d",
          file, __FILE__, __LINE__
        );
        rc = KVTREE_FAILURE;
      }

      /* close the file */
      kvtree_close(file, fd);
    } else {
      kvtree_err("Failed to open file %s @ %s:%d",
        file, __FILE__, __LINE__
      );
      rc = KVTREE_FAILURE;
    }
  }

  /* check for read errors */
  if (! kvtree_alltrue(rc == KVTREE_SUCCESS, comm)) {
    rc = KVTREE_FAILURE;
    goto cleanup;
  }

  /* create hashes to exchange data */
  kvtree* send = kvtree_new();
  kvtree* recv = kvtree_new();

  /* copy rank data into send hash */
  if (valid) {
    kvtree* rank_hash = kvtree_get(hash, "RANK");
    kvtree_merge(send, rank_hash);
  }

  /* exchange hashes */
  kvtree_exchange_direction(send, recv, comm, KVTREE_EXCHANGE_RIGHT);

  /* see if anyone sent us anything */
  int newvalid = 0;
  char* newfile = NULL;
  unsigned long newoffset = 0;
  kvtree_elem* elem = kvtree_elem_first(recv);
  if (elem != NULL) {
    /* got something, so now we'll read in the next step */
    newvalid = 1;

    /* get file name we should read */
    kvtree* elem_hash = kvtree_elem_hash(elem);
    char* value;
    if (kvtree_util_get_str(elem_hash, "FILE", &value) == KVTREE_SUCCESS)
    {
      /* return string of full path to file to caller */
      char mappath[1024];
      snprintf(mappath, sizeof(mappath), "%s%s", prefix, value);
      newfile = strdup(mappath);
    } else {
      rc = KVTREE_FAILURE;
    }

    /* get offset we should start reading from */
    if (kvtree_util_get_bytecount(elem_hash, "OFFSET", &newoffset) != KVTREE_SUCCESS)
    {
      rc = KVTREE_FAILURE;
    }
  }

  /* free the send and receive hashes */
  kvtree_delete(&recv);
  kvtree_delete(&send);

  /* get level id, and broadcast it from rank 0,
   * which we assume to be a reader in all steps */
  int level_id = -1;
  if (valid) {
    if (kvtree_util_get_int(hash, "LEVEL", &level_id) != KVTREE_SUCCESS)
    {
      rc = KVTREE_FAILURE;
    }
  }
  MPI_Bcast(&level_id, 1, MPI_INT, 0, comm);

  /* check for read errors */
  if (! kvtree_alltrue(rc == KVTREE_SUCCESS, comm)) {
    rc = KVTREE_FAILURE;
    goto cleanup;
  }

  /* set parameters for output or next iteration,
   * we already took care of updating ptr_fd earlier */
  if (valid) {
    kvtree_free(ptr_file);
  }
  *ptr_valid  = newvalid;
  *ptr_file   = newfile;
  *ptr_offset = newoffset;

  /* recurse if we still have levels to read */
  if (level_id > 1) {
    rc = kvtree_read_scatter_map(prefix, depth+1, ptr_valid, ptr_file, ptr_offset, comm);
  }

cleanup:
  /* free the hash */
  kvtree_delete(&hash);

  return rc;
}

/* read contents of summary file */
int kvtree_read_scatter(
  const char* prefix,
  kvtree* data,
  MPI_Comm comm)
{
  /* assume that we won't succeed in our fetch attempt */
  int rc = KVTREE_SUCCESS;

  int rank_world, ranks_world;
  MPI_Comm_rank(comm, &rank_world);
  MPI_Comm_size(comm, &ranks_world);

  /* build path to file */
  char mappath[1024];
  snprintf(mappath, sizeof(mappath), "%s", prefix);

  /* fetch file names and offsets containing file hash data */
  int valid = 0;
  char* file = NULL;
  unsigned long offset = 0;
  if (rank_world == 0) {
    /* rank 0 is only valid reader to start with */
    valid  = 1;
    file   = strdup(mappath);
    offset = 0;
  }
  if (kvtree_read_scatter_map(prefix, 1, &valid, &file, &offset, comm) != KVTREE_SUCCESS)
  {
    rc = KVTREE_FAILURE;
  }

  /* create hashes to exchange data */
  kvtree* send = kvtree_new();
  kvtree* recv = kvtree_new();

  /* read data from file */
  if (valid) {
    /* open file if necessary */
    int fd = kvtree_open(file, O_RDONLY);
    if (fd >= 0) {
      /* create hash to hold file contents */
      kvtree* save = kvtree_new();

      /* read hash from file */
      kvtree_lseek(file, fd, offset, SEEK_SET);
      ssize_t readsize = kvtree_read_fd(file, fd, save);
      if (readsize < 0) {
        kvtree_err("Failed to read file %s @ %s:%d",
          file, __FILE__, __LINE__
        );
        rc = KVTREE_FAILURE;
      }

      /* check that the number of ranks match */
      int ranks = 0;
      kvtree_util_get_int(save, "RANKS", &ranks);
      if (ranks != ranks_world) {
        kvtree_err("Invalid number of ranks in %s, got %d expected %d @ %s:%d",
          file, ranks, ranks_world, __FILE__, __LINE__
        );
        rc = KVTREE_FAILURE;
      }

      /* delete current send hash, set it to values from file,
       * delete file hash */
      kvtree_delete(&send);
      send = kvtree_extract(save, "RANK");
      kvtree_delete(&save);

      /* close the file */
      kvtree_close(file, fd);
    } else {
      kvtree_err("Failed to open %s @ %s:%d",
        file, __FILE__, __LINE__
      );
      rc = KVTREE_FAILURE;
    }

    /* delete file name string */
    kvtree_free(&file);
  }

  /* check that everyone read the data ok */
  if (! kvtree_alltrue(rc == KVTREE_SUCCESS, comm)) {
    kvtree_dbg(1, "kvtree_fetch_summary FAILURE @ %s:%d", __FILE__, __LINE__);
    rc = KVTREE_FAILURE;
    goto cleanup_hashes;
  }

  /* scatter to groups */
  kvtree_exchange_direction(send, recv, comm, KVTREE_EXCHANGE_RIGHT);

  /* iterate over the ranks that sent data to us, and set up our
   * list of files */
  kvtree_elem* elem;
  for (elem = kvtree_elem_first(recv);
       elem != NULL;
       elem = kvtree_elem_next(elem))
  {
    /* the key is the source rank, which we don't care about,
     * the info we need is in the element hash */
    kvtree* elem_hash = kvtree_elem_hash(elem);
    kvtree_merge(data, elem_hash);
  }

  /* check that everyone read the data ok */
  if (! kvtree_alltrue(rc == KVTREE_SUCCESS, comm)) {
    kvtree_dbg(1, "kvtree_fetch_summary FINAL FAILURE @ %s:%d", __FILE__, __LINE__);
    rc = KVTREE_FAILURE;
    goto cleanup_hashes;
  }

cleanup_hashes:
  /* delete send and receive hashes */
  kvtree_delete(&recv);
  kvtree_delete(&send);

  return rc;
}
