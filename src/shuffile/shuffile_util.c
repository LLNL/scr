#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <libgen.h>
#include <errno.h>
#include <stdarg.h>

// for stat
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <limits.h>
#include <unistd.h>

#include <fcntl.h>

/* compute crc32 */
#include <zlib.h>

#include "mpi.h"
#include "kvtree.h"
#include "kvtree_util.h"
#include "kvtree_mpi.h"

#include "shuffile_util.h"
#include "shuffile_io.h"

int shuffile_debug = 1;

int shuffile_rank = -1;
char* shuffile_hostname = NULL;

int shuffile_mpi_buf_size;
size_t shuffile_page_size;

static int shuffile_crc_on_copy = 0;

/* print error message to stdout */
void shuffile_err(const char *fmt, ...)
{
  va_list argp;
  fprintf(stdout, "SHUFFILE %s ERROR: rank %d on %s: ", SHUFFILE_VERSION, shuffile_rank, shuffile_hostname);
  va_start(argp, fmt);
  vfprintf(stdout, fmt, argp);
  va_end(argp);
  fprintf(stdout, "\n");
}

/* print warning message to stdout */
void shuffile_warn(const char *fmt, ...)
{
  va_list argp;
  fprintf(stdout, "SHUFFILE %s WARNING: rank %d on %s: ", SHUFFILE_VERSION, shuffile_rank, shuffile_hostname);
  va_start(argp, fmt);
  vfprintf(stdout, fmt, argp);
  va_end(argp);
  fprintf(stdout, "\n");
}

/* print message to stdout if shuffile_debug is set and it is >= level */
void shuffile_dbg(int level, const char *fmt, ...)
{
  va_list argp;
  if (level == 0 || (shuffile_debug > 0 && shuffile_debug >= level)) {
    fprintf(stdout, "SHUFFILE %s: rank %d on %s: ", SHUFFILE_VERSION, shuffile_rank, shuffile_hostname);
    va_start(argp, fmt);
    vfprintf(stdout, fmt, argp);
    va_end(argp);
    fprintf(stdout, "\n");
  }
}

/* print abort message and call MPI_Abort to kill run */
void shuffile_abort(int rc, const char *fmt, ...)
{
  va_list argp;
  fprintf(stderr, "SHUFFILE %s ABORT: rank %d on %s: ", SHUFFILE_VERSION, shuffile_rank, shuffile_hostname);
  va_start(argp, fmt);
  vfprintf(stderr, fmt, argp);
  va_end(argp);
  fprintf(stderr, "\n");

  MPI_Abort(MPI_COMM_WORLD, rc);
}

/* allocate size bytes, returns NULL if size == 0,
 * calls shuffile_abort if allocation fails */
void* shuffile_malloc(size_t size, const char* file, int line)
{
  void* ptr = NULL;
  if (size > 0) {
    ptr = malloc(size);
    if (ptr == NULL) {
      shuffile_abort(-1, "Failed to allocate %llu bytes @ %s:%d", file, line);
    }
  }
  return ptr;
}

/* caller really passes in a void**, but we define it as just void* to avoid printing
 * a bunch of warnings */
void shuffile_free(void* p)
{
  /* verify that we got a valid pointer to a pointer */
  if (p != NULL) {
    /* free memory if there is any */
    void* ptr = *(void**)p;
    if (ptr != NULL) {
       free(ptr);
    }

    /* set caller's pointer to NULL */
    *(void**)p = NULL;
  }
}

/* allocates a block of memory and aligns it to specified alignment */
void* shuffile_align_malloc(size_t size, size_t align)
{
  void* buf = NULL;
  if (posix_memalign(&buf, align, size) != 0) {
    return NULL;
  }
  return buf;

#if 0
  /* allocate size + one block + room to store our starting address */
  size_t bytes = size + align + sizeof(void*);

  /* allocate memory */
  void* start = SHUFFILE_MALLOC(bytes);
  if (start == NULL) {
    return NULL;
  }

  /* make room to store our starting address */
  void* buf = start + sizeof(void*);

  /* TODO: Compilers don't like modulo division on pointers */
  /* now align the buffer address to a block boundary */
  unsigned long long mask = (unsigned long long) (align - 1);
  unsigned long long addr = (unsigned long long) buf;
  unsigned long long offset = addr & mask;
  if (offset != 0) {
    buf = buf + (align - offset);
  }

  /* store the starting address in the bytes immediately before the buffer */
  void** tmp = buf - sizeof(void*);
  *tmp = start;

  /* finally, return the buffer address to the user */
  return buf;
#endif
}

/* frees a blocked allocated with a call to shuffile_align_malloc */
void shuffile_align_free(void* p)
{
  shuffile_free(p);

#if 0
  /* first lookup the starting address from the bytes immediately before the buffer */
  void** tmp = buf - sizeof(void*);
  void* start = *tmp;

  /* now free the memory */
  free(start);
#endif
}

/* sends a NUL-terminated string to a process,
 * allocates space and recieves a NUL-terminated string from a process,
 * can specify MPI_PROC_NULL as either send or recv rank */
int shuffile_str_sendrecv(
  const char* send_str, int send_rank,
  char** recv_str,      int recv_rank,
  MPI_Comm comm)
{
  MPI_Status status;

  /* get length of our send string */
  int send_len = 0;
  if (send_str != NULL) {
    send_len = strlen(send_str) + 1;
  }

  /* exchange length of strings, note that we initialize recv_len
   * so that it's valid if we recieve from MPI_PROC_NULL */
  int recv_len = 0;
  MPI_Sendrecv(
    &send_len, 1, MPI_INT, send_rank, 999,
    &recv_len, 1, MPI_INT, recv_rank, 999,
    comm, &status
  );

  /* if receive length is positive, allocate space to receive string */
  char* tmp_str = NULL;
  if (recv_len > 0) {
    tmp_str = (char*) SHUFFILE_MALLOC(recv_len);
  }

  /* exchange strings */
  MPI_Sendrecv(
    (void*) send_str, send_len, MPI_CHAR, send_rank, 999,
    (void*) tmp_str,  recv_len, MPI_CHAR, recv_rank, 999,
    comm, &status
  );

  /* return address of allocated string in caller's pointer */
  *recv_str = tmp_str;
  return SHUFFILE_SUCCESS;
}

int shuffile_alltrue(int flag, MPI_Comm comm)
{
  int all_true;
  MPI_Allreduce(&flag, &all_true, 1, MPI_INT, MPI_LAND, comm);
  return all_true;
}

/*
=========================================
File Copy Functions
=========================================
*/

int shuffile_swap_file_names(
  const char* file_send, int rank_send,
        char* file_recv, size_t size_recv, int rank_recv,
  const char* dir_recv, MPI_Comm comm)
{
  int rc = SHUFFILE_SUCCESS;

  /* determine whether we have a file to send */
  int have_outgoing = 0;
  if (rank_send != MPI_PROC_NULL &&
      file_send != NULL &&
      strcmp(file_send, "") != 0)
  {
    have_outgoing = 1;
  }

  /* nothing to send, make sure to use PROC_NULL in sendrecv call */
  if (! have_outgoing) {
    rank_send = MPI_PROC_NULL;
  }

  /* determine whether we are expecting to receive a file */
  int have_incoming = 0;
  if (rank_recv != MPI_PROC_NULL &&
      dir_recv != NULL &&
      strcmp(dir_recv, "") != 0)
  {
    have_incoming = 1;
  }

  /* nothing to recv, make sure to use PROC_NULL in sendrecv call */
  if (! have_incoming) {
    rank_recv = MPI_PROC_NULL;
  }

  /* exchange file names with partners, note that we initialize
   * file_recv_orig to NULL in case we recv from MPI_PROC_NULL */
  char* file_recv_orig = NULL;
  shuffile_str_sendrecv(file_send, rank_send, &file_recv_orig, rank_recv, comm);

  /* define the path to store our partner's file */
  if (have_incoming) {
    /* set path to file name */
    char* file_recv_orig_copy = strdup(file_recv_orig);
    //char* name = basename(file_recv_orig);
    //snprintf(file_recv, size_recv, "%s/%s", dir_recv, name);
    snprintf(file_recv, size_recv, "%s", file_recv_orig);

    /* free the file name we received */
    shuffile_free(&file_recv_orig_copy);
    shuffile_free(&file_recv_orig);
  }

  return rc;
}

static int shuffile_swap_files_copy(
  int have_outgoing, const char* file_send, kvtree* meta_send, int rank_send, uLong* crc32_send,
  int have_incoming, const char* file_recv, kvtree* meta_recv, int rank_recv, uLong* crc32_recv,
  MPI_Comm comm)
{
  int rc = SHUFFILE_SUCCESS;
  MPI_Request request[2];
  MPI_Status  status[2];

  /* allocate MPI send buffer */
  char *buf_send = NULL;
  if (have_outgoing) {
    buf_send = (char*) shuffile_align_malloc(shuffile_mpi_buf_size, shuffile_page_size);
    if (buf_send == NULL) {
      shuffile_abort(-1, "Allocating memory: malloc(%ld) errno=%d %s @ %s:%d",
              shuffile_mpi_buf_size, errno, strerror(errno), __FILE__, __LINE__
      );
      return SHUFFILE_FAILURE;
    }
  }

  /* allocate MPI recv buffer */
  char *buf_recv = NULL;
  if (have_incoming) {
    buf_recv = (char*) shuffile_align_malloc(shuffile_mpi_buf_size, shuffile_page_size);
    if (buf_recv == NULL) {
      shuffile_abort(-1, "Allocating memory: malloc(%ld) errno=%d %s @ %s:%d",
              shuffile_mpi_buf_size, errno, strerror(errno), __FILE__, __LINE__
      );
      return SHUFFILE_FAILURE;
    }
  }

  /* open the file to send: read-only mode */
  int fd_send = -1;
  if (have_outgoing) {
    fd_send = shuffile_open(file_send, O_RDONLY);
    if (fd_send < 0) {
      shuffile_abort(-1, "Opening file for send: shuffile_open(%s, O_RDONLY) errno=%d %s @ %s:%d",
              file_send, errno, strerror(errno), __FILE__, __LINE__
      );
    }
  }

  /* open the file to recv: truncate, write-only mode */
  int fd_recv = -1;
  if (have_incoming) {
    mode_t mode_file = shuffile_getmode(1, 1, 0);
    fd_recv = shuffile_open(file_recv, O_WRONLY | O_CREAT | O_TRUNC, mode_file);
    if (fd_recv < 0) {
      shuffile_abort(-1, "Opening file for recv: shuffile_open(%s, O_WRONLY | O_CREAT | O_TRUNC, ...) errno=%d %s @ %s:%d",
              file_recv, errno, strerror(errno), __FILE__, __LINE__
      );
    }
  }

  /* exchange file chunks */
  int nread, nwrite;
  int sending = 0;
  if (have_outgoing) {
    sending = 1;
  }
  int receiving = 0;
  if (have_incoming) {
    receiving = 1;
  }
  while (sending || receiving) {
    /* if we are still receiving a file, post a receive */
    if (receiving) {
      MPI_Irecv(buf_recv, shuffile_mpi_buf_size, MPI_BYTE, rank_recv, 0, comm, &request[0]);
    }

    /* if we are still sending a file, read a chunk, send it, and wait */
    if (sending) {
      nread = shuffile_read(file_send, fd_send, buf_send, shuffile_mpi_buf_size);
      if (shuffile_crc_on_copy && nread > 0) {
        *crc32_send = crc32(*crc32_send, (const Bytef*) buf_send, (uInt) nread);
      }
      if (nread < 0) {
        nread = 0;
      }
      MPI_Isend(buf_send, nread, MPI_BYTE, rank_send, 0, comm, &request[1]);
      MPI_Wait(&request[1], &status[1]);
      if (nread < shuffile_mpi_buf_size) {
        sending = 0;
      }
    }

    /* if we are still receiving a file,
     * wait on our receive to complete and write the data */
    if (receiving) {
      MPI_Wait(&request[0], &status[0]);
      MPI_Get_count(&status[0], MPI_BYTE, &nwrite);
      if (shuffile_crc_on_copy && nwrite > 0) {
        *crc32_recv = crc32(*crc32_recv, (const Bytef*) buf_recv, (uInt) nwrite);
      }
      shuffile_write(file_recv, fd_recv, buf_recv, nwrite);
      if (nwrite < shuffile_mpi_buf_size) {
        receiving = 0;
      }
    }
  }

  /* close the files */
  if (have_outgoing) {
    shuffile_close(file_send, fd_send);
  }
  if (have_incoming) {
    shuffile_close(file_recv, fd_recv);
  }

  /* set crc field on our file if it hasn't been set already */
#if 0
  if (shuffile_crc_on_copy && have_outgoing) {
    uLong meta_send_crc;
    if (shuffile_meta_get_crc32(meta_send, &meta_send_crc) != SHUFFILE_SUCCESS) {
      shuffile_meta_set_crc32(meta_send, *crc32_send);
    } else {
      /* TODO: we could check that the crc on the sent file matches and take some action if not */
    }
  }
#endif

  /* free the MPI buffers */
  shuffile_align_free(&buf_recv);
  shuffile_align_free(&buf_send);

  return rc;
}

static int shuffile_swap_files_move(
  int have_outgoing, const char* file_send, kvtree* meta_send, int rank_send, uLong* crc32_send,
  int have_incoming, const char* file_recv, kvtree* meta_recv, int rank_recv, uLong* crc32_recv,
  MPI_Comm comm)
{
  int rc = SHUFFILE_SUCCESS;
  MPI_Request request[2];
  MPI_Status  status[2];

  /* allocate MPI send buffer */
  char *buf_send = NULL;
  if (have_outgoing) {
    buf_send = (char*) shuffile_align_malloc(shuffile_mpi_buf_size, shuffile_page_size);
    if (buf_send == NULL) {
      shuffile_abort(-1, "Allocating memory: malloc(%ld) errno=%d %s @ %s:%d",
              shuffile_mpi_buf_size, errno, strerror(errno), __FILE__, __LINE__
      );
      return SHUFFILE_FAILURE;
    }
  }

  /* allocate MPI recv buffer */
  char *buf_recv = NULL;
  if (have_incoming) {
    buf_recv = (char*) shuffile_align_malloc(shuffile_mpi_buf_size, shuffile_page_size);
    if (buf_recv == NULL) {
      shuffile_abort(-1, "Allocating memory: malloc(%ld) errno=%d %s @ %s:%d",
              shuffile_mpi_buf_size, errno, strerror(errno), __FILE__, __LINE__
      );
      return SHUFFILE_FAILURE;
    }
  }

  /* since we'll overwrite our send file in place with the recv file,
   * which may be larger, we need to keep track of how many bytes we've
   * sent and whether we've sent them all */
  unsigned long filesize_send = 0;

  /* open our file */
  int fd = -1;
  if (have_outgoing) {
    /* we'll overwrite our send file (or just read it if there is no incoming) */
    filesize_send = shuffile_file_size(file_send);
    fd = shuffile_open(file_send, O_RDWR);
    if (fd < 0) {
      /* TODO: skip writes and return error? */
      shuffile_abort(-1, "Opening file for send/recv: shuffile_open(%s, O_RDWR) errno=%d %s @ %s:%d",
              file_send, errno, strerror(errno), __FILE__, __LINE__
      );
    }
  } else if (have_incoming) {
    /* if we're in this branch, then we only have an incoming file,
     * so we'll write our recv file from scratch */
    mode_t mode_file = shuffile_getmode(1, 1, 0);
    fd = shuffile_open(file_recv, O_WRONLY | O_CREAT | O_TRUNC, mode_file);
    if (fd < 0) {
      /* TODO: skip writes and return error? */
      shuffile_abort(-1, "Opening file for recv: shuffile_open(%s, O_WRONLY | O_CREAT | O_TRUNC, ...) errno=%d %s @ %s:%d",
              file_recv, errno, strerror(errno), __FILE__, __LINE__
      );
    }
  }

  /* exchange file chunks */
  int sending = 0;
  if (have_outgoing) {
    sending = 1;
  }
  int receiving = 0;
  if (have_incoming) {
    receiving = 1;
  }
  int nread, nwrite;
  off_t read_pos = 0, write_pos = 0;
  while (sending || receiving) {
    if (receiving) {
      /* prepare a buffer to receive up to shuffile_mpi_buf_size bytes */
      MPI_Irecv(buf_recv, shuffile_mpi_buf_size, MPI_BYTE, rank_recv, 0, comm, &request[0]);
    }

    if (sending) {
      /* compute number of bytes to read */
      unsigned long count = filesize_send - read_pos;
      if (count > shuffile_mpi_buf_size) {
        count = shuffile_mpi_buf_size;
      }

      /* read a chunk of up to shuffile_mpi_buf_size bytes into buf_send */
      lseek(fd, read_pos, SEEK_SET); /* seek to read position */
      nread = shuffile_read(file_send, fd, buf_send, count);
      if (shuffile_crc_on_copy && nread > 0) {
        *crc32_send = crc32(*crc32_send, (const Bytef*) buf_send, (uInt) nread);
      }
      if (nread < 0) {
        nread = 0;
      }
      read_pos += (off_t) nread; /* update read pointer */

      /* send chunk (if nread is smaller than shuffile_mpi_buf_size,
       * then we've read the whole file) */
      MPI_Isend(buf_send, nread, MPI_BYTE, rank_send, 0, comm, &request[1]);
      MPI_Wait(&request[1], &status[1]);

      /* check whether we've read the whole file */
      if (filesize_send == read_pos && count < shuffile_mpi_buf_size) {
        sending = 0;
      }
    }

    if (receiving) {
      /* count the number of bytes received */
      MPI_Wait(&request[0], &status[0]);
      MPI_Get_count(&status[0], MPI_BYTE, &nwrite);
      if (shuffile_crc_on_copy && nwrite > 0) {
        *crc32_recv = crc32(*crc32_recv, (const Bytef*) buf_recv, (uInt) nwrite);
      }

      /* write those bytes to file (if nwrite is smaller than shuffile_mpi_buf_size,
       * then we've received the whole file) */
      lseek(fd, write_pos, SEEK_SET); /* seek to write position */
      shuffile_write(file_recv, fd, buf_recv, nwrite);
      write_pos += (off_t) nwrite; /* update write pointer */

      /* if nwrite is smaller than shuffile_mpi_buf_size,
       * then assume we've received the whole file */
      if (nwrite < shuffile_mpi_buf_size) {
        receiving = 0;
      }
    }
  }

  /* close file and cleanup */
  if (have_outgoing && have_incoming) {
    /* sent and received a file; close it, truncate it to corect size, rename it */
    shuffile_close(file_send, fd);
    if (truncate(file_send, write_pos) == -1) {
      shuffile_err("Failed to truncate file: truncate(%s, %llu) errno=%d %s @ %s:%d",
              file_recv, (unsigned long long)write_pos, errno, strerror(errno), __FILE__, __LINE__
      );
    }
    rename(file_send, file_recv);
  } else if (have_outgoing) {
    /* only sent a file; close it, delete it, and remove its completion marker */
    shuffile_close(file_send, fd);
    shuffile_file_unlink(file_send);
  } else if (have_incoming) {
    /* only received a file; just need to close it */
    shuffile_close(file_recv, fd);
  }

#if 0
  if (shuffile_crc_on_copy && have_outgoing) {
    uLong meta_send_crc;
    if (shuffile_meta_get_crc32(meta_send, &meta_send_crc) != SHUFFILE_SUCCESS) {
      /* we transfer this meta data across below,
       * so may as well update these fields so we can use them */
      shuffile_meta_set_crc32(meta_send, *crc32_send);
      /* do not complete file send, we just deleted it above */
    } else {
      /* TODO: we could check that the crc on the sent file matches and take some action if not */
    }
  }
#endif

  /* free the MPI buffers */
  shuffile_align_free(&buf_recv);
  shuffile_align_free(&buf_send);

  return rc;
}

/* shuffile_swap_files -- copy or move a file from one node to another
 * if swap_type = COPY_FILES
 *   if file_send != NULL, send file_send to rank_send, who will make a copy,
 *   copy file from rank_recv if there is one to receive
 * if swap_type = MOVE_FILES
 *   if file_send != NULL, move file_send to rank_send
 *   save file from rank_recv if there is one to receive
 *   To conserve space (e.g., RAM disc), if file_send exists,
 *   any incoming file will overwrite file_send in place, one block at a time.
 *   It is then truncated and renamed according the size and name of the incoming file,
 *   or it is deleted (moved) if there is no incoming file.
 */
int shuffile_swap_files(
  int swap_type,
  const char* file_send, kvtree* meta_send, int rank_send,
  const char* file_recv, kvtree* meta_recv, int rank_recv,
  MPI_Comm comm)
{
  int rc = SHUFFILE_SUCCESS;

  /* determine whether we have a file to send */
  int have_outgoing = 0;
  if (rank_send != MPI_PROC_NULL &&
      file_send != NULL &&
      strcmp(file_send, "") != 0)
  {
    have_outgoing = 1;
  }

  /* determine whether we are expecting to receive a file */
  int have_incoming = 0;
  if (rank_recv != MPI_PROC_NULL &&
      file_recv != NULL &&
      strcmp(file_recv, "") != 0)
  {
    have_incoming = 1;
  }

  /* exchange meta file info with partners */
  kvtree_sendrecv(meta_send, rank_send, meta_recv, rank_recv, comm);

  /* initialize crc values */
  uLong crc32_send = crc32(0L, Z_NULL, 0);
  uLong crc32_recv = crc32(0L, Z_NULL, 0);

  /* exchange files */
  if (swap_type == COPY_FILES) {
    shuffile_swap_files_copy(
      have_outgoing, file_send, meta_send, rank_send, &crc32_send,
      have_incoming, file_recv, meta_recv, rank_recv, &crc32_recv,
      comm
    );
  } else if (swap_type == MOVE_FILES) {
    shuffile_swap_files_move(
      have_outgoing, file_send, meta_send, rank_send, &crc32_send,
      have_incoming, file_recv, meta_recv, rank_recv, &crc32_recv,
      comm
    );
  } else {
    shuffile_err("Unknown file transfer type: %d @ %s:%d",
            swap_type, __FILE__, __LINE__
    );
    return SHUFFILE_FAILURE;
  }

  /* mark received file as complete */
#if 0
  if (have_incoming) {
    /* check that our written file is the correct size */
    unsigned long filesize_wrote = shuffile_file_size(file_recv);
    if (shuffile_meta_check_filesize(meta_recv, filesize_wrote) != SHUFFILE_SUCCESS) {
      shuffile_err("Received file does not match expected size %s @ %s:%d",
              file_recv, __FILE__, __LINE__
      );
      shuffile_meta_set_complete(meta_recv, 0);
      rc = SHUFFILE_FAILURE;
    }

    /* check that there was no corruption in receiving the file */
    if (shuffile_crc_on_copy) {
      /* if we computed crc during the copy, and crc is set in the received meta data
       * check that our computed value matches */
      uLong crc32_recv_meta;
      if (shuffile_meta_get_crc32(meta_recv, &crc32_recv_meta) == SHUFFILE_SUCCESS) {
        if (crc32_recv != crc32_recv_meta) {
          shuffile_err("CRC32 mismatch detected when receiving file %s @ %s:%d",
                  file_recv, __FILE__, __LINE__
          );
          shuffile_meta_set_complete(meta_recv, 0);
          rc = SHUFFILE_FAILURE;
        }
      }
    }
  }
#endif

  return rc;
}
