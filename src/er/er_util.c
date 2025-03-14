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

#include "mpi.h"
#include "kvtree.h"

#include "er.h"
#include "er_util.h"

int er_debug = 1;

int er_rank = -1;
char* er_hostname = NULL;

int er_mpi_buf_size = 1024 * 1024;
size_t er_page_size;

int er_set_size = 8;


/* print error message to stdout */
void er_err(const char *fmt, ...)
{
  va_list argp;
  fprintf(stdout, "ER %s ERROR: rank %d on %s: ", ER_VERSION, er_rank, er_hostname);
  va_start(argp, fmt);
  vfprintf(stdout, fmt, argp);
  va_end(argp);
  fprintf(stdout, "\n");
}

/* print warning message to stdout */
void er_warn(const char *fmt, ...)
{
  va_list argp;
  fprintf(stdout, "ER %s WARNING: rank %d on %s: ", ER_VERSION, er_rank, er_hostname);
  va_start(argp, fmt);
  vfprintf(stdout, fmt, argp);
  va_end(argp);
  fprintf(stdout, "\n");
}

/* print message to stdout if er_debug is set and it is >= level */
void er_dbg(int level, const char *fmt, ...)
{
  va_list argp;
  if (level == 0 || (er_debug > 0 && er_debug >= level)) {
    fprintf(stdout, "ER %s: rank %d on %s: ", ER_VERSION, er_rank, er_hostname);
    va_start(argp, fmt);
    vfprintf(stdout, fmt, argp);
    va_end(argp);
    fprintf(stdout, "\n");
  }
}

/* print abort message and call MPI_Abort to kill run */
void er_abort(int rc, const char *fmt, ...)
{
  va_list argp;
  fprintf(stderr, "ER %s ABORT: rank %d on %s: ", ER_VERSION, er_rank, er_hostname);
  va_start(argp, fmt);
  vfprintf(stderr, fmt, argp);
  va_end(argp);
  fprintf(stderr, "\n");

  MPI_Abort(MPI_COMM_WORLD, rc);
}

/* allocate size bytes, returns NULL if size == 0,
 * calls er_abort if allocation fails */
void* er_malloc(size_t size, const char* file, int line)
{
  void* ptr = NULL;
  if (size > 0) {
    ptr = malloc(size);
    if (ptr == NULL) {
      er_abort(-1, "Failed to allocate %llu bytes @ %s:%d", file, line);
    }
  }
  return ptr;
}

/* caller really passes in a void**, but we define it as just void* to avoid printing
 * a bunch of warnings */
void er_free(void* p)
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
void* er_align_malloc(size_t size, size_t align)
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
  void* start = ER_MALLOC(bytes);
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

/* frees a blocked allocated with a call to er_align_malloc */
void er_align_free(void* p)
{
  er_free(p);

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
int er_str_sendrecv(
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
    tmp_str = (char*) ER_MALLOC(recv_len);
  }

  /* exchange strings */
  MPI_Sendrecv(
    (void*) send_str, send_len, MPI_CHAR, send_rank, 999,
    (void*) tmp_str,  recv_len, MPI_CHAR, recv_rank, 999,
    comm, &status
  );

  /* return address of allocated string in caller's pointer */
  *recv_str = tmp_str;
  return ER_SUCCESS;
}

int er_alltrue(int flag, MPI_Comm comm)
{
  int all_true;
  MPI_Allreduce(&flag, &all_true, 1, MPI_INT, MPI_LAND, comm);
  return all_true;
}
