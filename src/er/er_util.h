#ifndef ER_UTIL_H
#define ER_UTIL_H

#include "mpi.h"
#include "kvtree.h"

/** \file er_util.h
 *  \ingroup er
 *  \brief er utilities */

#define ER_FAILURE (1)

#define ER_MAX_FILENAME (1024)

extern int er_debug;

extern int er_rank;
extern char* er_hostname;

extern int er_mpi_buf_size;
extern size_t er_page_size;

extern int er_set_size;

/** print error message to stdout */
void er_err(const char *fmt, ...);

/** print warning message to stdout */
void er_warn(const char *fmt, ...);

/** print message to stdout if er_debug is set and it is >= level */
void er_dbg(int level, const char *fmt, ...);

/** print abort message and kill run */
void er_abort(int rc, const char *fmt, ...);

/** allocate size bytes, returns NULL if size == 0,
 * calls er_abort if allocation fails */
#define ER_MALLOC(X) er_malloc(X, __FILE__, __LINE__);
void* er_malloc(size_t size, const char* file, int line);

/** pass address of pointer to be freed, frees memory if not NULL and sets pointer to NULL */
void er_free(void* ptr);

/** allocates a block of memory and aligns it to specified alignment */
void* er_align_malloc(size_t size, size_t align);

/** frees a blocked allocated with a call to er_align_malloc */
void er_align_free(void* buf);

/** sends a NUL-terminated string to a process,
 * allocates space and recieves a NUL-terminated string from a process,
 * can specify MPI_PROC_NULL as either send or recv rank */
int er_str_sendrecv(
  const char* send_str, int send_rank,
  char** recv_str, int recv_rank,
  MPI_Comm comm
);

/** returns true (non-zero) if flag on each process in comm is true */
int er_alltrue(int flag, MPI_Comm comm);

#endif
