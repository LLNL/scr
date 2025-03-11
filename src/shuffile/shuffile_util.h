#ifndef SHUFFILE_UTIL_H
#define SHUFFILE_UTIL_H

#include "mpi.h"
#include "kvtree.h"

#define SHUFFILE_SUCCESS (0)
#define SHUFFILE_FAILURE (1)

#define SHUFFILE_VERSION "0.4.0"

#define SHUFFILE_MAX_FILENAME (1024)

extern int shuffile_debug;
extern int shuffile_rank;
extern char* shuffile_hostname;

extern int shuffile_mpi_buf_size;
extern size_t shuffile_page_size;

/** \file shuffile_util.h
 *  \ingroup shuffile
 *  \brief shuffile utilies */

/** print error message to stdout */
void shuffile_err(const char *fmt, ...);

/** print warning message to stdout */
void shuffile_warn(const char *fmt, ...);

/** print message to stdout if shuffile_debug is set and it is >= level */
void shuffile_dbg(int level, const char *fmt, ...);

/** print abort message and kill run */
void shuffile_abort(int rc, const char *fmt, ...);

/** allocate size bytes, returns NULL if size == 0,
 * calls shuffile_abort if allocation fails */
#define SHUFFILE_MALLOC(X) shuffile_malloc(X, __FILE__, __LINE__);
void* shuffile_malloc(size_t size, const char* file, int line);

/** pass address of pointer to be freed, frees memory if not NULL and sets pointer to NULL */
void shuffile_free(void* ptr);

/** allocates a block of memory and aligns it to specified alignment */
void* shuffile_align_malloc(size_t size, size_t align);

/** frees a blocked allocated with a call to shuffile_align_malloc */
void shuffile_align_free(void* buf);

/** sends a NUL-terminated string to a process,
 * allocates space and recieves a NUL-terminated string from a process,
 * can specify MPI_PROC_NULL as either send or recv rank */
int shuffile_str_sendrecv(
  const char* send_str, int send_rank,
  char** recv_str, int recv_rank,
  MPI_Comm comm
);

int shuffile_swap_file_names(
  const char* file_send, int rank_send,
        char* file_recv, size_t size_recv, int rank_recv,
  const char* dir_recv, MPI_Comm comm
);

/** returns true (non-zero) if flag on each process in comm is true */
int shuffile_alltrue(int flag, MPI_Comm comm);

#include "kvtree.h"

#define COPY_FILES (0)
#define MOVE_FILES (1)

int shuffile_swap_files(
  int swap_type,
  const char* file_send, kvtree* meta_send, int rank_send,
  const char* file_recv, kvtree* meta_recv, int rank_recv,
  MPI_Comm comm
);

#endif
