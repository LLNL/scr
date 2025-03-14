#ifndef SPATH_H
#define SPATH_H

#include <stdarg.h>
#include <sys/types.h>

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
extern "C" {
#endif

#define SPATH_SUCCESS (0)
#define SPATH_FAILURE (1)

#define SPATH_VERSION "0.4.0"

/* TODO: for formatted strings, use special %| character (or something
 * similar) to denote directories in portable way */

/** \defgroup spath SPath
 *  \brief Represent and manipulate file system paths
 *
 * Create an spath object from a string. The library includes
 * functions to extract components (such as dirname, basename). It can
 * create an absolute path or compute a relative path from a source
 * path to a destination path. It can also simplify a path (i.e.,
 * convert `../foo//bar` to `foo/bar`). */

/** \file spath.h
 *  \ingroup spath
 *  \brief This file defines the data structure for a path,
 *  which is an double-linked list of elements,
 *  where each element contains a component (char string).
 *
 * Stores path as a linked list, breaking path at each directory marker
 * and terminating NUL.  Can append and insert paths or cut and slice
 * them.  Can initialize a path from a string and extract a path into
 * a string.  Path consists of a number of components indexed from 0.
 *
 * Examples:
 * * root directory "/" consists of a path with two components both of
 *   which are empty strings */

/********************************************************/
/** \name Define hash and element structures */
///@{
struct spath_elem_struct;

/** define the structure for a path element */
typedef struct spath_elem_struct {
  char* component; /* pointer to strdup'd component string */
  size_t chars;    /* number of chars in component */
  struct spath_elem_struct* next; /* pointer to next element */
  struct spath_elem_struct* prev; /* pointer to previous element */
} spath_elem;

/** define the structure for a path object */
typedef struct spath_struct {
  int components;      /* number of components in path */
  size_t chars;        /* number of chars in path */
  spath_elem* head; /* pointer to first element */
  spath_elem* tail; /* pointer to last element */
} spath;
///@}

/********************************************************/
/** \name Allocate and delete path objects */
///@{
/** allocates a new path */
spath* spath_new(void);

/** allocates a path from string */
spath* spath_from_str(const char* str);

/** allocates a path from formatted string */
spath* spath_from_strf(const char* format, ...);

/** allocates and returns a copy of path */
spath* spath_dup(const spath* path);

/** frees a path and sets path pointer to NULL */
int spath_delete(spath** ptr_path);
///@}

/********************************************************/
/** \name get size and string functions */
///@{

/** returns 1 if path has 0 components, 0 otherwise */
int spath_is_null(const spath* path);

/** return number of components in path */
int spath_components(const spath* path);

/** return number of characters needed to store path
 * (excludes terminating NUL) */
size_t spath_strlen(const spath* path);

/** copy string into user buffer, abort if buffer is too small,
 * return number of bytes written */
size_t spath_strcpy(char* buf, size_t n, const spath* path);

/** allocate memory and return path in string form,
 * caller is responsible for freeing string with free() */
char* spath_strdup(const spath* path);
///@}

/********************************************************/
/** \name insert, append, prepend functions */
///@{

/** inserts path2 so head element in path2 starts at specified offset
 * in path1, e.g.,
 * - 0   - before first element of path1
 * - N-1 - before last element of path1
 * - N   - after last element of path1 */
int spath_insert(spath* path1, int offset, const spath* ptr_path2);

/** prepends path2 to path1 */
int spath_prepend(spath* path1, const spath* ptr_path2);

/** appends path2 to path1 */
int spath_append(spath* path1, const spath* ptr_path2);

/** inserts components in string so first component in string starts
 * at specified offset in path, e.g.,
 * - 0   - before first element of path
 * - N-1 - before last element of path
 * - N   - after last element of path */
int spath_insert_str(spath* path, int offset, const char* str);

/** prepends components in string to path */
int spath_prepend_str(spath* path, const char* str);

/** appends components in string to path */
int spath_append_str(spath* path, const char* str);

/** inserts components in string so first component in string starts
 * at specified offset in path, e.g.,
 * - 0   - before first element of path
 * - N-1 - before last element of path
 * - N   - after last element of path */
int spath_insert_strf(spath* path, int offset, const char* format, ...);

/** prepends components in string to path */
int spath_prepend_strf(spath* path, const char* format, ...);

/** adds new components to end of path using printf-like formatting */
int spath_append_strf(spath* path, const char* format, ...);
///@}

/********************************************************/
/** \name cut, slice, and subpath functions */
///@{

/** keeps upto length components of path starting at specified location
 * and discards the rest, offset can be negative to count
 * from back, a negative length copies the remainder of the string */
int spath_slice(spath* path, int offset, int length);

/** drops last component from path */
int spath_dirname(spath* path);

/** only leaves last component of path */
int spath_basename(spath* path);

/** copies upto length components of path starting at specified location
 * and returns subpath as new path, offset can be negative to count
 * from back, a negative length copies the remainder of the string */
spath* spath_sub(spath* path, int offset, int length);

/** chops path at specified location and returns remainder as new path,
 * offset can be negative to count from back of path */
spath* spath_cut(spath* path, int offset);
///@}

/********************************************************/
/** \name simplify and resolve functions */
///@{

/** allocate a new path initialized with current working dir */
spath* spath_cwd(void);

/** apply realpath to given path */
int spath_realpath(spath* path);

/** removes consecutive '/', '.', '..', and trailing '/' */
int spath_reduce(spath* path);

/** creates path from string, calls reduce, calls path_strdup,
 * and deletes path, caller must free returned string with free */
char* spath_strdup_reduce_str(const char* str);

/** return 1 if path starts with an empty string, 0 otherwise */
int spath_is_absolute(const spath* path);

/** return 1 if child is contained in tree starting at parent, 0 otherwise */
int spath_is_child(const spath* parent, const spath* child);

/** compute and return relative path from src to dst */
spath* spath_relative(const spath* src, const spath* dst);
///@}

/* enable C++ codes to include this header directly */
#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* SPATH_H */
