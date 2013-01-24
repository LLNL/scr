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

#ifndef SCR_PATH_H
#define SCR_PATH_H

#include <stdarg.h>
#include <sys/types.h>

/* TODO: for formatted strings, use special %| character (or something
 * similar) to denote directories in portable way */

/* Stores path as a linked list, breaking path at each directory marker
 * and terminating NUL.  Can append and insert paths or cut and slice
 * them.  Can initialize a path from a string and extract a path into
 * a string.  Path consists of a number of components indexed from 0.
 *
 * Examples:
 * * root directory "/" consists of a path with two components both of
 *   which are empty strings */

/*
=========================================
This file defines the data structure for a path,
which is an double-linked list of elements,
where each element contains a component (char string).
=========================================
*/

/*
=========================================
Define hash and element structures
=========================================
*/

struct scr_path_elem_struct;

/* define the structure for a path element */
typedef struct scr_path_elem_struct {
  char* component; /* pointer to strdup'd component string */
  size_t chars;    /* number of chars in component */
  struct scr_path_elem_struct* next; /* pointer to next element */
  struct scr_path_elem_struct* prev; /* pointer to previous element */
} scr_path_elem;

/* define the structure for a path object */
typedef struct {
  int components;      /* number of components in path */
  size_t chars;        /* number of chars in path */
  scr_path_elem* head; /* pointer to first element */
  scr_path_elem* tail; /* pointer to last element */
} scr_path;

/*
=========================================
Allocate and delete path objects
=========================================
*/

/* allocates a new path */
scr_path* scr_path_new();

/* allocates a path from string */
scr_path* scr_path_from_str(const char* str);

/* allocates a path from formatted string */
scr_path* scr_path_from_strf(const char* format, ...);

/* allocates and returns a copy of path */
scr_path* scr_path_dup(const scr_path* path);

/* frees a path and sets path pointer to NULL */
int scr_path_delete(scr_path** ptr_path);

/*
=========================================
get size and string functions
=========================================
*/

/* returns 1 if path has 0 components, 0 otherwise */
int scr_path_is_null(const scr_path* path);

/* return number of components in path */
int scr_path_components(const scr_path* path);

/* return number of characters needed to store path
 * (excludes terminating NUL) */
size_t scr_path_strlen(const scr_path* path);

/* copy string into user buffer, abort if buffer is too small,
 * return number of bytes written */
size_t scr_path_strcpy(char* buf, size_t n, const scr_path* path);

/* allocate memory and return path in string form,
 * caller is responsible for freeing string with scr_free() */
char* scr_path_strdup(const scr_path* path);

/*
=========================================
insert, append, prepend functions
=========================================
*/

/* inserts path2 so head element in path2 starts at specified offset
 * in path1, e.g.,
 *   0   - before first element of path1
 *   N-1 - before last element of path1
 *   N   - after last element of path1 */
int scr_path_insert(scr_path* path1, int offset, const scr_path* ptr_path2);

/* prepends path2 to path1 */
int scr_path_prepend(scr_path* path1, const scr_path* ptr_path2);

/* appends path2 to path1 */
int scr_path_append(scr_path* path1, const scr_path* ptr_path2);

/* inserts components in string so first component in string starts
 * at specified offset in path, e.g.,
 *   0   - before first element of path
 *   N-1 - before last element of path
 *   N   - after last element of path */
int scr_path_insert_str(scr_path* path, int offset, const char* str);

/* prepends components in string to path */
int scr_path_prepend_str(scr_path* path, const char* str);

/* appends components in string to path */
int scr_path_append_str(scr_path* path, const char* str);

/* inserts components in string so first component in string starts
 * at specified offset in path, e.g.,
 *   0   - before first element of path
 *   N-1 - before last element of path
 *   N   - after last element of path */
int scr_path_insert_strf(scr_path* path, int offset, const char* format, ...);

/* prepends components in string to path */
int scr_path_prepend_strf(scr_path* path, const char* format, ...);

/* adds new components to end of path using printf-like formatting */
int scr_path_append_strf(scr_path* path, const char* format, ...);

/*
=========================================
cut, slice, and subpath functions
=========================================
*/

/* keeps upto length components of path starting at specified location
 * and discards the rest, offset can be negative to count
 * from back, a negative length copies the remainder of the string */
int scr_path_slice(scr_path* path, int offset, int length);

/* drops last component from path */
int scr_path_dirname(scr_path* path);

/* only leaves last component of path */
int scr_path_basename(scr_path* path);

/* copies upto length components of path starting at specified location
 * and returns subpath as new path, offset can be negative to count
 * from back, a negative length copies the remainder of the string */
scr_path* scr_path_sub(scr_path* path, int offset, int length);

/* chops path at specified location and returns remainder as new path,
 * offset can be negative to count from back of path */
scr_path* scr_path_cut(scr_path* path, int offset);

/*
=========================================
simplify and resolve functions
=========================================
*/

/* removes consecutive '/', '.', '..', and trailing '/' */
int scr_path_reduce(scr_path* path);

/* return 1 if path starts with an empty string, 0 otherwise */
int scr_path_is_absolute(const scr_path* path);

/* return 1 if child is contained in tree starting at parent, 0 otherwise */
int scr_path_is_child(const scr_path* parent, const scr_path* child);

/* compute and return relative path from src to dst */
scr_path* scr_path_relative(const scr_path* src, const scr_path* dst);

#endif /* SCR_PATH_H */
