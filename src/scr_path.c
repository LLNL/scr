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

/* Defines a double linked list representing a file path. */

#include "scr.h"
#include "scr_err.h"
#include "scr_path.h"
#include "scr_util.h"

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>

#include <stdint.h>

/*
=========================================
Private functions
=========================================
*/

static inline int scr_path_elem_init(scr_path_elem* elem)
{
  elem->component = NULL;
  elem->chars     = 0;
  elem->next      = NULL;
  elem->prev      = NULL;
  return SCR_SUCCESS;
}

static inline int scr_path_init(scr_path* path)
{
  path->components = 0;
  path->chars      = 0;
  path->head       = NULL;
  path->tail       = NULL;
  return SCR_SUCCESS;
}

/* allocate and initialize a new path element */
static scr_path_elem* scr_path_elem_alloc()
{
  scr_path_elem* elem = (scr_path_elem*) malloc(sizeof(scr_path_elem));
  if (elem != NULL) {
    scr_path_elem_init(elem);
  } else {
    scr_abort(-1, "Failed to allocate memory for path element @ %s:%d",
      __FILE__, __LINE__
    );
  }
  return elem;
}

/* free a path element */
static int scr_path_elem_free(scr_path_elem** ptr_elem)
{
  if (ptr_elem != NULL) {
    /* got an address to the pointer of an element,
     * dereference to get pointer to elem */
    scr_path_elem* elem = *ptr_elem;
    if (elem != NULL) {
      /* free the component which was strdup'ed */
      scr_free(&(elem->component));
    } 
  }

  /* free the element structure itself */
  scr_free(ptr_elem);

  return SCR_SUCCESS;
}

/* allocate a new path */
static scr_path* scr_path_alloc()
{
  scr_path* path = (scr_path*) malloc(sizeof(scr_path));
  if (path != NULL) {
    scr_path_init(path);
  } else {
    scr_abort(-1, "Failed to allocate memory for path object @ %s:%d",
      __FILE__, __LINE__
    );
  }
  return path;
}

/* allocate and return a duplicate of specified elememnt,
 * only copies value not next and previoud pointers */
static scr_path_elem* scr_path_elem_dup(const scr_path_elem* elem)
{
  /* check that element is not NULL */
  if (elem == NULL) {
    return NULL;
  }

  /* allocate new element */
  scr_path_elem* dup_elem = scr_path_elem_alloc();
  if (dup_elem == NULL) {
    scr_abort(-1, "Failed to allocate memory for path element @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* set component and chars fields (next and prev will be set later) */
  dup_elem->component = strdup(elem->component);
  dup_elem->chars     = elem->chars;

  return dup_elem;
}

/* return element at specified offset in path
 *   0   - points to first element
 *   N-1 - points to last element */
static scr_path_elem* scr_path_elem_index(const scr_path* path, int index)
{
  /* check that we got a path */
  if (path == NULL) {
    scr_abort(-1, "Assert that path are not NULL @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* check that index is in range */
  if (index < 0 || index >= path->components) {
    scr_abort(-1, "Offset %d is out of range [0,%d) @ %s:%d",
      index, path->components, __FILE__, __LINE__
    );
  }

  /* scan until we find element at specified index */
  scr_path_elem* current = NULL;
  if (path->components > 0) {
    int i;
    int from_head = index;
    int from_tail = path->components - index - 1;
    if (from_head <= from_tail) {
      /* shorter to start at head and go forwards */
      current = path->head;
      for (i = 0; i < from_head; i++) {
        current = current->next;
      }
    } else {
      /* shorter to start at tail and go backwards */
      current = path->tail;
      for (i = 0; i < from_tail; i++) {
        current = current->prev;
      }
    }
  }

  return current;
}

/* insert element at specified offset in path
 *   0   - before first element
 *   N-1 - before last element
 *   N   - after last element */
static int scr_path_elem_insert(scr_path* path, int offset, scr_path_elem* elem)
{
  /* check that we got a path and element */
  if (path == NULL || elem == NULL) {
    scr_abort(-1, "Assert that path and elem are not NULL @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* check that offset is in range */
  if (offset < 0 || offset > path->components) {
    scr_abort(-1, "Offset %d is out of range @ %s:%d",
      offset, path->components, __FILE__, __LINE__
    );
  }

  /* if offset equals number of components, insert after last element */
  if (offset == path->components) {
    /* attach to path */
    path->components++;
    path->chars += elem->chars;

    /* get pointer to tail element and point to element as new tail */
    scr_path_elem* tail = path->tail;
    path->tail = elem;

    /* tie element to tail */
    elem->prev = tail;
    elem->next = NULL;

    /* fix up old tail element */
    if (tail != NULL) {
      /* tie last element to new element */
      tail->next = elem;
    } else {
      /* if tail is NULL, this is the only element in path, so set head */
      path->head = elem;
    }

    return SCR_SUCCESS;
  }

  /* otherwise, insert element before current element */

  /* lookup element at specified offset */
  scr_path_elem* current = scr_path_elem_index(path, offset);

  /* attach to path */
  path->components++;
  path->chars += elem->chars;

  /* insert element before current */
  if (current != NULL) {
    /* get pointer to element before current */
    scr_path_elem* prev = current->prev;
    elem->prev = prev;
    elem->next = current;
    if (prev != NULL) {
      /* tie previous element to new element */
      prev->next = elem;
    } else {
      /* if prev is NULL, this element is the new head of the path */
      path->head = elem;
    }
    current->prev = elem;
  } else {
    /* if current is NULL, this is the only element in the path */
    path->head = elem;
    path->tail = elem;
    elem->prev = NULL;
    elem->next = NULL;
  }

  return SCR_SUCCESS;
}

/* extract specified element from path */
static int scr_path_elem_extract(scr_path* path, scr_path_elem* elem)
{
  /* check that we got a path and element */
  if (path == NULL || elem == NULL) {
    /* nothing to do in this case */
    scr_abort(-1, "Assert that path and elem are not NULL @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* TODO: would be nice to verify that elem is part of path */

  /* subtract component and number of chars from path */
  path->components--;
  path->chars -= elem->chars;

  /* lookup address of elements of next and previous items */
  scr_path_elem* prev = elem->prev;
  scr_path_elem* next = elem->next;

  /* fix up element that comes before */
  if (prev != NULL) {
    /* there's an item before this one, tie it to next item */
    prev->next = next;
  } else {
    /* we're the first item, point head to next item */
    path->head = next;
  }

  /* fix up element that comes after */
  if (next != NULL) {
    /* there's an item after this one, tie it to previous item */
    next->prev = prev;
  } else {
    /* we're the last item, point tail to previous item */
    path->tail = prev;
  }

  return SCR_SUCCESS;
}

/* allocates and returns a string filled in with formatted text,
 * assumes that caller has called va_start before and will call va_end
 * after */
static char* scr_path_alloc_strf(const char* format, va_list args1, va_list args2)
{
  /* get length of component string */
  size_t chars = (size_t) vsnprintf(NULL, 0, format, args1);

  /* allocate space to hold string, add one for the terminating NUL */
  size_t strlen = chars + 1;
  char* str = (char*) malloc(strlen);
  if (str == NULL) {
    scr_abort(-1, "Failed to allocate memory for path component string @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* copy formatted string into new memory */
  vsnprintf(str, strlen, format, args2);

  /* return string */
  return str;
}

/*
=========================================
Allocate and delete path objects
=========================================
*/

/* allocate a new path */
scr_path* scr_path_new()
{
  scr_path* path = scr_path_alloc();
  if (path == NULL) {
    scr_abort(-1, "Failed to allocate memory for path object @ %s:%d",
      __FILE__, __LINE__
    );
  }
  return path;
}

/* allocates a path from string */
scr_path* scr_path_from_str(const char* str)
{
  /* allocate a path object */
  scr_path* path = scr_path_alloc();
  if (path == NULL) {
    scr_abort(-1, "Failed to allocate memory for path object @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* check that str is not NULL */
  if (str != NULL) {
    /* iterate through components of string */
    const char* start = str;
    const char* end   = str;
    while (1) {
      /* scan end until we stop on a '/' or '\0' character */
      while (*end != '/' && *end != '\0') {
        end++;
      }

      /* compute number of bytes to copy this component
       * (including terminating NULL) */
      size_t buflen = end - start + 1;
      char* buf = (char*) malloc(buflen);
      if (buf == NULL) {
        scr_abort(-1, "Failed to allocate memory for component string @ %s:%d",
          __FILE__, __LINE__
        );
      }

      /* copy characters into string buffer and add terminating NUL */
      size_t chars = buflen - 1;
      if (chars > 0) {
        strncpy(buf, start, chars);
      }
      buf[chars] = '\0';

      /* allocate new element */
      scr_path_elem* elem = scr_path_elem_alloc();
      if (elem == NULL) {
        scr_abort(-1, "Failed to allocate memory for path component @ %s:%d",
          __FILE__, __LINE__
        );
      }

      /* record string in element */
      elem->component = buf;
      elem->chars     = chars;

      /* add element to path */
      scr_path_elem_insert(path, path->components, elem);

      if (*end != '\0') {
        /* advance to next character */
        end++;
        start = end;
      } else {
        /* stop, we've hit the end of the input string */
        break;
      }
    }
  }

  return path;
}

/* allocates a path from formatted string */
scr_path* scr_path_from_strf(const char* format, ...)
{
  /* allocate formatted string */
  va_list args1, args2;
  va_start(args1, format);
  va_start(args2, format);
  char* str = scr_path_alloc_strf(format, args1, args2);
  va_end(args2);
  va_end(args1);
  if (str == NULL) {
    scr_abort(-1, "Failed to allocate memory for path component string @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* create path from string */
  scr_path* path = scr_path_from_str(str);

  /* free the string */
  scr_free(&str);

  return path;
}

/* duplicate a path */
scr_path* scr_path_dup(const scr_path* path)
{
  /* easy if path is NULL */
  if (path == NULL) {
    return NULL;
  }

  /* allocate a new path */
  scr_path* dup_path = scr_path_new();
  if (dup_path == NULL) {
    scr_abort(-1, "Failed to allocate path object @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* get pointer to first element and delete elements in list */
  scr_path_elem* current = path->head;
  while (current != NULL) {
    /* get pointer to element after current, delete current,
     * and set current to next */
    scr_path_elem* dup_elem = scr_path_elem_dup(current);
    if (dup_elem == NULL) {
      scr_abort(-1, "Failed to allocate path element object @ %s:%d",
        __FILE__, __LINE__
      );
    }

    /* insert new element at end of path */
    scr_path_elem_insert(dup_path, dup_path->components, dup_elem);

    /* advance to next element */
    current = current->next;
  }

  return dup_path;
}

/* free a path */
int scr_path_delete(scr_path** ptr_path)
{
  if (ptr_path != NULL) {
    /* got an address to the pointer of a path object,
     * dereference to get pointer to path */
    scr_path* path = *ptr_path;
    if (path != NULL) {
      /* get pointer to first element and delete elements in list */
      scr_path_elem* current = path->head;
      while (current != NULL) {
        /* get pointer to element after current, delete current,
         * and set current to next */
        scr_path_elem* next = current->next;
        scr_path_elem_free(&current);
        current = next;
      }
    }
  }

  /* free the path object itself */
  scr_free(ptr_path);

  return SCR_SUCCESS;
}

/*
=========================================
get size and string functions
=========================================
*/

/* returns 1 if path has 0 components, 0 otherwise */
int scr_path_is_null(const scr_path* path)
{
  if (path != NULL) {
    int components = path->components;
    if (components > 0) {
      return 0;
    }
  }
  return 1;
}

/* return number of components in path */
int scr_path_components(const scr_path* path)
{
  if (path != NULL) {
    int components = path->components;
    return components;
  }
  return 0;
}

/* return number of characters needed to store path
 * (not including terminating NUL) */
size_t scr_path_strlen(const scr_path* path)
{
  if (path != NULL) {
    /* need a '/' between components so include this in our count */
    int components = path->components;
    if (components > 0) {
      size_t slashes = (size_t) (components - 1);
      size_t chars   = path->chars;
      size_t strlen  = slashes + chars;
      return strlen;
    }
  }
  return 0;
}

/* copies path into buf, caller must ensure buf is large enough */
static int scr_path_strcpy_internal(char* buf, const scr_path* path)
{
  /* copy contents into string buffer */
  char* ptr = buf;
  scr_path_elem* current = path->head;
  while (current != NULL) {
    /* copy component to buffer */
    char* component = current->component;
    size_t chars    = current->chars;
    memcpy((void*)ptr, (void*)component, chars);
    ptr += chars;

    /* if there is another component, add a slash */
    scr_path_elem* next = current->next;
    if (next != NULL) {
      *ptr = '/';
      ptr++;
    }

    /* move to next component */
    current = next;
  }

  /* terminate the string */
  *ptr = '\0';

  return SCR_SUCCESS;
}

/* copy string into user buffer, abort if buffer is too small */
size_t scr_path_strcpy(char* buf, size_t n, const scr_path* path)
{
  /* check that we have a pointer to a path */
  if (path == NULL) {
    scr_abort(-1, "Cannot copy NULL pointer to string @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* we can't copy a NULL path */
  if (scr_path_is_null(path)) {
    scr_abort(-1, "Cannot copy a NULL path to string @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* get length of path */
  size_t strlen = scr_path_strlen(path) + 1;

  /* if user buffer is too small, abort */
  if (n < strlen) {
    scr_abort(-1, "User buffer of %d bytes is too small to hold string of %d bytes @ %s:%d",
      n, strlen, __FILE__, __LINE__
    );
  }

  /* copy contents into string buffer */
  scr_path_strcpy_internal(buf, path);

  /* return number of bytes we copied to buffer */
  return strlen;
}

/* allocate memory and return path in string form */
char* scr_path_strdup(const scr_path* path)
{
  /* if we have no pointer to a path object return NULL */
  if (path == NULL) {
    return NULL;
  }

  /* if we have no components return NULL */
  if (path->components <= 0) {
    return NULL;
  }

  /* compute number of bytes we need to allocate and allocate string */
  size_t buflen = scr_path_strlen(path) + 1;
  char* buf = (char*) malloc(buflen);
  if (buf == NULL) {
    scr_abort(-1, "Failed to allocate buffer for path @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* copy contents into string buffer */
  scr_path_strcpy_internal(buf, path);

  /* return new string to caller */
  return buf;
}

/*
=========================================
insert, append, prepend functions
=========================================
*/

/* integrates path2 so head element in path2 starts at specified offset
 * in path1 and deletes path2, e.g.,
 *   0   - before first element
 *   N-1 - before last element
 *   N   - after last element */
static int scr_path_combine(scr_path* path1, int offset, scr_path** ptr_path2)
{
  if (path1 != NULL) {
    /* check that offset is in range */
    int components = path1->components;
    if (offset < 0 || offset > components) {
      scr_abort(-1, "Offset %d is out of range [0,%d] @ %s:%d",
        offset, components, __FILE__, __LINE__
      );
    }

    if (ptr_path2 != NULL) {
      /* got an address to the pointer of a path object,
       * dereference to get pointer to path */
      scr_path* path2 = *ptr_path2;
      if (path2 != NULL) {
        /* get pointer to head and tail of path2 */
        scr_path_elem* head2 = path2->head;
        scr_path_elem* tail2 = path2->tail;

        /* if offset equals number of components, insert after last element,
         * otherwise, insert element before specified element */
        if (offset == components) {
          /* get pointer to tail of path1 */
          scr_path_elem* tail1 = path1->tail;
          if (tail1 != NULL) {
            /* join tail of path1 to head of path2 */
            tail1->next = head2;
          } else {
            /* path1 has no items, set head to head of path2 */
            path1->head = head2;
          }

          /* if path2 has a head element, tie it to tail of path1 */
          if (head2 != NULL) {
            head2->prev = tail1;
          }

          /* set new tail of path1 */
          path1->tail = tail2;
        } else {
            /* lookup element at specified offset */
            scr_path_elem* current = scr_path_elem_index(path1, offset);

            /* get pointer to element before current */
            scr_path_elem* prev = current->prev;

            /* tie previous element to head of path2 */
            if (prev != NULL) {
              /* tie previous element to new element */
              prev->next = head2;
            } else {
              /* if prev is NULL, head of path2 will be new head of path1 */
              path1->head = head2;
            }

            /* tie current to tail of path2 */
            current->prev = tail2;

            /* tie head of path2 to previous */
            if (head2 != NULL) {
              head2->prev = prev;
            }

            /* tie tail of path2 to current */
            if (tail2 != NULL) {
              tail2->next = current;
            }
        }

        /* add component and character counts to first path */
        path1->components += path2->components;
        path1->chars      += path2->chars;
      }
    }

    /* free the path2 struct */
    scr_free(ptr_path2);
  } else {
    scr_abort(-1, "Cannot attach a path to a NULL path @ %s:%d",
      __FILE__, __LINE__
    );
  }

  return SCR_SUCCESS;
}

/* inserts path2 so head element in path2 starts at specified offset
 * in path1, e.g.,
 *   0   - before first element of path1
 *   N-1 - before last element of path1
 *   N   - after last element of path1 */
int scr_path_insert(scr_path* path1, int offset, const scr_path* path2)
{
  int rc = SCR_SUCCESS;
  if (path1 != NULL) {
    /* make a copy of path2, and combint at specified offset in path1,
     * combine deletes copy of path2 */
    scr_path* path2_copy = scr_path_dup(path2);
    rc = scr_path_combine(path1, offset, &path2_copy);
  } else {
    scr_abort(-1, "Cannot attach a path to a NULL path @ %s:%d",
      __FILE__, __LINE__
    );
  }
  return rc;
}

/* prepends path2 to path1 */
int scr_path_prepend(scr_path* path1, const scr_path* path2)
{
  int rc = scr_path_insert(path1, 0, path2);
  return rc;
}

/* appends path2 to path1 */
int scr_path_append(scr_path* path1, const scr_path* path2)
{
  int rc = SCR_SUCCESS;
  if (path1 != NULL) {
    rc = scr_path_insert(path1, path1->components, path2);
  } else {
    scr_abort(-1, "Cannot attach a path to a NULL path @ %s:%d",
      __FILE__, __LINE__
    );
  }
  return rc;
}

/* inserts components in string so first component in string starts
 * at specified offset in path, e.g.,
 *   0   - before first element of path
 *   N-1 - before last element of path
 *   N   - after last element of path */
int scr_path_insert_str(scr_path* path, int offset, const char* str)
{
  /* verify that we got a path as input */
  if (path == NULL) {
    scr_abort(-1, "Cannot insert string to a NULL path @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* create a path from this string */
  scr_path* newpath = scr_path_from_str(str);
  if (newpath == NULL) {
    scr_abort(-1, "Failed to allocate path for insertion @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* attach newpath to original path */
  int rc = scr_path_combine(path, offset, &newpath);
  return rc;
}

/* prepends components in string to path */
int scr_path_prepend_str(scr_path* path, const char* str)
{
  int rc = scr_path_insert_str(path, 0, str);
  return rc;
}

/* appends components in string to path */
int scr_path_append_str(scr_path* path, const char* str)
{
  int rc = SCR_SUCCESS;
  if (path != NULL) {
    rc = scr_path_insert_str(path, path->components, str);
  } else {
    scr_abort(-1, "Cannot attach string to a NULL path @ %s:%d",
      __FILE__, __LINE__
    );
  }
  return rc;
}

/* inserts components in string so first component in string starts
 * at specified offset in path, e.g.,
 *   0   - before first element of path
 *   N-1 - before last element of path
 *   N   - after last element of path */
int scr_path_insert_strf(scr_path* path, int offset, const char* format, ...)
{
  /* verify that we got a path as input */
  if (path == NULL) {
    scr_abort(-1, "Cannot append string to a NULL path @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* allocate formatted string */
  va_list args1, args2;
  va_start(args1, format);
  va_start(args2, format);
  char* str = scr_path_alloc_strf(format, args1, args2);
  va_end(args2);
  va_end(args1);
  if (str == NULL) {
    scr_abort(-1, "Failed to allocate memory for path component string @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* attach str to path */
  int rc = scr_path_insert_str(path, offset, str);

  /* free the string */
  scr_free(&str);

  return rc;
}

/* prepends components in string to path */
int scr_path_prepend_strf(scr_path* path, const char* format, ...)
{
  /* verify that we got a path as input */
  if (path == NULL) {
    scr_abort(-1, "Cannot append string to a NULL path @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* allocate formatted string */
  va_list args1, args2;
  va_start(args1, format);
  va_start(args2, format);
  char* str = scr_path_alloc_strf(format, args1, args2);
  va_end(args2);
  va_end(args1);
  if (str == NULL) {
    scr_abort(-1, "Failed to allocate memory for path component string @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* attach str to path */
  int rc = scr_path_insert_str(path, 0, str);

  /* free the string */
  scr_free(&str);

  return rc;
}

/* adds a new component to end of path using printf-like formatting */
int scr_path_append_strf(scr_path* path, const char* format, ...)
{
  /* verify that we got a path as input */
  if (path == NULL) {
    scr_abort(-1, "Cannot append string to a NULL path @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* allocate formatted string */
  va_list args1, args2;
  va_start(args1, format);
  va_start(args2, format);
  char* str = scr_path_alloc_strf(format, args1, args2);
  va_end(args2);
  va_end(args1);
  if (str == NULL) {
    scr_abort(-1, "Failed to allocate memory for path component string @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* attach str to path */
  int rc = scr_path_insert_str(path, path->components, str);

  /* free the string */
  scr_free(&str);

  return rc;
}

/*
=========================================
cut, slice, and subpath functions
=========================================
*/

/* keeps upto length components of path starting at specified location
 * and discards the rest, offset can be negative to count
 * from back, a negative length copies the remainder of the string */
int scr_path_slice(scr_path* path, int offset, int length)
{
  /* check that we have a path */
  if (path == NULL) {
    return SCR_SUCCESS;
  }

  /* force offset into range */
  int components = path->components;
  if (components > 0) {
    while (offset < 0) {
      offset += components;
    }
    while (offset >= components) {
      offset -= components;
    }
  } else {
    /* nothing left to slice */
    return SCR_SUCCESS;
  }

  /* lookup first element to be head of new path */
  scr_path_elem* current = scr_path_elem_index(path, offset);

  /* delete any items before this one */
  scr_path_elem* elem = current->prev;
  while (elem != NULL) {
    scr_path_elem* prev = elem->prev;
    scr_path_elem_free(&elem);
    elem = prev;
  }

  /* remember our starting element and intialize tail to NULL */
  scr_path_elem* head = current;
  scr_path_elem* tail = NULL;

  /* step through length elements or to the end of the list,
   * a negative length means we step until end of list */
  components = 0;
  size_t chars = 0;
  while ((length < 0 || length > 0) && current != NULL) {
    /* count number of components and characters in list and
     * update tail */
    components++;
    chars += current->chars;
    tail = current;

    /* advance to next element */
    current = current->next;
    if (length > 0) {
      length--;
    }
  }

  /* current now points to first element to be cut,
   * delete it and all trailing items */
  while (current != NULL) {
    scr_path_elem* next = current->next;
    scr_path_elem_free(&current);
    current = next;
  }

  /* set new path members */
  path->components = components;
  path->chars      = chars;
  if (components > 0) {
    /* we have some components, update head and tail, terminate the list */
    path->head = head;
    path->tail = tail;
    head->prev = NULL;
    tail->next = NULL;
  } else {
    /* otherwise, we have no items in the path */
    path->head = NULL;
    path->tail = NULL;
  }

  return SCR_SUCCESS;
}

/* drops last component from path */
int scr_path_dirname(scr_path* path)
{
  int components = scr_path_components(path);
  if (components > 0) {
    int rc = scr_path_slice(path, 0, components-1);
    return rc;
  }
  return SCR_SUCCESS;
}

/* only leaves last component of path */
int scr_path_basename(scr_path* path)
{
  int rc = scr_path_slice(path, -1, 1);
  return rc;
}

/* copies upto length components of path starting at specified location
 * and returns subpath as new path, offset can be negative to count
 * from back, a negative length copies the remainder of the string */
scr_path* scr_path_sub(scr_path* path, int offset, int length)
{
  /* check that we have a path */
  if (path == NULL) {
    return NULL;
  }

  /* force offset into range */
  int components = path->components;
  if (components > 0) {
    while (offset < 0) {
      offset += components;
    }
    while (offset >= components) {
      offset -= components;
    }
  } else {
    /* in this case, unless length == 0, we'll fail check below,
     * and if length == 0, we'll return an empty path */
    offset = 0;
  }

  /* allocate and initialize an empty path object */
  scr_path* newpath = scr_path_alloc();
  if (newpath == NULL) {
    scr_abort(-1, "Failed to allocate memory for path object @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* return the empty path if source path is empty */
  if (components == 0) {
    return newpath;
  }

  /* lookup first element to be head of new path */
  scr_path_elem* current = scr_path_elem_index(path, offset);

  /* copy elements from path and attach to newpath */
  while ((length < 0 || length > 0) && current != NULL) {
    /* duplicate element */
    scr_path_elem* elem = scr_path_elem_dup(current);
    if (elem == NULL) {
      scr_abort(-1, "Failed to duplicate element of path object @ %s:%d",
        __FILE__, __LINE__
      );
    }

    /* insert element into newpath */
    scr_path_elem_insert(newpath, newpath->components, elem);

    /* advance to next element */
    current = current->next;
    if (length > 0) {
      length--;
    }
  }

  /* return our newly constructed path */
  return newpath;
}

/* chops path at specified location and returns remainder as new path,
 * offset can be negative to count from back */
scr_path* scr_path_cut(scr_path* path, int offset)
{
  /* check that we have a path */
  if (path == NULL) {
    return NULL;
  }

  /* allocate and initialize an empty path object */
  scr_path* newpath = scr_path_alloc();
  if (newpath == NULL) {
    scr_abort(-1, "Failed to allocate memory for path object @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* if path is empty, return an empty path */
  int components = path->components;
  if (components == 0) {
    return newpath;
  }

  /* force offset into range */
  while (offset < 0) {
    offset += components;
  }
  while (offset >= components) {
    offset -= components;
  }

  /* lookup first element to be head of new path */
  scr_path_elem* current = scr_path_elem_index(path, offset);

  /* set head and tail of newpath2 */
  newpath->head = current;
  newpath->tail = path->tail;

  /* set tail (and head) of path */
  if (current != NULL) {
    /* get element before current to be new tail */
    scr_path_elem* prev = current->prev;

    /* cut current from previous element */
    current->prev = NULL;

    if (prev != NULL) {
      /* cut previous element from current */
      prev->next = NULL;
    } else {
      /* if there is no element before current,
       * we cut the first element, so update head */
      path->head = NULL;
    }

    /* set previous element as new tail for path */
    path->tail = prev;
  } else {
    /* current is NULL, meaning path is empty */
    path->head = NULL;
    path->tail = NULL;
  }

  /* finally, cycle through newpath, subtract counts from path
   * and add to newpath */
  while (current != NULL) {
    /* subtract counts from path */
    path->components--;
    path->chars -= current->chars;

    /* add counts to newpath */
    newpath->components++;
    newpath->chars += current->chars;

    /* advance to next element */
    current = current->next;
  }
  
  /* return our newly constructed path */
  return newpath;
}

/*
=========================================
simplify and resolve functions
=========================================
*/

/* removes consecutive '/', '.', '..', and trailing '/' */
int scr_path_reduce(scr_path* path)
{
  /* check that we got a path */
  if (path == NULL) {
    /* nothing to do in this case */
    return SCR_SUCCESS;
  }


  /* now iterate through and remove any "." and empty strings,
   * we go from back to front to handle paths like "./" */
  scr_path_elem* current = path->tail;
  while (current != NULL) {
    /* get pointer to previous element */
    scr_path_elem* prev = current->prev;
 
    /* check whether component string matches "." or "" */
    char* component = current->component;
    if (strcmp(component, ".") == 0) {
      /* pull element out of path and delete it */
      scr_path_elem_extract(path, current);
      scr_path_elem_free(&current);
    } else if (strcmp(component, "") == 0 && current != path->head) {
      /* head is allowed to be empty string so that we don't chop leading '/' */
      /* pull element out of path and delete it */
      scr_path_elem_extract(path, current);
      scr_path_elem_free(&current);
    }

    /* advance to previous item */
    current = prev;
  }

  /* now remove any ".." and any preceding component */
  current = path->head;
  while (current != NULL) {
    /* get pointer to previous and next elements */
    scr_path_elem* prev = current->prev;
    scr_path_elem* next = current->next;
 
    /* check whether component string matches ".." */
    char* component = current->component;
    if (strcmp(component, "..") == 0) {
      /* pull current and previous elements out of path and delete them */
      if (prev != NULL) {
        /* check that previous is not "..", since we go front to back,
         * previous ".." shouldn't exist unless it couldn't be popped */
        char* prev_component = prev->component;
        if (strcmp(prev_component, "..") != 0) {
          /* check that item is not empty, only empty strings left
           * should be one at very beginning of string */
          if (strcmp(prev_component, "") != 0) {
            /* delete previous element */
            scr_path_elem_extract(path, prev);
            scr_path_elem_free(&prev);

            /* delete current element */
            scr_path_elem_extract(path, current);
            scr_path_elem_free(&current);
          } else {
            scr_abort(-1, "Cannot pop past root directory @ %s:%d",
              __FILE__, __LINE__
            );
          }
        } else {
          /* previous is also "..", so keep going */
        }
      } else {
        /* we got some path like "../foo", leave it in this form */
      }
    }

    /* advance to next item */
    current = next;
  }

  return SCR_SUCCESS;
}

/* return 1 if path starts with an empty string, 0 otherwise */
int scr_path_is_absolute(const scr_path* path)
{
  if (path != NULL) {
    if (path->components > 0) {
      const scr_path_elem* head = path->head;
      const char* component = head->component; 
      if (strcmp(component, "") == 0) {
        return 1;
      }
    }
  }
  return 0;
}

/* return 1 if child is contained in tree starting at parent, 0 otherwise */
int scr_path_is_child(const scr_path* parent, const scr_path* child)
{
  /* check that we got pointers to both parent and child paths */
  if (parent == NULL || child == NULL) {
    return 0;
  }

  /* check that parent and child aren't NULL paths */
  if (scr_path_is_null(parent)) {
    return 0;
  }
  if (scr_path_is_null(child)) {
    return 0;
  }

  /* TODO: check that paths are absolute */

  /* TODO: reduce paths? */

  /* get pointers to start of parent and child */
  int equal = 1;
  scr_path_elem* parent_elem = parent->head;
  scr_path_elem* child_elem  = child->head;
  while (parent_elem != NULL && child_elem != NULL) {
    /* compare strings for this element */
    const char* parent_component = parent_elem->component;
    const char* child_component  = child_elem->component;
    if (strcmp(parent_component, child_component) != 0) {
      /* found a component in child that's not in parent */
      equal = 0;
      break;
    }

    /* advance to compare next element */
    parent_elem = parent_elem->next;
    child_elem  = child_elem->next;
  }

  /* if everything is equal and we've run out of parent components
   * but not child components, assume child path is under parent path */
  if (equal && parent_elem == NULL && child_elem != NULL) {
    return 1;
  }

  return 0;
}

/* compute and return relative path from src to dst */
scr_path* scr_path_relative(const scr_path* src, const scr_path* dst)
{
  /* check that we don't have NULL pointers */
  if (src == NULL || dst == NULL) {
    scr_abort(-1, "Either src or dst pointer is NULL @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* we can't get to a NULL path from a non-NULL path */
  int src_components = src->components;
  int dst_components = dst->components;
  if (src_components > 0 && dst_components == 0) {
    scr_abort(-1, "Cannot get from non-NULL path to NULL path @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* allocate a new path to record relative path */
  scr_path* rel = scr_path_new();
  if (rel == NULL) {
    scr_abort(-1, "Failed to allocate memory for relative path @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* walk down both paths until we find the first location where they
   * differ */
  const scr_path_elem* src_elem = src->head;
  const scr_path_elem* dst_elem = dst->head;
  while (1) {
    /* check that we have valid src and dst elements */
    if (src_elem == NULL) {
      break;
    }
    if (dst_elem == NULL) {
      break;
    }

    /* check that the current component is the same */
    const char* src_component = src_elem->component;
    const char* dst_component = dst_elem->component;
    if (strcmp(src_component, dst_component) != 0) {
      break;
    }

    /* go to next component */
    src_elem = src_elem->next;
    dst_elem = dst_elem->next;
  }

  /* if there is anything left in source, we need to pop back */
  while (src_elem != NULL) {
    /* pop back one level, and go to next element */
    scr_path_append_str(rel, "..");
    src_elem = src_elem->next;
  }

  /* now tack on any items left from dst */
  while (dst_elem != NULL) {
    const char* dst_component = dst_elem->component;
    scr_path_append_str(rel, dst_component);
    dst_elem = dst_elem->next;
  }

  return rel;
}

/*
=========================================
I/O routines with paths
=========================================
*/

#if 0
/* tests whether the file or directory is readable */
int scr_path_is_readable(const scr_path* file)
{
  /* convert to string and delegate to I/O routine */
  char* file_str = scr_path_strdup(file);
  int rc = scr_file_is_readable(file_str);
  scr_free(&file_str);
  return rc;
}

/* tests whether the file or directory is writeable */
int scr_path_is_writeable(const scr_path* file)
{
  /* convert to string and delegate to I/O routine */
  char* file_str = scr_path_strdup(file);
  int rc = scr_file_is_writable(file_str);
  scr_free(&file_str);
  return rc;
}
#endif

#ifndef HIDE_TV
/*
=========================================
Pretty print for TotalView debug window
=========================================
*/

/* This enables a nicer display when diving on a path variable
 * under the TotalView debugger.  It requires TV 8.8 or later. */

#include "tv_data_display.h"

static int TV_ttf_display_type(const scr_path* path)
{
  if (path == NULL) {
    /* empty path, nothing to display here */
    return TV_ttf_format_ok;
  }

  if (scr_path_is_null(path)) {
    /* empty path, nothing to display here */
    return TV_ttf_format_ok;
  }

  /* print path in string form */
  char* str = scr_path_strdup(path);
  TV_ttf_add_row("path", TV_ttf_type_ascii_string, str);
  scr_free(&str);

  return TV_ttf_format_ok;
}
#endif /* HIDE_TV */
