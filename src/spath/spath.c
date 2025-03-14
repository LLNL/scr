/* Defines a double linked list representing a file path. */

#include "spath.h"
#include "spath_util.h"

#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>

#include <stdint.h>

/*
=========================================
Private functions
=========================================
*/

static inline int spath_elem_init(spath_elem* elem)
{
  elem->component = NULL;
  elem->chars     = 0;
  elem->next      = NULL;
  elem->prev      = NULL;
  return SPATH_SUCCESS;
}

static inline int spath_init(spath* path)
{
  path->components = 0;
  path->chars      = 0;
  path->head       = NULL;
  path->tail       = NULL;
  return SPATH_SUCCESS;
}

/* allocate and initialize a new path element */
static spath_elem* spath_elem_alloc()
{
  spath_elem* elem = (spath_elem*) SPATH_MALLOC(sizeof(spath_elem));
  spath_elem_init(elem);
  return elem;
}

/* free a path element */
static int spath_elem_free(spath_elem** ptr_elem)
{
  if (ptr_elem != NULL) {
    /* got an address to the pointer of an element,
     * dereference to get pointer to elem */
    spath_elem* elem = *ptr_elem;
    if (elem != NULL) {
      /* free the component which was strdup'ed */
      spath_free(&(elem->component));
    } 
  }

  /* free the element structure itself */
  spath_free(ptr_elem);

  return SPATH_SUCCESS;
}

/* allocate a new path */
static spath* spath_alloc()
{
  spath* path = (spath*) SPATH_MALLOC(sizeof(spath));
  spath_init(path);
  return path;
}

/* allocate and return a duplicate of specified elememnt,
 * only copies value not next and previoud pointers */
static spath_elem* spath_elem_dup(const spath_elem* elem)
{
  /* check that element is not NULL */
  if (elem == NULL) {
    return NULL;
  }

  /* allocate new element */
  spath_elem* dup_elem = spath_elem_alloc();
  if (dup_elem == NULL) {
    fprintf(stderr, "Failed to allocate memory for path element @ %s:%d",
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
static spath_elem* spath_elem_index(const spath* path, int index)
{
  /* check that we got a path */
  if (path == NULL) {
    fprintf(stderr, "Assert that path are not NULL @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* check that index is in range */
  if (index < 0 || index >= path->components) {
    fprintf(stderr, "Offset %d is out of range [0,%d) @ %s:%d",
      index, path->components, __FILE__, __LINE__
    );
  }

  /* scan until we find element at specified index */
  spath_elem* current = NULL;
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
static int spath_elem_insert(spath* path, int offset, spath_elem* elem)
{
  /* check that we got a path and element */
  if (path == NULL || elem == NULL) {
    fprintf(stderr, "Assert that path and elem are not NULL @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* check that offset is in range */
  if (offset < 0 || offset > path->components) {
    fprintf(stderr, "Offset %d is out of range of [0,%d] @ %s:%d",
      offset, path->components, __FILE__, __LINE__
    );
  }

  /* if offset equals number of components, insert after last element */
  if (offset == path->components) {
    /* attach to path */
    path->components++;
    path->chars += elem->chars;

    /* get pointer to tail element and point to element as new tail */
    spath_elem* tail = path->tail;
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

    return SPATH_SUCCESS;
  }

  /* otherwise, insert element before current element */

  /* lookup element at specified offset */
  spath_elem* current = spath_elem_index(path, offset);

  /* attach to path */
  path->components++;
  path->chars += elem->chars;

  /* insert element before current */
  if (current != NULL) {
    /* get pointer to element before current */
    spath_elem* prev = current->prev;
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

  return SPATH_SUCCESS;
}

/* extract specified element from path */
static int spath_elem_extract(spath* path, spath_elem* elem)
{
  /* check that we got a path and element */
  if (path == NULL || elem == NULL) {
    /* nothing to do in this case */
    fprintf(stderr, "Assert that path and elem are not NULL @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* TODO: would be nice to verify that elem is part of path */

  /* subtract component and number of chars from path */
  path->components--;
  path->chars -= elem->chars;

  /* lookup address of elements of next and previous items */
  spath_elem* prev = elem->prev;
  spath_elem* next = elem->next;

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

  return SPATH_SUCCESS;
}

/* allocates and returns a string filled in with formatted text,
 * assumes that caller has called va_start before and will call va_end
 * after */
static char* spath_alloc_strf(const char* format, va_list args1, va_list args2)
{
  /* get length of component string */
  size_t chars = (size_t) vsnprintf(NULL, 0, format, args1);

  /* allocate space to hold string, add one for the terminating NUL */
  size_t strlen = chars + 1;
  char* str = (char*) SPATH_MALLOC(strlen);

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
spath* spath_new()
{
  spath* path = spath_alloc();
  if (path == NULL) {
    fprintf(stderr, "Failed to allocate memory for path object @ %s:%d",
      __FILE__, __LINE__
    );
  }
  return path;
}

/* allocates a path from string */
spath* spath_from_str(const char* str)
{
  /* allocate a path object */
  spath* path = spath_alloc();
  if (path == NULL) {
    fprintf(stderr, "Failed to allocate memory for path object @ %s:%d",
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
      char* buf = (char*) SPATH_MALLOC(buflen);

      /* copy characters into string buffer and add terminating NUL */
      size_t chars = buflen - 1;
      if (chars > 0) {
        strncpy(buf, start, chars);
      }
      buf[chars] = '\0';

      /* allocate new element */
      spath_elem* elem = spath_elem_alloc();
      if (elem == NULL) {
        fprintf(stderr, "Failed to allocate memory for path component @ %s:%d",
          __FILE__, __LINE__
        );
      }

      /* record string in element */
      elem->component = buf;
      elem->chars     = chars;

      /* add element to path */
      spath_elem_insert(path, path->components, elem);

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
spath* spath_from_strf(const char* format, ...)
{
  /* allocate formatted string */
  va_list args1, args2;
  va_start(args1, format);
  va_start(args2, format);
  char* str = spath_alloc_strf(format, args1, args2);
  va_end(args2);
  va_end(args1);
  if (str == NULL) {
    fprintf(stderr, "Failed to allocate memory for path component string @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* create path from string */
  spath* path = spath_from_str(str);

  /* free the string */
  spath_free(&str);

  return path;
}

/* duplicate a path */
spath* spath_dup(const spath* path)
{
  /* easy if path is NULL */
  if (path == NULL) {
    return NULL;
  }

  /* allocate a new path */
  spath* dup_path = spath_new();
  if (dup_path == NULL) {
    fprintf(stderr, "Failed to allocate path object @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* get pointer to first element and delete elements in list */
  spath_elem* current = path->head;
  while (current != NULL) {
    /* get pointer to element after current, delete current,
     * and set current to next */
    spath_elem* dup_elem = spath_elem_dup(current);
    if (dup_elem == NULL) {
      fprintf(stderr, "Failed to allocate path element object @ %s:%d",
        __FILE__, __LINE__
      );
    }

    /* insert new element at end of path */
    spath_elem_insert(dup_path, dup_path->components, dup_elem);

    /* advance to next element */
    current = current->next;
  }

  return dup_path;
}

/* free a path */
int spath_delete(spath** ptr_path)
{
  if (ptr_path != NULL) {
    /* got an address to the pointer of a path object,
     * dereference to get pointer to path */
    spath* path = *ptr_path;
    if (path != NULL) {
      /* get pointer to first element and delete elements in list */
      spath_elem* current = path->head;
      while (current != NULL) {
        /* get pointer to element after current, delete current,
         * and set current to next */
        spath_elem* next = current->next;
        spath_elem_free(&current);
        current = next;
      }
    }
  }

  /* free the path object itself */
  spath_free(ptr_path);

  return SPATH_SUCCESS;
}

/*
=========================================
get size and string functions
=========================================
*/

/* returns 1 if path has 0 components, 0 otherwise */
int spath_is_null(const spath* path)
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
int spath_components(const spath* path)
{
  if (path != NULL) {
    int components = path->components;
    return components;
  }
  return 0;
}

/* return number of characters needed to store path
 * (not including terminating NUL) */
size_t spath_strlen(const spath* path)
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
static int spath_strcpy_internal(char* buf, const spath* path)
{
  /* copy contents into string buffer */
  char* ptr = buf;
  spath_elem* current = path->head;
  while (current != NULL) {
    /* copy component to buffer */
    char* component = current->component;
    size_t chars    = current->chars;
    memcpy((void*)ptr, (void*)component, chars);
    ptr += chars;

    /* if there is another component, add a slash */
    spath_elem* next = current->next;
    if (next != NULL) {
      *ptr = '/';
      ptr++;
    }

    /* move to next component */
    current = next;
  }

  /* terminate the string */
  *ptr = '\0';

  return SPATH_SUCCESS;
}

/* copy string into user buffer, abort if buffer is too small */
size_t spath_strcpy(char* buf, size_t n, const spath* path)
{
  /* check that we have a pointer to a path */
  if (path == NULL) {
    fprintf(stderr, "Cannot copy NULL pointer to string @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* we can't copy a NULL path */
  if (spath_is_null(path)) {
    fprintf(stderr, "Cannot copy a NULL path to string @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* get length of path */
  size_t strlen = spath_strlen(path) + 1;

  /* if user buffer is too small, abort */
  if (n < strlen) {
    fprintf(stderr, "User buffer of %d bytes is too small to hold string of %d bytes @ %s:%d",
      (int)n, (int)strlen, __FILE__, __LINE__
    );
  }

  /* copy contents into string buffer */
  spath_strcpy_internal(buf, path);

  /* return number of bytes we copied to buffer */
  return strlen;
}

/* allocate memory and return path in string form */
char* spath_strdup(const spath* path)
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
  size_t buflen = spath_strlen(path) + 1;
  char* buf = (char*) SPATH_MALLOC(buflen);

  /* copy contents into string buffer */
  spath_strcpy_internal(buf, path);

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
static int spath_combine(spath* path1, int offset, spath** ptr_path2)
{
  if (path1 != NULL) {
    /* check that offset is in range */
    int components = path1->components;
    if (offset < 0 || offset > components) {
      fprintf(stderr, "Offset %d is out of range [0,%d] @ %s:%d",
        offset, components, __FILE__, __LINE__
      );
    }

    if (ptr_path2 != NULL) {
      /* got an address to the pointer of a path object,
       * dereference to get pointer to path */
      spath* path2 = *ptr_path2;
      if (path2 != NULL) {
        /* get pointer to head and tail of path2 */
        spath_elem* head2 = path2->head;
        spath_elem* tail2 = path2->tail;

        /* if offset equals number of components, insert after last element,
         * otherwise, insert element before specified element */
        if (offset == components) {
          /* get pointer to tail of path1 */
          spath_elem* tail1 = path1->tail;
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
            spath_elem* current = spath_elem_index(path1, offset);

            /* get pointer to element before current */
            spath_elem* prev = current->prev;

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
    spath_free(ptr_path2);
  } else {
    fprintf(stderr, "Cannot attach a path to a NULL path @ %s:%d",
      __FILE__, __LINE__
    );
    return SPATH_FAILURE;
  }

  return SPATH_SUCCESS;
}

/* inserts path2 so head element in path2 starts at specified offset
 * in path1, e.g.,
 *   0   - before first element of path1
 *   N-1 - before last element of path1
 *   N   - after last element of path1 */
int spath_insert(spath* path1, int offset, const spath* path2)
{
  int rc = SPATH_SUCCESS;
  if (path1 != NULL) {
    /* make a copy of path2, and combint at specified offset in path1,
     * combine deletes copy of path2 */
    spath* path2_copy = spath_dup(path2);
    rc = spath_combine(path1, offset, &path2_copy);
  } else {
    fprintf(stderr, "Cannot attach a path to a NULL path @ %s:%d",
      __FILE__, __LINE__
    );
    return SPATH_FAILURE;
  }
  return rc;
}

/* prepends path2 to path1 */
int spath_prepend(spath* path1, const spath* path2)
{
  int rc = spath_insert(path1, 0, path2);
  return rc;
}

/* appends path2 to path1 */
int spath_append(spath* path1, const spath* path2)
{
  int rc = SPATH_SUCCESS;
  if (path1 != NULL) {
    rc = spath_insert(path1, path1->components, path2);
  } else {
    fprintf(stderr, "Cannot attach a path to a NULL path @ %s:%d",
      __FILE__, __LINE__
    );
    return SPATH_FAILURE;
  }
  return rc;
}

/* inserts components in string so first component in string starts
 * at specified offset in path, e.g.,
 *   0   - before first element of path
 *   N-1 - before last element of path
 *   N   - after last element of path */
int spath_insert_str(spath* path, int offset, const char* str)
{
  /* verify that we got a path as input */
  if (path == NULL) {
    fprintf(stderr, "Cannot insert string to a NULL path @ %s:%d",
      __FILE__, __LINE__
    );
    return SPATH_FAILURE;
  }

  /* create a path from this string */
  spath* newpath = spath_from_str(str);
  if (newpath == NULL) {
    fprintf(stderr, "Failed to allocate path for insertion @ %s:%d",
      __FILE__, __LINE__
    );
    return SPATH_FAILURE;
  }

  /* attach newpath to original path */
  int rc = spath_combine(path, offset, &newpath);
  return rc;
}

/* prepends components in string to path */
int spath_prepend_str(spath* path, const char* str)
{
  int rc = spath_insert_str(path, 0, str);
  return rc;
}

/* appends components in string to path */
int spath_append_str(spath* path, const char* str)
{
  int rc = SPATH_SUCCESS;
  if (path != NULL) {
    rc = spath_insert_str(path, path->components, str);
  } else {
    fprintf(stderr, "Cannot attach string to a NULL path @ %s:%d",
      __FILE__, __LINE__
    );
    return SPATH_FAILURE;
  }
  return rc;
}

/* inserts components in string so first component in string starts
 * at specified offset in path, e.g.,
 *   0   - before first element of path
 *   N-1 - before last element of path
 *   N   - after last element of path */
int spath_insert_strf(spath* path, int offset, const char* format, ...)
{
  /* verify that we got a path as input */
  if (path == NULL) {
    fprintf(stderr, "Cannot append string to a NULL path @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* allocate formatted string */
  va_list args1, args2;
  va_start(args1, format);
  va_start(args2, format);
  char* str = spath_alloc_strf(format, args1, args2);
  va_end(args2);
  va_end(args1);
  if (str == NULL) {
    fprintf(stderr, "Failed to allocate memory for path component string @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* attach str to path */
  int rc = spath_insert_str(path, offset, str);

  /* free the string */
  spath_free(&str);

  return rc;
}

/* prepends components in string to path */
int spath_prepend_strf(spath* path, const char* format, ...)
{
  /* verify that we got a path as input */
  if (path == NULL) {
    fprintf(stderr, "Cannot append string to a NULL path @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* allocate formatted string */
  va_list args1, args2;
  va_start(args1, format);
  va_start(args2, format);
  char* str = spath_alloc_strf(format, args1, args2);
  va_end(args2);
  va_end(args1);
  if (str == NULL) {
    fprintf(stderr, "Failed to allocate memory for path component string @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* attach str to path */
  int rc = spath_insert_str(path, 0, str);

  /* free the string */
  spath_free(&str);

  return rc;
}

/* adds a new component to end of path using printf-like formatting */
int spath_append_strf(spath* path, const char* format, ...)
{
  /* verify that we got a path as input */
  if (path == NULL) {
    fprintf(stderr, "Cannot append string to a NULL path @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* allocate formatted string */
  va_list args1, args2;
  va_start(args1, format);
  va_start(args2, format);
  char* str = spath_alloc_strf(format, args1, args2);
  va_end(args2);
  va_end(args1);
  if (str == NULL) {
    fprintf(stderr, "Failed to allocate memory for path component string @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* attach str to path */
  int rc = spath_insert_str(path, path->components, str);

  /* free the string */
  spath_free(&str);

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
int spath_slice(spath* path, int offset, int length)
{
  /* check that we have a path */
  if (path == NULL) {
    return SPATH_SUCCESS;
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
    return SPATH_SUCCESS;
  }

  /* lookup first element to be head of new path */
  spath_elem* current = spath_elem_index(path, offset);

  /* delete any items before this one */
  spath_elem* elem = current->prev;
  while (elem != NULL) {
    spath_elem* prev = elem->prev;
    spath_elem_free(&elem);
    elem = prev;
  }

  /* remember our starting element and intialize tail to NULL */
  spath_elem* head = current;
  spath_elem* tail = NULL;

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
    spath_elem* next = current->next;
    spath_elem_free(&current);
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

  return SPATH_SUCCESS;
}

/* drops last component from path */
int spath_dirname(spath* path)
{
  int components = spath_components(path);
  if (components > 0) {
    int rc = spath_slice(path, 0, components-1);
    return rc;
  }
  return SPATH_SUCCESS;
}

/* only leaves last component of path */
int spath_basename(spath* path)
{
  int rc = spath_slice(path, -1, 1);
  return rc;
}

/* copies upto length components of path starting at specified location
 * and returns subpath as new path, offset can be negative to count
 * from back, a negative length copies the remainder of the string */
spath* spath_sub(spath* path, int offset, int length)
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
  spath* newpath = spath_alloc();
  if (newpath == NULL) {
    fprintf(stderr, "Failed to allocate memory for path object @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* return the empty path if source path is empty */
  if (components == 0) {
    return newpath;
  }

  /* lookup first element to be head of new path */
  spath_elem* current = spath_elem_index(path, offset);

  /* copy elements from path and attach to newpath */
  while ((length < 0 || length > 0) && current != NULL) {
    /* duplicate element */
    spath_elem* elem = spath_elem_dup(current);
    if (elem == NULL) {
      fprintf(stderr, "Failed to duplicate element of path object @ %s:%d",
        __FILE__, __LINE__
      );
    }

    /* insert element into newpath */
    spath_elem_insert(newpath, newpath->components, elem);

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
spath* spath_cut(spath* path, int offset)
{
  /* check that we have a path */
  if (path == NULL) {
    return NULL;
  }

  /* allocate and initialize an empty path object */
  spath* newpath = spath_alloc();
  if (newpath == NULL) {
    fprintf(stderr, "Failed to allocate memory for path object @ %s:%d",
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
  spath_elem* current = spath_elem_index(path, offset);

  /* set head and tail of newpath2 */
  newpath->head = current;
  newpath->tail = path->tail;

  /* set tail (and head) of path */
  if (current != NULL) {
    /* get element before current to be new tail */
    spath_elem* prev = current->prev;

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

/* allocate a new path initialized with current working dir */
spath* spath_cwd(void)
{
  /* get current working directory, let it allocate a string for us */
  char* cwd = getcwd(NULL, 0);
  if (cwd == NULL) {
    fprintf(stderr, "Call to getcwd failed: %s @ %s:%d",
      strerror(errno), __FILE__, __LINE__
    );
    return NULL;
  }

  /* convert to an spath */
  spath* path = spath_from_str(cwd);

  /* free current working dir string */
  spath_free(&cwd);
  
  return path;
}

/* transform given spath object by calling realpath */
int spath_realpath(spath* path)
{
  /* check that we got a path */
  if (path == NULL) {
    /* nothing to do in this case */
    return SPATH_SUCCESS;
  }

  /* get path in string form */
  char* path_str = spath_strdup(path);

  /* call realpath to do the work */
  char* newpath = realpath(path_str, NULL);
  if (newpath == NULL) {
    fprintf(stderr, "Call to realpath(`%s', NULL) failed: %s @ %s:%d",
      path_str, strerror(errno), __FILE__, __LINE__
    );
    spath_free(&path_str);
    return SPATH_FAILURE;
  }

  /* truncate callers path object */
  spath_slice(path, 0, 0);

  /* refill it with string from realpath */
  spath_insert_str(path, 0, newpath);

  /* free memory allocated to us */
  spath_free(&newpath);
  spath_free(&path_str);

  return SPATH_SUCCESS;
}

/* removes consecutive '/', '.', '..', and trailing '/' */
int spath_reduce(spath* path)
{
  /* check that we got a path */
  if (path == NULL) {
    /* nothing to do in this case */
    return SPATH_SUCCESS;
  }


  /* now iterate through and remove any "." and empty strings,
   * we go from back to front to handle paths like "./" */
  spath_elem* current = path->tail;
  while (current != NULL) {
    /* get pointer to previous element */
    spath_elem* prev = current->prev;
 
    /* check whether component string matches "." or "" */
    char* component = current->component;
    if (strcmp(component, ".") == 0) {
      /* pull element out of path and delete it */
      spath_elem_extract(path, current);
      spath_elem_free(&current);
    } else if (strcmp(component, "") == 0 && current != path->head) {
      /* head is allowed to be empty string so that we don't chop leading '/' */
      /* pull element out of path and delete it */
      spath_elem_extract(path, current);
      spath_elem_free(&current);
    }

    /* advance to previous item */
    current = prev;
  }

  /* now remove any ".." and any preceding component */
  current = path->head;
  while (current != NULL) {
    /* get pointer to previous and next elements */
    spath_elem* prev = current->prev;
    spath_elem* next = current->next;
 
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
            spath_elem_extract(path, prev);
            spath_elem_free(&prev);

            /* delete current element */
            spath_elem_extract(path, current);
            spath_elem_free(&current);
          } else {
            fprintf(stderr, "Cannot pop past root directory @ %s:%d",
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

  return SPATH_SUCCESS;
}

/* creates path from string, calls reduce, calls path_strdup,
 * and deletes path, caller must free returned string with free */
char* spath_strdup_reduce_str(const char* str)
{
  spath* path = spath_from_str(str);
  spath_reduce(path);
  char* newstr = spath_strdup(path);
  spath_delete(&path);
  return newstr;
}

/* return 1 if path starts with an empty string, 0 otherwise */
int spath_is_absolute(const spath* path)
{
  if (path != NULL) {
    if (path->components > 0) {
      const spath_elem* head = path->head;
      const char* component = head->component; 
      if (strcmp(component, "") == 0) {
        return 1;
      }
    }
  }
  return 0;
}

/* return 1 if child is contained in tree starting at parent, 0 otherwise */
int spath_is_child(const spath* parent, const spath* child)
{
  /* check that we got pointers to both parent and child paths */
  if (parent == NULL || child == NULL) {
    return 0;
  }

  /* check that parent and child aren't NULL paths */
  if (spath_is_null(parent)) {
    return 0;
  }
  if (spath_is_null(child)) {
    return 0;
  }

  /* TODO: check that paths are absolute */

  /* TODO: reduce paths? */

  /* get pointers to start of parent and child */
  int equal = 1;
  spath_elem* parent_elem = parent->head;
  spath_elem* child_elem  = child->head;
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
spath* spath_relative(const spath* src, const spath* dst)
{
  /* check that we don't have NULL pointers */
  if (src == NULL || dst == NULL) {
    fprintf(stderr, "Either src or dst pointer is NULL @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* we can't get to a NULL path from a non-NULL path */
  int src_components = src->components;
  int dst_components = dst->components;
  if (src_components > 0 && dst_components == 0) {
    fprintf(stderr, "Cannot get from non-NULL path to NULL path @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* allocate a new path to record relative path */
  spath* rel = spath_new();
  if (rel == NULL) {
    fprintf(stderr, "Failed to allocate memory for relative path @ %s:%d",
      __FILE__, __LINE__
    );
  }

  /* walk down both paths until we find the first location where they
   * differ */
  const spath_elem* src_elem = src->head;
  const spath_elem* dst_elem = dst->head;
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
    spath_append_str(rel, "..");
    src_elem = src_elem->next;
  }

  /* now tack on any items left from dst */
  while (dst_elem != NULL) {
    const char* dst_component = dst_elem->component;
    spath_append_str(rel, dst_component);
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
int spath_is_readable(const spath* file)
{
  /* convert to string and delegate to I/O routine */
  char* file_str = spath_strdup(file);
  int rc = scr_file_is_readable(file_str);
  spath_free(&file_str);
  return rc;
}

/* tests whether the file or directory is writeable */
int spath_is_writeable(const spath* file)
{
  /* convert to string and delegate to I/O routine */
  char* file_str = spath_strdup(file);
  int rc = scr_file_is_writable(file_str);
  spath_free(&file_str);
  return rc;
}
#endif

#ifdef HAVE_TV
/*
=========================================
Pretty print for TotalView debug window
=========================================
*/

/* This enables a nicer display when diving on a path variable
 * under the TotalView debugger.  It requires TV 8.8 or later. */

#include "tv_data_display.h"

static int TV_ttf_display_type(const spath* path)
{
  if (path == NULL) {
    /* empty path, nothing to display here */
    return TV_ttf_format_ok;
  }

  if (spath_is_null(path)) {
    /* empty path, nothing to display here */
    return TV_ttf_format_ok;
  }

  /* when last tested, this string needs to be dynamically allocated
   * but not freed in order for TV to dispaly the correct value */
  /* print path in string form */
  //char str[2048];
  //spath_strcpy(str, sizeof(str), path);
  char* str = spath_strdup(path);
  TV_ttf_add_row("path", TV_ttf_type_ascii_string, str);
  //spath_free(&str);

  return TV_ttf_format_ok;
}
#endif /* HAVE_TV */
