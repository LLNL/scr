#include <string.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>

#include <sys/time.h>

#include "axl_internal.h"

unsigned long axl_file_buf_size;

/* returns the current linux timestamp (secs + usecs since epoch) as a double */
double axl_seconds()
{
  struct timeval tv;
  gettimeofday(&tv, NULL);
  double secs = (double) tv.tv_sec + (double) tv.tv_usec / (double) 1000000.0;
  return secs;
}

/* caller really passes in a void**, but we define it as just void* to avoid printing
 * a bunch of warnings */
void axl_free(void* p)
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

/* Clone of apsrintf().  See the standard asprintf() man page for details */
int asprintf(char** strp, const char* fmt, ...)
{
    /*
     * This code is taken from the vmalloc(3) man page and modified slightly.
     */
    int n;
    int size = 100;     /* Guess we need no more than 100 bytes */
    char* p;
    char* np;
    va_list ap;

    p = malloc(size);
    if (p == NULL) {
        *strp = NULL;
        return -ENOMEM;
    }

    while (1) {
        /* Try to print in the allocated space */

        va_start(ap, fmt);
        n = vsnprintf(p, size, fmt, ap);
        va_end(ap);

        /* Check error code */

        if (n < 0) {
            *strp = NULL;
            return -1;
        }

        /* If that worked, return the string */
        if (n < size) {
            *strp = p;
            return n;
        }

        /* Else try again with more space */

        size = n + 1;       /* Precisely what is needed */

        np = realloc(p, size);
        if (np == NULL) {
            *strp = NULL;
            free(p);
            return -ENOMEM;
        } else {
            p = np;
        }
    }
}


/*
 * This is an helper function to iterate though a file list for a given
 * AXL ID.  Usage:
 *
 *    char* src;
 *    char* dst;
 *    char* kvtree_elem *elem = NULL;
 *
 *    while ((elem = axl_get_next_path(id, elem, &src, &dst))) {
 *        printf("src %s, dst %s\n", src, dst);
 *    }
 *
 *    src or dst can be set to NULL if you don't care about the value.
 */
kvtree_elem* axl_get_next_path(int id, kvtree_elem* elem, char** src, char** dst)
{
    if (! elem) {
        /* lookup transfer info for the given id */
        kvtree* file_list = axl_kvtrees[id];
        if (! file_list) {
            return NULL;
        }

        kvtree* files = kvtree_get(file_list, AXL_KEY_FILES);
        elem = kvtree_elem_first(files);
    } else {
        elem = kvtree_elem_next(elem);
    }

    if (src) {
        *src = kvtree_elem_key(elem);
    }

    if (dst) {
        /* get destination for this file */
        kvtree* elem_hash = kvtree_elem_hash(elem);
        kvtree_util_get_str(elem_hash, AXL_KEY_FILE_DEST, dst);
    }

    return (elem);
}
