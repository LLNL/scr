#include  <stdlib.h>
#include  <stdio.h>

/** \file spath_util.h
 *  \ingroup spath
 *  \brief utilities for spath */

#define SPATH_MALLOC(X) spath_malloc(X, __FILE__, __LINE__);

/** allocate size bytes, returns NULL if size == 0 */
/* TODO: should somehowe abort if allocation fails */
void* spath_malloc(size_t size, const char* file, int line);

/* caller really passes in a void**, but we define it as just void* to avoid printing
 * a bunch of warnings */
void spath_free(void* p);
