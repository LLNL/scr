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

#include "kvtree.h"

#include "redset.h"
#include "redset_internal.h"
#include "redset_util.h"

int redset_debug = 1;
int redset_rank = 0;
char* redset_hostname = NULL;

/* print error message to stdout */
void redset_err(const char *fmt, ...)
{
  va_list argp;
  fprintf(stdout, "REDSET %s ERROR: rank %d on %s: ", REDSET_VERSION, redset_rank, redset_hostname);
  va_start(argp, fmt);
  vfprintf(stdout, fmt, argp);
  va_end(argp);
  fprintf(stdout, "\n");
}

/* print warning message to stdout */
void redset_warn(const char *fmt, ...)
{
  va_list argp;
  fprintf(stdout, "REDSET %s WARNING: rank %d on %s: ", REDSET_VERSION, redset_rank, redset_hostname);
  va_start(argp, fmt);
  vfprintf(stdout, fmt, argp);
  va_end(argp);
  fprintf(stdout, "\n");
}

/* print message to stdout if redset_debug is set and it is >= level */
void redset_dbg(int level, const char *fmt, ...)
{
  va_list argp;
  if (level == 0 || (redset_debug > 0 && redset_debug >= level)) {
    fprintf(stdout, "REDSET %s: rank %d on %s: ", REDSET_VERSION, redset_rank, redset_hostname);
    va_start(argp, fmt);
    vfprintf(stdout, fmt, argp);
    va_end(argp);
    fprintf(stdout, "\n");
  }
}

/* print abort message and exit run */
void redset_abort(int rc, const char *fmt, ...)
{
  va_list argp;
  fprintf(stderr, "REDSET %s ABORT: rank %d on %s: ", REDSET_VERSION, redset_rank, redset_hostname);
  va_start(argp, fmt);
  vfprintf(stderr, fmt, argp);
  va_end(argp);
  fprintf(stderr, "\n");

  exit(1);
}
